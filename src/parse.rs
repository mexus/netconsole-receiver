//! Message parsing.

use std::str::FromStr;

use color_eyre::{
    eyre::{Context, ContextCompat},
    Result,
};

use crate::{Continuation, FragmentInformation, Level, RawParsedEntry, Timestamp};

/// Parses a raw incoming message.
pub fn parse(message: &[u8]) -> Result<RawParsedEntry<'_>> {
    // <level>,<sequnum>,<timestamp>,<contflag>;<message text>
    let semicolon = memchr::memchr(b';', message).wrap_err("No semicolon found")?;
    let header = message.get(..semicolon).expect("It exists!");
    let body = message.get(semicolon + 1..).unwrap_or_default();

    let mut fields = header.split(|c| *c == b',');

    let level = parse_array(fields.next().wrap_err("Level is missing")?)
        .map(Level)
        .wrap_err("Can't parse level")?;

    let sequence_number: u64 = parse_array(fields.next().wrap_err("Sequence number is missing")?)
        .wrap_err("Can't parse sequence number")?;

    let timestamp = parse_array(fields.next().wrap_err("Timestamp is missing")?)
        .map(Timestamp)
        .wrap_err("Can't parse timestamp")?;

    let continuation = fields.next().wrap_err("Continuation flag is missing")?;
    let continuation = match continuation {
        &[b'c'] => Continuation::Begin,
        &[b'+'] => Continuation::Continue,
        &[b'-'] => Continuation::No,
        invalid => {
            color_eyre::eyre::bail!(
                "Invalid value of the continuation field: {:?}",
                String::from_utf8_lossy(invalid)
            )
        }
    };

    let mut fragment_information = None;
    // Rest of the fields should follow a pattern of key=value.
    for field in fields {
        if let Some(equals) = memchr::memchr(b'=', field) {
            let key = field.get(0..equals).expect("It definitely exists");
            if key != b"ncfrag" {
                tracing::warn!("Unexpected key \"{}\"", String::from_utf8_lossy(key));
                continue;
            }
            if fragment_information.is_some() {
                color_eyre::eyre::bail!("Fragment information is specified twice!");
            }
            let value = field.get(equals + 1..).unwrap_or_default();

            // So it's ncfrag=<byte-offset>/<total-bytes>
            let mut values = value.split(|c| *c == b'/');
            let byte_offset =
                parse_array(values.next().wrap_err("Byte offset in ncfrag is missing")?)
                    .wrap_err("Can't parse byte offset")?;
            let total_bytes =
                parse_array(values.next().wrap_err("Total bytes in ncfrag is missing")?)
                    .wrap_err("Can't parse total bytes")?;
            fragment_information = Some(FragmentInformation {
                byte_offset,
                total_bytes,
            });
        } else {
            color_eyre::eyre::bail!(
                "Unexpected field with value {}",
                String::from_utf8_lossy(field)
            );
        }
    }

    Ok(RawParsedEntry {
        level,
        sequence_number,
        timestamp,
        continuation,
        fragment_information,
        data: body,
    })
}

/// Like `.parse()`, but for a byte slice.
fn parse_array<T>(value: &[u8]) -> Result<T>
where
    T: FromStr,
    <T as FromStr>::Err: std::error::Error + Send + Sync + 'static,
{
    let value = std::str::from_utf8(value).wrap_err("Non-unicode array")?;
    value
        .parse()
        .wrap_err_with(|| format!("Invalid value \"{value}\""))
}
