//! Helper utilities.

mod aggregator;
mod buffers;
mod fragments;
mod parse;

use std::fmt;

pub use aggregator::{MessageAggregator, MessageProcessor};
pub use buffers::{Buffer, Buffers};
pub use fragments::Fragments;
pub use parse::parse;

/// Meta-information when a message is fragmented.
struct FragmentInformation {
    byte_offset: u32,
    total_bytes: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Continuation {
    // This is a separate message.
    No,
    /// This is the beginning of a series of messages.
    Begin,
    /// This is a continuation of a previous message.
    Continue,
}

pub struct RawParsedEntry<'a> {
    level: Level,
    sequence_number: u64,
    timestamp: Timestamp,
    continuation: Continuation,
    fragment_information: Option<FragmentInformation>,
    data: &'a [u8],
}

impl RawParsedEntry<'_> {
    /// Get the entry's sequence number.
    pub fn sequence_number(&self) -> u64 {
        self.sequence_number
    }
}

/// A single message, possibly reconstructed from fragments.
pub struct SingleMessage {
    pub level: Level,
    pub sequence_number: u64,
    pub timestamp: Timestamp,
    pub continuation: Continuation,
    pub data: Vec<u8>,
}

/// Log level/priority.
#[derive(Debug, Clone, Copy)]
pub struct Level(pub u8);

impl fmt::Display for Level {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            0 => write!(f, "emergency"),
            1 => write!(f, "alert"),
            2 => write!(f, "critical"),
            3 => write!(f, "error"),
            4 => write!(f, "warning"),
            5 => write!(f, "notice"),
            6 => write!(f, "informational"),
            7 => write!(f, "debug"),
            unknown => write!(f, "unknown #{unknown}"),
        }
    }
}

/// Timestamp since the system start (in microseconds).
#[derive(Debug, Clone, Copy)]
pub struct Timestamp(pub u64);

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // 408968514 -> 408.968514
        let seconds = self.0 / 1_000_000;
        let microseconds = self.0 % 1_000_000;
        write!(f, "{seconds}.{microseconds:06}s")
    }
}
