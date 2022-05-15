use std::{fmt, net::SocketAddr, time::Duration};

use color_eyre::{eyre::Context, Result};
use netconsole_handler::{parse, MessageAggregator, MessageProcessor, SingleMessage};
use structopt::StructOpt;
use tokio::net::UdpSocket;

#[tokio::main]
async fn main() -> Result<()> {
    use tracing::metadata::LevelFilter;
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};

    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .with_env_var(EnvFilter::DEFAULT_ENV)
        .from_env()
        .wrap_err("Unable to initialize tracing env filter")?;

    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(filter)
        .init();

    run().await
}

#[derive(StructOpt)]
struct Args {
    listen_address: SocketAddr,
}

async fn run() -> Result<()> {
    let Args { listen_address } = Args::from_args();

    tracing::info!("Listening at {listen_address} (UDP)");

    let socket = UdpSocket::bind(listen_address)
        .await
        .wrap_err_with(|| format!("Unable to bind a UDP socket to {listen_address}"))?;

    // Maximum UDP payload size is slightly smaller, but who cares.
    let mut buffer = [0u8; 2usize.pow(16)];

    let mut aggregator = MessageAggregator::new(MessageTracing);

    const TICK_TIMEOUT: Duration = Duration::from_secs(5);

    loop {
        let (len, source) =
            match tokio::time::timeout(TICK_TIMEOUT, socket.recv_from(&mut buffer)).await {
                Ok(Ok((len, source))) => (len, source),
                Ok(Err(e)) => {
                    tracing::error!("Unable to receive data from the socket: {e:#}");
                    continue;
                }
                Err(_timeout) => {
                    tracing::debug!("No data for quite some time.. ({TICK_TIMEOUT:?})");
                    aggregator.process_timeouts();
                    continue;
                }
            };

        tracing::debug!("Received {len} bytes from {source}");
        let data = &buffer[0..len];
        match parse(data) {
            Ok(raw) => {
                let sequence_number = raw.sequence_number;
                if let Err(e) = aggregator.process(raw) {
                    tracing::warn!("Unable to process a message #{sequence_number}: {e:#}")
                }
            }
            Err(e) => tracing::warn!("Unable to parse incoming entry: {e:#}"),
        };
    }
}

struct MessageTracing;

impl MessageProcessor for MessageTracing {
    fn process_one(
        &mut self,
        SingleMessage {
            level,
            sequence_number,
            timestamp,
            continuation: _,
            data,
        }: SingleMessage,
    ) {
        let data = String::from_utf8_lossy(&data);
        tracing::info!("[{level}] [#{sequence_number}] [{timestamp}]: {data}");
    }

    fn process_many(&mut self, messages: Vec<SingleMessage>) {
        tracing::info!("{}", FormatChunk(&messages));
    }
}

struct FormatChunk<'a>(&'a [SingleMessage]);

impl fmt::Display for FormatChunk<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "A chunk of [{}] messages:", self.0.len())?;
        for SingleMessage {
            level,
            sequence_number,
            timestamp,
            continuation: _,
            data,
        } in self.0
        {
            let data = String::from_utf8_lossy(data);
            writeln!(f, "[{level}] [#{sequence_number}] [{timestamp}]: {data}")?;
        }
        Ok(())
    }
}
