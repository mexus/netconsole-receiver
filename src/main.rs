use std::{
    collections::HashMap,
    fmt,
    net::{IpAddr, SocketAddr},
    time::Duration,
};

use color_eyre::{
    eyre::{ensure, Context},
    Result,
};
use itertools::Itertools;
use netconsole_receiver::{
    parse, Buffer, Buffers, MessageAggregator, MessageProcessor, SingleMessage,
};
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

/// Receives and parses netconsole extended messages.
#[derive(StructOpt)]
struct Args {
    /// UDP address to bind to.
    listen_address: SocketAddr,

    /// IP addresses to receive data from.
    source_addresses: Vec<IpAddr>,

    /// Amount of binding attempts when listen address is not available.
    #[structopt(long, default_value = "10")]
    bind_attempts: usize,
}

async fn run() -> Result<()> {
    let Args {
        listen_address,
        source_addresses,
        bind_attempts,
    } = Args::from_args();

    ensure!(
        !source_addresses.is_empty(),
        "At least one source address must be provided"
    );

    let mut attempt = 0;
    let socket = loop {
        attempt += 1;
        match UdpSocket::bind(listen_address).await {
            Ok(socket) => break socket,
            Err(e) if e.kind() == std::io::ErrorKind::AddrNotAvailable => {
                const ATTEMPTS_INTERVAL: Duration = Duration::from_secs(5);
                tracing::warn!(
                    "Attempt {attempt}. Binding address {listen_address} is not available."
                );
                ensure!(
                    attempt < bind_attempts,
                    "Attempts to bind to {listen_address} exceeded"
                );
                tracing::info!("Waiting for {ATTEMPTS_INTERVAL:?} before the next attempt.");
                tokio::time::sleep(ATTEMPTS_INTERVAL).await;
            }
            Err(e) => match e.kind() {
                std::io::ErrorKind::AddrNotAvailable => {}
                _ => {
                    return Err(color_eyre::Report::new(e)
                        .wrap_err(format!("Unable to bind to {listen_address}")))
                }
            },
        }
    };

    tracing::info!("Listening at {listen_address} (UDP)");
    tracing::info!(
        "Will accept data from the following IPs (UDP): {}",
        source_addresses.iter().format(",")
    );

    let mut buffers = Buffers::new(source_addresses.len());

    let mut processors = source_addresses
        .into_iter()
        .map(|source| {
            let (sender, receiver) = tokio::sync::mpsc::channel(1);
            tokio::spawn(process_source(source, receiver));
            (source, sender)
        })
        .collect::<HashMap<_, _>>();

    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);

    loop {
        let mut buffer = tokio::select! {
            buffer = buffers.receive() => buffer,
            _ = &mut ctrl_c => {
                tracing::info!("Terminating");
                drop(buffers);
                break;
            }
        };

        let result = tokio::select! {
            result = socket.recv_from(&mut buffer) => result,
            _ = &mut ctrl_c => {
                tracing::info!("Terminating");
                drop(buffers);
                break;
            }
        };
        let (len, source) = match result {
            Ok((len, source)) => (len, source),
            Err(e) => {
                tracing::error!("Unable to receive data from the socket: {e:#}");
                continue;
            }
        };
        buffer.resize(len);
        let source = source.ip();
        if let Some(sender) = processors.get(&source) {
            if sender.send(buffer).await.is_err() {
                tracing::warn!("Processor for {source} is dead");
                processors.remove(&source);
            }
        } else {
            // Unknown source.
            tracing::warn!("Received data from unregistered source {source}");
        };
    }
    Ok(())
}

async fn process_source(source: IpAddr, mut incoming: tokio::sync::mpsc::Receiver<Buffer>) {
    const TICK_TIMEOUT: Duration = Duration::from_secs(5);

    let mut aggregator = MessageAggregator::new(MessageTracing { source });

    loop {
        let data = match tokio::time::timeout(TICK_TIMEOUT, incoming.recv()).await {
            Ok(Some(buffer)) => buffer,
            Ok(None) => {
                // Channel closed.
                aggregator.force_flush();
                break;
            }
            Err(_timeout) => {
                tracing::debug!("No data for quite some time from {source} ({TICK_TIMEOUT:?}) ...");
                aggregator.process_timeouts();
                continue;
            }
        };
        match parse(&data) {
            Ok(raw) => {
                let sequence_number = raw.sequence_number();
                if let Err(e) = aggregator.process(raw) {
                    tracing::warn!(
                        "Unable to process a message #{sequence_number} from {source}: {e:#}"
                    )
                }
            }
            Err(e) => tracing::warn!("Unable to parse incoming entry: {e:#}"),
        };
    }
}

struct MessageTracing {
    source: IpAddr,
}

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
        let source = &self.source;
        tracing::info!("[{source}] [{level}] [#{sequence_number}] [{timestamp}]: {data}");
    }

    fn process_many(&mut self, messages: Vec<SingleMessage>) {
        let source = &self.source;
        tracing::info!("[{source}] {}", FormatChunk(&messages));
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
