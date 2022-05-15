//! Helper utilities.

mod aggregator;
mod fragments;
mod parse;

pub use aggregator::{MessageAggregator, MessageProcessor};
pub use fragments::Fragments;
pub use parse::parse;

/// Meta-information when a message is fragmented.
pub struct FragmentInformation {
    pub byte_offset: u32,
    pub total_bytes: u32,
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
    pub level: u8,
    pub sequence_number: u64,
    pub timestamp: u64,
    pub continuation: Continuation,
    pub fragment_information: Option<FragmentInformation>,
    pub data: &'a [u8],
}

/// A single message, possibly reconstructed from fragments.
pub struct SingleMessage {
    pub level: u8,
    pub sequence_number: u64,
    pub timestamp: u64,
    pub continuation: Continuation,
    pub data: Vec<u8>,
}
