//! Messages aggregator.

use std::{
    collections::{hash_map::Entry, HashMap, VecDeque},
    convert::identity,
    time::Duration,
};

use color_eyre::Result;
use tokio::time::Instant;

use crate::{Continuation, Fragments, RawParsedEntry, SingleMessage};

/// Per-message fragments aggregator.
struct FragmentsAggregator {
    level: u8,
    timestamp: u64,
    continuation: Continuation,
    fragments: Fragments,
    begin: Instant,
}

type SequenceNumber = u64;

impl FragmentsAggregator {
    /// Creates a fragment aggregator from the first fragment.
    pub fn new(
        level: u8,
        timestamp: u64,
        continuation: Continuation,
        byte_offset: u32,
        total_bytes: u32,
        data: &[u8],
    ) -> Self {
        let mut fragments = Fragments::with_placeholder(total_bytes as usize, b'.');
        fragments
            .insert(byte_offset as usize, data)
            .expect("Insertion of the first fragment can't fail");
        Self {
            level,
            timestamp,
            continuation,
            fragments,
            begin: Instant::now(),
        }
    }

    /// Records a fragment.
    pub fn add_fragment(&mut self, byte_offset: u32, data: &[u8]) -> Result<()> {
        self.fragments.insert(byte_offset as usize, data)?;
        Ok(())
    }

    /// Whether all the required fragments have been collected.
    pub fn is_done(&self) -> bool {
        self.fragments.is_done()
    }

    /// Consumes the internal buffer and returns a fully formed message.
    pub fn consume(self, sequence_number: SequenceNumber) -> (Instant, SingleMessage) {
        (
            self.begin,
            SingleMessage {
                level: self.level,
                sequence_number,
                timestamp: self.timestamp,
                continuation: self.continuation,
                data: self.fragments.consume(),
            },
        )
    }
}

/// Message aggregator.
pub struct MessageAggregator<F> {
    fragments: HashMap<SequenceNumber, FragmentsAggregator>,
    state: State,
    message_processor: F,
    cached: VecDeque<(Instant, SingleMessage)>,
    last_processed_message: SequenceNumber,
}

enum State {
    Empty,
    Aggregating {
        begin_at: Instant,
        messages: Vec<SingleMessage>,
    },
}

impl Default for State {
    fn default() -> Self {
        State::Empty
    }
}

impl<F> MessageAggregator<F>
where
    F: MessageProcessor,
{
    /// Constructs a new message aggregator.
    pub fn new(message_processor: F) -> Self {
        Self {
            fragments: <_>::default(),
            state: <_>::default(),
            message_processor,
            cached: <_>::default(),
            last_processed_message: u64::MAX,
        }
    }

    /// Processes a single parsed packet.
    pub fn process(&mut self, raw: RawParsedEntry<'_>) -> Result<()> {
        if let Some(fragment_info) = raw.fragment_information {
            match self.fragments.entry(raw.sequence_number) {
                Entry::Occupied(mut occupied) => {
                    let fragments = occupied.get_mut();
                    fragments.add_fragment(fragment_info.byte_offset, raw.data)?;
                    if fragments.is_done() {
                        // Yay, we're done!
                        let fragments = occupied.remove();
                        let (received_at, data) = fragments.consume(raw.sequence_number);
                        self.add_message_cached(data, received_at);
                    }
                }
                Entry::Vacant(empty) => {
                    empty.insert(FragmentsAggregator::new(
                        raw.level,
                        raw.timestamp,
                        raw.continuation,
                        fragment_info.byte_offset,
                        fragment_info.total_bytes,
                        raw.data,
                    ));
                }
            }
        } else {
            self.add_message_cached(
                SingleMessage {
                    level: raw.level,
                    sequence_number: raw.sequence_number,
                    timestamp: raw.timestamp,
                    continuation: raw.continuation,
                    data: raw.data.to_vec(),
                },
                Instant::now(),
            )
        }

        Ok(())
    }

    fn add_message_cached(&mut self, message: SingleMessage, received_at: Instant) {
        if self.cached.is_empty()
            && self.last_processed_message.wrapping_add(1) == message.sequence_number
        {
            // No need to cache, messages are already ordered.
            self.add_message(message);
            return;
        }
        // We don't report messages immediately since they might be re-ordered.
        let position = self
            .cached
            .binary_search_by_key(&message.sequence_number, |(_instant, message)| {
                message.sequence_number
            })
            .unwrap_or_else(identity);
        self.cached.insert(position, (received_at, message));
    }

    fn add_message(&mut self, message: SingleMessage) {
        self.last_processed_message = message.sequence_number;
        match message.continuation {
            Continuation::No => {
                // Previously stored messages are discarded.
                self.flush_continuation();
                self.message_processor.process_one(message);
            }
            Continuation::Begin => {
                // Previously stored messages are discarded.
                self.flush_continuation();
                self.state = State::Aggregating {
                    begin_at: Instant::now(),
                    messages: vec![message],
                };
            }
            Continuation::Continue => {
                match &mut self.state {
                    State::Empty => {
                        // This is unexpected => the first message is lost.
                        tracing::warn!(
                            "First message from the continued set of messages is probably lost."
                        );
                        self.state = State::Aggregating {
                            begin_at: Instant::now(),
                            messages: vec![message],
                        }
                    }
                    State::Aggregating {
                        messages: aggregating,
                        ..
                    } => aggregating.push(message),
                }
            }
        }
    }

    pub fn process_timeouts(&mut self) {
        const COMBINE_FOR: Duration = Duration::from_secs(5);
        const CONTINUATION_TIMEOUT: Duration = Duration::from_secs(7);

        self.flush_fragments(COMBINE_FOR);
        let now = Instant::now();
        while let Some((added, message)) = self.cached.pop_front() {
            if now.duration_since(added) < COMBINE_FOR {
                // Stop!
                self.cached.push_front((added, message));
                break;
            }
            self.add_message(message);
        }

        // Now let's check if we need to flush the continuation.
        match std::mem::take(&mut self.state) {
            State::Empty => { /* No op */ }
            State::Aggregating {
                begin_at,
                mut messages,
            } => {
                if now.duration_since(begin_at) > CONTINUATION_TIMEOUT {
                    // We've been waiting for too long.
                    if messages.len() == 1 {
                        let message = messages.pop().expect("There is a message");
                        self.message_processor.process_one(message);
                    } else {
                        self.message_processor.process_many(messages)
                    }
                } else {
                    // We can wait more!
                    self.state = State::Aggregating { begin_at, messages }
                }
            }
        }
    }

    /// Calls processor on the currently cached messages.
    fn flush_continuation(&mut self) {
        match std::mem::take(&mut self.state) {
            State::Empty => { /* No op */ }
            State::Aggregating { mut messages, .. } => {
                if messages.len() == 1 {
                    let message = messages.pop().expect("There is a message");
                    self.message_processor.process_one(message);
                } else {
                    self.message_processor.process_many(messages)
                }
            }
        }
    }

    /// Flushes all the not yet fragments that are older than the `oldest`.
    fn flush_fragments(&mut self, oldest: Duration) {
        let mut fragments = HashMap::new();
        for (sequence_number, aggregator) in std::mem::take(&mut self.fragments) {
            if aggregator.begin.elapsed() >= oldest {
                fragments.insert(sequence_number, aggregator);
            } else {
                // Outdated. Flush!
                tracing::warn!("Message #{sequence_number} hasn't been fully received");
                let (received_at, message) = aggregator.consume(sequence_number);
                self.add_message_cached(message, received_at);
            }
        }
        self.fragments = fragments;
    }
}

/// A trait for message processing.
pub trait MessageProcessor {
    /// A single message is available.
    fn process_one(&mut self, message: SingleMessage);

    /// A bunch of connected messages are available.
    fn process_many(&mut self, messages: Vec<SingleMessage>);
}
