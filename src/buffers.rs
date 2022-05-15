//! Buffers passing utilities.

use std::ops::{Deref, DerefMut};

use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

/// A RAII buffer.
pub struct Buffer {
    inner: Vec<u8>,
    sender: UnboundedSender<Vec<u8>>,
}

impl Deref for Buffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for Buffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Buffer {
    /// Maximum UDP payload size is slightly smaller, but who cares.
    pub const BUFFER_SIZE: usize = 2usize << 16;

    /// Resizes the buffer.
    pub fn resize(&mut self, new_size: usize) {
        self.inner.resize(new_size, 0)
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        let inner = std::mem::take(&mut self.inner);
        // We don't care if the receiving end has disappeared.
        let _ = self.sender.send(inner);
    }
}

/// Buffers manager.
pub struct Buffers {
    receiver: UnboundedReceiver<Vec<u8>>,
    sender: UnboundedSender<Vec<u8>>,
}

impl Buffers {
    /// Creates a buffer manager with the given amount of underlying buffers.
    pub fn new(buffers_count: usize) -> Self {
        let (sender, receiver) = unbounded_channel();
        for _ in 0..buffers_count {
            sender
                .send(Vec::<u8>::with_capacity(Buffer::BUFFER_SIZE))
                .expect("The operation can't fail");
        }
        Self { receiver, sender }
    }

    /// Waits for a buffer to become available.
    pub async fn receive(&mut self) -> Buffer {
        let mut inner = self
            .receiver
            .recv()
            .await
            .expect("There definitely exists at least one sender");
        inner.resize(Buffer::BUFFER_SIZE, 0);
        Buffer {
            inner,
            sender: self.sender.clone(),
        }
    }
}
