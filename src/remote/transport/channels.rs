use super::*;

// --- publisher ---

/// Snapshot of a [`RemotePublisher`]'s lifetime counters.
#[derive(Debug, Default, Clone, Copy)]
pub struct RemotePublisherStats {
    /// Successful `publish()` calls.
    pub published: u64,
    /// Frames the broadcast queue refused (no subscriber attached
    /// at that moment).
    pub dropped: u64,
}

/// Publisher handle returned by [`RemoteTransport::publisher`].
pub struct RemotePublisher<T> {
    pub(crate) tx: broadcast::Sender<Arc<[u8]>>,
    pub(crate) seq: u64,
    pub(crate) published: AtomicU64,
    pub(crate) dropped: AtomicU64,
    pub(crate) _phantom: PhantomData<fn() -> T>,
}

impl<T> RemotePublisher<T> {
    /// Snapshot lifetime counters.
    pub fn stats(&self) -> RemotePublisherStats {
        RemotePublisherStats {
            published: self.published.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
        }
    }
}

/// Owned writable handle the publisher fills before `publish`.
///
/// Mirrors the local transport's header+payload split: `header` is
/// the small Pod metadata (a `T::Header`), `payload` is the
/// variable-length byte buffer.
pub struct RemoteLoan<T: LocalPayload> {
    pub header: T::Header,
    pub payload: Vec<u8>,
}

impl<T: LocalPayload> RemoteLoan<T> {
    fn new(byte_count: usize) -> Self {
        Self {
            header: <T::Header as bytemuck::Zeroable>::zeroed(),
            payload: vec![0u8; byte_count],
        }
    }
}

impl<T: LocalPayload> PublisherOps<T> for RemotePublisher<T> {
    type Loan = RemoteLoan<T>;

    fn loan(&mut self, byte_count: usize) -> Result<Self::Loan> {
        Ok(RemoteLoan::new(byte_count))
    }

    fn publish(&mut self, loan: Self::Loan) -> Result<u64> {
        let header_bytes = bytemuck::bytes_of(&loan.header);
        let mut frame = Vec::with_capacity(header_bytes.len() + loan.payload.len());
        frame.extend_from_slice(header_bytes);
        frame.extend_from_slice(&loan.payload);
        let bytes: Arc<[u8]> = Arc::from(frame.into_boxed_slice());
        // `broadcast::send` returns Err only when there are no
        // subscribers; treat that as a no-op rather than an error,
        // but count the drop so operators can see it.
        if self.tx.send(bytes).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
        self.published.fetch_add(1, Ordering::Relaxed);
        self.seq = self.seq.wrapping_add(1);
        Ok(self.seq)
    }
}

// --- subscriber ---

/// Subscriber handle returned by [`RemoteTransport::subscriber`].
///
/// Multiple subscribers can coexist on the same topic; each has its
/// own broadcast receiver, so a slow subscriber doesn't stall its
/// peers. Each receiver has a fixed-size buffer; if a subscriber
/// falls behind by more than the buffer depth, its next `take()`
/// returns [`Error::Lagged`](crate::Error::Lagged).
pub struct RemoteSubscriber<T> {
    pub(crate) rx: broadcast::Receiver<Arc<[u8]>>,
    pub(crate) received: AtomicU64,
    pub(crate) lagged: AtomicU64,
    pub(crate) disconnects: AtomicU64,
    pub(crate) _phantom: PhantomData<fn() -> T>,
}

/// Snapshot of a [`RemoteSubscriber`]'s lifetime counters.
#[derive(Debug, Default, Clone, Copy)]
pub struct RemoteSubscriberStats {
    /// Samples successfully returned from `take()`.
    pub received: u64,
    /// Total samples dropped by broadcast lag events (sum of `n`
    /// across every `Error::Lagged { dropped: n }` observed).
    pub lagged: u64,
    /// Times the channel was observed closed via
    /// `Error::Disconnected`.
    pub disconnects: u64,
}

impl<T> RemoteSubscriber<T> {
    /// Snapshot lifetime counters.
    pub fn stats(&self) -> RemoteSubscriberStats {
        RemoteSubscriberStats {
            received: self.received.load(Ordering::Relaxed),
            lagged: self.lagged.load(Ordering::Relaxed),
            disconnects: self.disconnects.load(Ordering::Relaxed),
        }
    }
}

/// Owned sample handed back from the subscriber. The wire frame
/// is split into the Pod header (decoded eagerly via
/// `bytemuck::from_bytes`) and the trailing variable-length byte
/// payload (kept as `Vec<u8>` for the caller to reinterpret).
pub struct RemoteSample<T: LocalPayload> {
    pub header: T::Header,
    pub payload: Vec<u8>,
}

impl<T: LocalPayload> RemoteSample<T> {
    pub fn header(&self) -> &T::Header {
        &self.header
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

impl<T: LocalPayload> SubscriberOps<T> for RemoteSubscriber<T> {
    type Sample = RemoteSample<T>;

    fn take(&mut self) -> Result<Option<Self::Sample>> {
        match self.rx.try_recv() {
            Ok(bytes) => {
                let header_size = std::mem::size_of::<T::Header>();
                if bytes.len() < header_size {
                    return Err(Error::Remote(format!(
                        "frame too small: got {} bytes, expected at least {} (header)",
                        bytes.len(),
                        header_size,
                    )));
                }
                let header: T::Header = *bytemuck::from_bytes(&bytes[..header_size]);
                let payload = bytes[header_size..].to_vec();
                self.received.fetch_add(1, Ordering::Relaxed);
                Ok(Some(RemoteSample { header, payload }))
            }
            Err(broadcast::error::TryRecvError::Empty) => Ok(None),
            Err(broadcast::error::TryRecvError::Closed) => {
                self.disconnects.fetch_add(1, Ordering::Relaxed);
                Err(Error::Disconnected)
            }
            Err(broadcast::error::TryRecvError::Lagged(n)) => {
                self.lagged.fetch_add(n, Ordering::Relaxed);
                Err(Error::Lagged { dropped: n })
            }
        }
    }
}

