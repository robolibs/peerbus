//! Data-agnostic topic quality-of-service knobs.
//!
//! These policies describe transport behavior for opaque bytes. They do not
//! imply compression, codecs, semantic deltas, or datatype-specific logic.

/// Delivery behavior for a topic when receivers cannot keep up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryPolicy {
    /// Preserve every message while transport limits allow it.
    Reliable,
    /// Freshness wins: slow receivers may skip older queued samples.
    Latest,
    /// Loss is acceptable; future datagram/chunked paths can use this.
    BestEffort,
}

impl DeliveryPolicy {
    pub(crate) fn to_wire(self) -> u8 {
        match self {
            Self::Reliable => 0,
            Self::Latest => 1,
            Self::BestEffort => 2,
        }
    }

    pub(crate) fn from_wire(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Reliable),
            1 => Some(Self::Latest),
            2 => Some(Self::BestEffort),
            _ => None,
        }
    }
}

/// Generic per-topic transport QoS.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TopicQos {
    pub delivery: DeliveryPolicy,
    pub max_message_bytes: usize,
    pub max_inflight_bytes: usize,
    pub chunk_bytes: usize,
    pub subscriber_queue: usize,
    pub priority: u8,
}

impl Default for TopicQos {
    fn default() -> Self {
        Self {
            delivery: DeliveryPolicy::Reliable,
            max_message_bytes: 16 * 1024 * 1024,
            max_inflight_bytes: 64 * 1024 * 1024,
            chunk_bytes: 256 * 1024,
            subscriber_queue: 256,
            priority: 0,
        }
    }
}

impl TopicQos {
    pub fn reliable() -> Self {
        Self::default()
    }

    pub fn latest() -> Self {
        Self {
            delivery: DeliveryPolicy::Latest,
            ..Self::default()
        }
    }

    pub fn best_effort() -> Self {
        Self {
            delivery: DeliveryPolicy::BestEffort,
            ..Self::default()
        }
    }

    pub fn with_max_message_bytes(mut self, max_message_bytes: usize) -> Self {
        self.max_message_bytes = max_message_bytes;
        self
    }

    pub fn with_max_inflight_bytes(mut self, max_inflight_bytes: usize) -> Self {
        self.max_inflight_bytes = max_inflight_bytes;
        self
    }

    pub fn with_chunk_bytes(mut self, chunk_bytes: usize) -> Self {
        self.chunk_bytes = chunk_bytes;
        self
    }

    pub fn with_subscriber_queue(mut self, subscriber_queue: usize) -> Self {
        self.subscriber_queue = subscriber_queue;
        self
    }

    pub fn with_priority(mut self, priority: u8) -> Self {
        self.priority = priority;
        self
    }
}
