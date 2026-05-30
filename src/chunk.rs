//! Generic opaque-byte chunking and reassembly.
//!
//! Chunking is transport-level only: chunk bodies are bytes, with no datatype
//! inspection. The high bit of the existing `u32` stream frame length marks a
//! chunk frame, leaving legacy raw frames unchanged.

use std::collections::HashMap;

use crate::error::{Error, Result};
use crate::qos::DeliveryPolicy;

pub const CHUNK_FRAME_FLAG: u32 = 0x8000_0000;
pub const CHUNK_FRAME_LEN_MASK: u32 = 0x7fff_ffff;
pub const CHUNK_HEADER_LEN: usize = 8 + 4 + 4 + 8 + 4;

#[derive(Debug, Clone, Copy)]
pub struct ChunkFrame<'a> {
    pub message_id: u64,
    pub chunk_index: u32,
    pub chunk_count: u32,
    pub message_len: u64,
    pub chunk: &'a [u8],
}

#[derive(Debug)]
pub struct Reassembler {
    policy: DeliveryPolicy,
    max_inflight_bytes: usize,
    messages: HashMap<u64, PartialMessage>,
    inflight_bytes: usize,
    incomplete_dropped: u64,
}

#[derive(Debug)]
struct PartialMessage {
    chunks: Vec<Option<Vec<u8>>>,
    message_len: usize,
    received: usize,
    received_bytes: usize,
}

impl Reassembler {
    pub fn new(policy: DeliveryPolicy, max_inflight_bytes: usize) -> Self {
        Self {
            policy,
            max_inflight_bytes,
            messages: HashMap::new(),
            inflight_bytes: 0,
            incomplete_dropped: 0,
        }
    }

    pub fn incomplete_dropped(&self) -> u64 {
        self.incomplete_dropped
    }

    pub fn push(&mut self, frame: ChunkFrame<'_>) -> Result<Option<Vec<u8>>> {
        validate_chunk_frame(&frame)?;
        let message_len = usize::try_from(frame.message_len).map_err(|_| Error::FrameTooLarge {
            actual: frame.message_len,
            limit: usize::MAX as u64,
        })?;
        if message_len > self.max_inflight_bytes {
            return Err(Error::FrameTooLarge {
                actual: frame.message_len,
                limit: self.max_inflight_bytes as u64,
            });
        }

        if matches!(
            self.policy,
            DeliveryPolicy::Latest | DeliveryPolicy::BestEffort
        ) {
            self.drop_older_than(frame.message_id);
        }

        if !self.messages.contains_key(&frame.message_id) {
            let new_inflight =
                self.inflight_bytes
                    .checked_add(message_len)
                    .ok_or(Error::FrameTooLarge {
                        actual: u64::MAX,
                        limit: self.max_inflight_bytes as u64,
                    })?;
            if new_inflight > self.max_inflight_bytes {
                return Err(Error::FrameTooLarge {
                    actual: new_inflight as u64,
                    limit: self.max_inflight_bytes as u64,
                });
            }
            self.inflight_bytes = new_inflight;
            self.messages.insert(
                frame.message_id,
                PartialMessage {
                    chunks: vec![None; frame.chunk_count as usize],
                    message_len,
                    received: 0,
                    received_bytes: 0,
                },
            );
        }

        let partial = self.messages.get_mut(&frame.message_id).expect("inserted");
        if partial.message_len != message_len || partial.chunks.len() != frame.chunk_count as usize
        {
            return Err(Error::HandshakeMalformed(format!(
                "chunk metadata changed for message {}",
                frame.message_id
            )));
        }

        let idx = frame.chunk_index as usize;
        if partial.chunks[idx].is_none() {
            partial.received += 1;
            partial.received_bytes += frame.chunk.len();
            partial.chunks[idx] = Some(frame.chunk.to_vec());
        }

        if partial.received != partial.chunks.len() {
            return Ok(None);
        }

        if partial.received_bytes != partial.message_len {
            return Err(Error::HandshakeMalformed(format!(
                "reassembled message length mismatch: chunks={} expected={}",
                partial.received_bytes, partial.message_len
            )));
        }

        let partial = self.messages.remove(&frame.message_id).expect("complete");
        self.inflight_bytes = self.inflight_bytes.saturating_sub(partial.message_len);
        let mut out = Vec::with_capacity(partial.message_len);
        for chunk in partial.chunks {
            let chunk = chunk.expect("all chunks present");
            out.extend_from_slice(&chunk);
        }
        Ok(Some(out))
    }

    fn drop_older_than(&mut self, message_id: u64) {
        let old: Vec<_> = self
            .messages
            .keys()
            .copied()
            .filter(|id| *id < message_id)
            .collect();
        for id in old {
            if let Some(partial) = self.messages.remove(&id) {
                self.inflight_bytes = self.inflight_bytes.saturating_sub(partial.message_len);
                self.incomplete_dropped += 1;
            }
        }
    }
}

pub fn make_chunk_payload(
    message_id: u64,
    chunk_index: u32,
    chunk_count: u32,
    message_len: usize,
    chunk: &[u8],
) -> Result<Vec<u8>> {
    if chunk_count == 0 || chunk_index >= chunk_count {
        return Err(Error::HandshakeMalformed(format!(
            "invalid chunk index {chunk_index}/{chunk_count}"
        )));
    }
    if chunk.len() > CHUNK_FRAME_LEN_MASK as usize - CHUNK_HEADER_LEN {
        return Err(Error::FrameTooLarge {
            actual: (chunk.len() + CHUNK_HEADER_LEN) as u64,
            limit: CHUNK_FRAME_LEN_MASK as u64,
        });
    }
    let mut out = Vec::with_capacity(CHUNK_HEADER_LEN + chunk.len());
    out.extend_from_slice(&message_id.to_le_bytes());
    out.extend_from_slice(&chunk_index.to_le_bytes());
    out.extend_from_slice(&chunk_count.to_le_bytes());
    out.extend_from_slice(&(message_len as u64).to_le_bytes());
    out.extend_from_slice(&(chunk.len() as u32).to_le_bytes());
    out.extend_from_slice(chunk);
    Ok(out)
}

pub fn parse_chunk_payload(bytes: &[u8]) -> Result<ChunkFrame<'_>> {
    if bytes.len() < CHUNK_HEADER_LEN {
        return Err(Error::HandshakeMalformed(format!(
            "chunk frame truncated: {} bytes",
            bytes.len()
        )));
    }
    let message_id = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
    let chunk_index = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    let chunk_count = u32::from_le_bytes(bytes[12..16].try_into().unwrap());
    let message_len = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
    let chunk_len = u32::from_le_bytes(bytes[24..28].try_into().unwrap()) as usize;
    let end = CHUNK_HEADER_LEN.saturating_add(chunk_len);
    if bytes.len() < end {
        return Err(Error::HandshakeMalformed(format!(
            "chunk body truncated: declared {} bytes, have {}",
            chunk_len,
            bytes.len().saturating_sub(CHUNK_HEADER_LEN)
        )));
    }
    let frame = ChunkFrame {
        message_id,
        chunk_index,
        chunk_count,
        message_len,
        chunk: &bytes[CHUNK_HEADER_LEN..end],
    };
    validate_chunk_frame(&frame)?;
    Ok(frame)
}

fn validate_chunk_frame(frame: &ChunkFrame<'_>) -> Result<()> {
    if frame.chunk_count == 0 {
        return Err(Error::HandshakeMalformed(
            "chunk_count must be greater than zero".to_string(),
        ));
    }
    if frame.chunk_index >= frame.chunk_count {
        return Err(Error::HandshakeMalformed(format!(
            "chunk_index {} out of {}",
            frame.chunk_index, frame.chunk_count
        )));
    }
    if frame.chunk.len() as u64 > frame.message_len {
        return Err(Error::HandshakeMalformed(format!(
            "chunk_len {} exceeds message_len {}",
            frame.chunk.len(),
            frame.message_len
        )));
    }
    Ok(())
}
