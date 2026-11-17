//! Canonical bounded framing shared by native runner control and retained
//! acceptance roles. Authentication and message semantics belong to callers.
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};

pub(crate) fn encode<T: Serialize>(value: &T, limit: usize) -> Result<Vec<u8>> {
    let bytes = serde_json::to_vec(value)?;
    ensure!(
        !bytes.is_empty() && bytes.len() <= limit,
        "runner frame exceeds limit"
    );
    let mut frame = u32::try_from(bytes.len())?.to_be_bytes().to_vec();
    frame.extend(bytes);
    Ok(frame)
}

pub(crate) fn decode<T: Serialize + for<'a> Deserialize<'a>>(
    bytes: &[u8],
    limit: usize,
) -> Result<T> {
    ensure!(
        bytes.len() > 4 && bytes.len() - 4 <= limit,
        "invalid runner frame"
    );
    let size = u32::from_be_bytes(bytes[..4].try_into()?) as usize;
    ensure!(size == bytes.len() - 4, "runner frame length mismatch");
    let value = serde_json::from_slice(&bytes[4..])?;
    ensure!(encode(&value, limit)? == bytes, "noncanonical runner frame");
    Ok(value)
}
