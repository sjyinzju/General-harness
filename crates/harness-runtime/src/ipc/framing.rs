//! Length-prefix protocol framing for IPC messages.
//!
//! Frame format:
//! ```text
//! [4 bytes: length (u32, big-endian)] [variable-length: UTF-8 JSON payload]
//! ```
//!
//! A single read may return a partial frame; framing handles reassembly.
//! Frames exceeding the maximum size are rejected.

use harness_core::{CoreError, ErrorCode, ErrorSource};

use super::transport::IpcConnection;

/// Represents a frame that was too large.
pub struct FrameTooLarge(pub usize);

/// Read a complete frame from the connection.
///
/// Reads the 4-byte length prefix, then reads exactly that many bytes
/// of payload. Rejects frames larger than `max_size`.
pub async fn read_frame(
    conn: &mut IpcConnection,
    max_size: usize,
) -> Result<Vec<u8>, super::FrameReadError> {
    // Read 4-byte length prefix
    let mut len_buf = [0u8; 4];
    match conn.read_exact(&mut len_buf).await {
        Ok(()) => {}
        Err(e) => {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                return Err(super::FrameReadError::Eof);
            }
            return Err(super::FrameReadError::Error(CoreError::new(
                ErrorCode::ProtocolError,
                format!("frame length read error: {e}"),
                ErrorSource::System,
            )));
        }
    }

    let frame_len = u32::from_be_bytes(len_buf) as usize;

    if frame_len > max_size {
        // Drain the oversized frame to keep the connection clean
        let mut drain = vec![0u8; max_size.min(65536)];
        let mut remaining = frame_len;
        while remaining > 0 {
            let to_read = remaining.min(drain.len());
            let drain_slice = &mut drain[..to_read];
            let n = conn.read(drain_slice).await.map_err(|e| {
                CoreError::new(
                    ErrorCode::ProtocolError,
                    format!("drain error: {e}"),
                    ErrorSource::System,
                )
            })?;
            if n == 0 {
                break;
            }
            remaining -= n;
        }
        return Err(super::FrameReadError::TooLarge(frame_len));
    }

    // Read the payload
    let mut payload = vec![0u8; frame_len];
    conn.read_exact(&mut payload).await.map_err(|e| {
        super::FrameReadError::Error(CoreError::new(
            ErrorCode::ProtocolError,
            format!("frame payload read error: {e}"),
            ErrorSource::System,
        ))
    })?;

    Ok(payload)
}

/// Write a complete frame to the connection.
///
/// Writes the 4-byte length prefix, then the payload bytes.
pub async fn write_frame(conn: &mut IpcConnection, payload: &[u8]) -> Result<(), CoreError> {
    let len = payload.len() as u32;
    let len_bytes = len.to_be_bytes();

    // Write length prefix
    conn.write_all(&len_bytes).await.map_err(|e| {
        CoreError::new(
            ErrorCode::ProtocolError,
            format!("frame length write error: {e}"),
            ErrorSource::System,
        )
    })?;

    // Write payload
    conn.write_all(payload).await.map_err(|e| {
        CoreError::new(
            ErrorCode::ProtocolError,
            format!("frame payload write error: {e}"),
            ErrorSource::System,
        )
    })?;

    conn.flush().await.map_err(|e| {
        CoreError::new(
            ErrorCode::ProtocolError,
            format!("frame flush error: {e}"),
            ErrorSource::System,
        )
    })?;

    Ok(())
}
