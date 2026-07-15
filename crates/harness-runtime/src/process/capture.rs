//! Stream capture — independent stdout/stderr readers with bounded channels,
//! memory thresholds, spool files, and byte limits.
//!
//! Pipeline per stream:
//!   pipe → reader task (8 KiB chunks) → bounded mpsc(64) → sink task
//!
//! The reader NEVER blocks on storage: the bounded channel provides
//! backpressure against the sink, and the sink is either memory or a local
//! file append. A flooding child can therefore never deadlock the harness.
//!
//! Sink behavior:
//! - `CapturePolicy::Pipe`  — count bytes + keep a bounded preview. Bytes past
//!   `byte_limit` are counted, marked truncated, and dropped (never stored in
//!   an unbounded Vec).
//! - `CapturePolicy::Spool` — buffer in memory up to `max_memory_bytes`; once
//!   exceeded, flush to `<spool_dir>/<stream>.spool` and stream the rest to
//!   the file. Storage stops at `byte_limit` (truncated), but the pipe keeps
//!   draining so the child never blocks. On EOF a `<stream>.spool.meta.json`
//!   metadata file is written next to the spool.
//!
//! The preview (first `PREVIEW_LIMIT` bytes, lossy UTF-8) is redacted with the
//! execution's `ProcessEventRedactor` before it leaves this module.

use std::path::PathBuf;
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::redactor::ProcessEventRedactor;

const CHUNK_SIZE: usize = 8 * 1024;
const CHANNEL_CAPACITY: usize = 64;
const PREVIEW_LIMIT: usize = 2048;

#[derive(Debug, Clone)]
pub struct StreamCaptureConfig {
    pub execution_id: String,
    /// "stdout" or "stderr" — used for spool file naming and tracing.
    pub stream_name: &'static str,
    /// In-memory threshold before spilling to the spool file. `None` = pure
    /// in-memory accounting (CapturePolicy::Pipe).
    pub spool_after_bytes: Option<usize>,
    /// Directory for spool files. Required when `spool_after_bytes` is set.
    pub spool_dir: Option<PathBuf>,
    /// Hard cap on stored bytes. Bytes beyond it are counted + dropped.
    pub byte_limit: usize,
    pub redactor: Arc<ProcessEventRedactor>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StreamCaptureResult {
    /// Total bytes received from the pipe (draining continues past limits).
    pub total_bytes: u64,
    /// Bytes actually persisted (memory-accounted or written to spool).
    pub stored_bytes: u64,
    /// True when `total_bytes` exceeded `byte_limit`.
    pub truncated: bool,
    /// Spool file path when the memory threshold was exceeded.
    pub spool_ref: Option<String>,
    /// First bytes of the stream, lossy-decoded and redacted.
    pub preview: String,
}

pub struct StreamCaptureHandle {
    reader: JoinHandle<()>,
    sink: JoinHandle<StreamCaptureResult>,
}

impl StreamCaptureHandle {
    /// Wait for the stream to drain (EOF) and return the capture result.
    /// `drain_timeout` bounds the wait: if descendants still hold the pipe
    /// open after the process tree was killed, the reader is aborted (which
    /// closes the channel) and the sink finalizes with what it has.
    pub async fn finish(self, drain_timeout: std::time::Duration) -> StreamCaptureResult {
        let StreamCaptureHandle { reader, mut sink } = self;
        let empty = || StreamCaptureResult {
            total_bytes: 0,
            stored_bytes: 0,
            truncated: false,
            spool_ref: None,
            preview: String::new(),
        };
        tokio::select! {
            res = &mut sink => {
                reader.abort();
                res.unwrap_or_else(|_| empty())
            }
            () = tokio::time::sleep(drain_timeout) => {
                // Pipe still held open (e.g. surviving descendant). Abort the
                // reader; the sink sees channel close, drains buffered chunks,
                // and returns partial results.
                reader.abort();
                match tokio::time::timeout(std::time::Duration::from_secs(5), &mut sink).await {
                    Ok(Ok(result)) => result,
                    _ => {
                        sink.abort();
                        empty()
                    }
                }
            }
        }
    }
}

/// Spawn the reader + sink tasks for one stream.
pub fn spawn_stream_capture<R>(source: R, cfg: StreamCaptureConfig) -> StreamCaptureHandle
where
    R: AsyncRead + Unpin + Send + 'static,
{
    let (tx, rx) = mpsc::channel::<Vec<u8>>(CHANNEL_CAPACITY);

    let reader = tokio::spawn(async move {
        let mut source = source;
        let mut buf = vec![0u8; CHUNK_SIZE];
        loop {
            match source.read(&mut buf).await {
                Ok(0) | Err(_) => break, // EOF or broken pipe
                Ok(n) => {
                    if tx.send(buf[..n].to_vec()).await.is_err() {
                        break; // sink gone
                    }
                }
            }
        }
    });

    let sink = tokio::spawn(run_sink(rx, cfg));

    StreamCaptureHandle { reader, sink }
}

async fn run_sink(
    mut rx: mpsc::Receiver<Vec<u8>>,
    cfg: StreamCaptureConfig,
) -> StreamCaptureResult {
    let mut total: u64 = 0;
    let mut stored: u64 = 0;
    let mut truncated = false;
    let mut preview: Vec<u8> = Vec::new();
    let mut mem_buf: Vec<u8> = Vec::new();
    let mut spool_file: Option<tokio::fs::File> = None;
    let mut spool_path: Option<PathBuf> = None;

    while let Some(chunk) = rx.recv().await {
        total += chunk.len() as u64;

        if preview.len() < PREVIEW_LIMIT {
            let take = (PREVIEW_LIMIT - preview.len()).min(chunk.len());
            preview.extend_from_slice(&chunk[..take]);
        }

        // How much of this chunk may still be stored under the byte limit?
        let remaining = (cfg.byte_limit as u64).saturating_sub(stored) as usize;
        if remaining == 0 {
            truncated = true;
            continue; // keep draining, store nothing
        }
        let storable = &chunk[..remaining.min(chunk.len())];
        if storable.len() < chunk.len() {
            truncated = true;
        }

        match cfg.spool_after_bytes {
            None => {
                // Pipe policy: memory accounting only (no unbounded Vec).
                stored += storable.len() as u64;
            }
            Some(threshold) => {
                if spool_file.is_none() && mem_buf.len() + storable.len() > threshold {
                    // Spill: open spool file, flush the memory buffer.
                    let dir = cfg
                        .spool_dir
                        .clone()
                        .expect("spool policy requires spool_dir (validated at spawn)");
                    let path = dir.join(format!("{}.spool", cfg.stream_name));
                    match tokio::fs::File::create(&path).await {
                        Ok(mut f) => {
                            if f.write_all(&mem_buf).await.is_ok() {
                                spool_file = Some(f);
                                spool_path = Some(path);
                                mem_buf.clear();
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                execution_id = %cfg.execution_id,
                                stream = cfg.stream_name,
                                error = %e,
                                "spool_file_create_failed; draining without storage"
                            );
                            truncated = true;
                            continue;
                        }
                    }
                }
                if let Some(f) = spool_file.as_mut() {
                    if f.write_all(storable).await.is_ok() {
                        stored += storable.len() as u64;
                    } else {
                        truncated = true;
                    }
                } else {
                    mem_buf.extend_from_slice(storable);
                    stored += storable.len() as u64;
                }
            }
        }
    }

    if let Some(f) = spool_file.as_mut() {
        let _ = f.flush().await;
    }

    // Spool metadata/reference.
    if let (Some(path), Some(dir)) = (&spool_path, &cfg.spool_dir) {
        let meta = serde_json::json!({
            "execution_id": cfg.execution_id,
            "stream": cfg.stream_name,
            "total_bytes": total,
            "stored_bytes": stored,
            "truncated": truncated,
            "spool_file": path.file_name().and_then(|n| n.to_str()),
            "created_at": chrono::Utc::now().to_rfc3339(),
        });
        let meta_path = dir.join(format!("{}.spool.meta.json", cfg.stream_name));
        let _ = tokio::fs::write(&meta_path, meta.to_string()).await;
    }

    let preview = cfg.redactor.redact_lossy(&preview);
    tracing::debug!(
        execution_id = %cfg.execution_id,
        stream = cfg.stream_name,
        total_bytes = total,
        stored_bytes = stored,
        truncated,
        "stream_capture_complete"
    );

    StreamCaptureResult {
        total_bytes: total,
        stored_bytes: stored,
        truncated,
        spool_ref: spool_path.map(|p| p.to_string_lossy().into_owned()),
        preview,
    }
}
