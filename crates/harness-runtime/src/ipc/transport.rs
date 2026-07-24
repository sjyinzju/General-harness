//! Platform-abstract IPC transport layer.
//!
//! Windows: Named Pipe (same-user access, reject remote clients)
//!
//! The transport provides a bidirectional byte stream with:
//! - read_exact, read, write_all, flush operations
//! - connection identification (for logging/diagnostics)

use std::io;

/// A bidirectional IPC connection.
///
/// Wraps both server-side (accepted) and client-side (connected) connections.
pub struct IpcConnection {
    inner: PipeInner,
    label: String,
}

enum PipeInner {
    #[cfg(windows)]
    Server(tokio::net::windows::named_pipe::NamedPipeServer),
    #[cfg(windows)]
    Client(tokio::net::windows::named_pipe::NamedPipeClient),
}

impl IpcConnection {
    #[cfg(windows)]
    fn from_server(pipe: tokio::net::windows::named_pipe::NamedPipeServer, label: &str) -> Self {
        Self {
            inner: PipeInner::Server(pipe),
            label: label.to_string(),
        }
    }

    #[cfg(windows)]
    fn from_client(pipe: tokio::net::windows::named_pipe::NamedPipeClient, label: &str) -> Self {
        Self {
            inner: PipeInner::Client(pipe),
            label: label.to_string(),
        }
    }

    /// Get a connection identifier for logging.
    pub fn id(&self) -> String {
        self.label.clone()
    }

    /// Read exactly `buf.len()` bytes.
    pub async fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        let expected = buf.len();
        let n = match &mut self.inner {
            #[cfg(windows)]
            PipeInner::Server(pipe) => tokio::io::AsyncReadExt::read_exact(pipe, buf).await?,
            #[cfg(windows)]
            PipeInner::Client(pipe) => tokio::io::AsyncReadExt::read_exact(pipe, buf).await?,
        };
        if n != expected {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("read_exact: expected {expected} bytes, got {n}"),
            ));
        }
        Ok(())
    }

    /// Read some bytes into buf. Returns number of bytes read.
    pub async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match &mut self.inner {
            #[cfg(windows)]
            PipeInner::Server(pipe) => tokio::io::AsyncReadExt::read(pipe, buf).await,
            #[cfg(windows)]
            PipeInner::Client(pipe) => tokio::io::AsyncReadExt::read(pipe, buf).await,
        }
    }

    /// Write all bytes.
    pub async fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        match &mut self.inner {
            #[cfg(windows)]
            PipeInner::Server(pipe) => tokio::io::AsyncWriteExt::write_all(pipe, buf).await,
            #[cfg(windows)]
            PipeInner::Client(pipe) => tokio::io::AsyncWriteExt::write_all(pipe, buf).await,
        }
    }

    /// Flush output.
    pub async fn flush(&mut self) -> io::Result<()> {
        match &mut self.inner {
            #[cfg(windows)]
            PipeInner::Server(pipe) => tokio::io::AsyncWriteExt::flush(pipe).await,
            #[cfg(windows)]
            PipeInner::Client(pipe) => tokio::io::AsyncWriteExt::flush(pipe).await,
        }
    }
}

/// IPC listener that accepts incoming connections.
pub struct IpcListener {
    #[cfg(windows)]
    pipe_name: String,
    counter: u64,
}

impl IpcListener {
    /// Bind to an IPC endpoint.
    ///
    /// On Windows, `endpoint` is the pipe name (e.g., "harness-supervisor").
    /// The actual pipe path will be `\\.\pipe\<endpoint>`.
    pub async fn bind(endpoint: &str) -> io::Result<Self> {
        #[cfg(windows)]
        {
            let pipe_name = format!(r"\\.\pipe\{}", endpoint);
            tracing::info!(pipe_name = %pipe_name, "IPC listener bound");
            Ok(IpcListener {
                pipe_name,
                counter: 0,
            })
        }
        #[cfg(not(windows))]
        {
            let _ = endpoint;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "Unix IPC not yet implemented",
            ))
        }
    }

    /// Accept an incoming connection.
    pub async fn accept(&mut self) -> io::Result<IpcConnection> {
        #[cfg(windows)]
        {
            // Create a new NamedPipeServer for each connection.
            let server = tokio::net::windows::named_pipe::ServerOptions::new()
                .first_pipe_instance(false)
                .reject_remote_clients(true)
                .create(&self.pipe_name)?;

            server.connect().await?;
            self.counter += 1;

            let label = format!("pipe-{}", self.counter);
            tracing::debug!(conn_id = %label, "named pipe client connected");
            Ok(IpcConnection::from_server(server, &label))
        }
        #[cfg(not(windows))]
        {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "Unix IPC not yet implemented",
            ))
        }
    }
}

/// IPC client for connecting to a supervisor.
pub struct IpcClient;

impl IpcClient {
    /// Connect to the supervisor IPC endpoint.
    ///
    /// On Windows, `endpoint` is the pipe name (e.g., "harness-supervisor").
    pub async fn connect(endpoint: &str) -> io::Result<IpcConnection> {
        #[cfg(windows)]
        {
            let pipe_name = format!(r"\\.\pipe\{}", endpoint);
            let client = tokio::net::windows::named_pipe::ClientOptions::new().open(&pipe_name)?;

            let label = format!("client-pipe-{}", std::process::id());
            tracing::debug!(conn_id = %label, "connected to IPC server");
            Ok(IpcConnection::from_client(client, &label))
        }
        #[cfg(not(windows))]
        {
            let _ = endpoint;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "Unix IPC not yet implemented",
            ))
        }
    }
}
