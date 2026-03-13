//! QUIC client for sending FlowFiles to remote RuniFi instances.
//!
//! The client connects to a remote QUIC server, performs the protocol
//! handshake, and sends FlowFile frames. It supports connection pooling
//! and automatic reconnection.

use std::net::SocketAddr;
use std::sync::Arc;

use parking_lot::Mutex;
use tracing::{debug, info, warn};

use crate::error::{TransportError, TransportResult};
use crate::protocol::{self, Capabilities, WireFlowFile};
use crate::tls;
use crate::transfer::SMALL_FILE_THRESHOLD;

/// Configuration for the QUIC client.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Remote server address.
    pub remote_addr: SocketAddr,
    /// Server name for TLS SNI (usually "localhost" for dev).
    pub server_name: String,
    /// TLS client configuration.
    pub tls_config: Arc<rustls::ClientConfig>,
    /// Maximum number of pooled connections.
    pub max_connections: usize,
    /// Batch size for inline stream transfers.
    pub batch_size: usize,
}

/// A QUIC client that sends FlowFiles to a remote server.
pub struct QuicClient {
    endpoint: quinn::Endpoint,
    config: ClientConfig,
    /// Pool of active connections.
    connections: Mutex<Vec<quinn::Connection>>,
}

impl QuicClient {
    /// Create a new QUIC client.
    pub fn new(config: ClientConfig) -> TransportResult<Self> {
        let mut endpoint =
            quinn::Endpoint::client("0.0.0.0:0".parse().unwrap()).map_err(TransportError::Io)?;

        let quinn_client_config = quinn::ClientConfig::new(Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from((*config.tls_config).clone())
                .map_err(|e| TransportError::Tls(format!("quinn TLS config error: {e}")))?,
        ));

        endpoint.set_default_client_config(quinn_client_config);

        Ok(Self {
            endpoint,
            config,
            connections: Mutex::new(Vec::new()),
        })
    }

    /// Get or create a connection to the remote server.
    async fn get_connection(&self) -> TransportResult<quinn::Connection> {
        // Try to reuse an existing connection
        {
            let mut pool = self.connections.lock();
            while let Some(conn) = pool.pop() {
                // Check if connection is still alive
                if conn.close_reason().is_none() {
                    return Ok(conn);
                }
            }
        }

        // Create a new connection
        let conn = self
            .endpoint
            .connect(self.config.remote_addr, &self.config.server_name)
            .map_err(TransportError::Connect)?
            .await
            .map_err(TransportError::Connection)?;

        debug!(
            remote = %self.config.remote_addr,
            "established QUIC connection"
        );

        Ok(conn)
    }

    /// Return a connection to the pool.
    fn return_connection(&self, conn: quinn::Connection) {
        let mut pool = self.connections.lock();
        if pool.len() < self.config.max_connections && conn.close_reason().is_none() {
            pool.push(conn);
        }
    }

    /// Send a single FlowFile to the remote server.
    pub async fn send(&self, ff: WireFlowFile) -> TransportResult<()> {
        let conn = self.get_connection().await?;
        let result = self.send_on_connection(&conn, ff).await;

        if result.is_ok() {
            self.return_connection(conn);
        }

        result
    }

    /// Send a single FlowFile on an existing connection.
    async fn send_on_connection(
        &self,
        conn: &quinn::Connection,
        ff: WireFlowFile,
    ) -> TransportResult<()> {
        let (mut send, mut recv) = conn.open_bi().await.map_err(TransportError::Connection)?;

        // Send handshake
        let handshake = protocol::encode_handshake(Capabilities::all());
        send.write_all(&handshake)
            .await
            .map_err(|e| TransportError::Protocol(format!("handshake write error: {e}")))?;

        // Encode and send the FlowFile frame (Phase 1: all strategies use chunked write)
        let frame = protocol::encode_flowfile(&ff);
        send.write_all(&frame)
            .await
            .map_err(|e| TransportError::Protocol(format!("flowfile write error: {e}")))?;

        // Send end-of-stream
        let eos = protocol::encode_end_of_stream();
        send.write_all(&eos)
            .await
            .map_err(|e| TransportError::Protocol(format!("eos write error: {e}")))?;

        // Finish writing
        send.finish()
            .map_err(|e| TransportError::Protocol(format!("stream finish error: {e}")))?;

        // Read response (handshake + ack)
        let response = recv.read_to_end(64 * 1024).await?;

        if response.len() >= 11 {
            // Parse handshake response
            let _handshake = protocol::decode_handshake(&response[..11])?;

            // Parse ack if present
            if response.len() > 11 {
                let frame = protocol::decode_frame(&response[11..])?;
                match frame {
                    protocol::DecodedFrame::Ack(ack) => {
                        if ack.status != protocol::AckStatus::Success {
                            return Err(TransportError::Protocol(format!(
                                "transfer rejected: {:?} - {}",
                                ack.status,
                                ack.message.unwrap_or_default()
                            )));
                        }
                    }
                    _ => {
                        warn!("unexpected response frame after handshake");
                    }
                }
            }
        }

        debug!("FlowFile sent successfully");
        Ok(())
    }

    /// Send a batch of small FlowFiles using inline stream strategy.
    pub async fn send_batch(&self, files: Vec<WireFlowFile>) -> TransportResult<()> {
        if files.is_empty() {
            return Ok(());
        }

        // Verify all files are small enough for batching
        let all_small = files
            .iter()
            .all(|ff| ff.content.len() as u64 <= SMALL_FILE_THRESHOLD);

        if !all_small {
            // Fall back to individual sends for large files
            for ff in files {
                self.send(ff).await?;
            }
            return Ok(());
        }

        let conn = self.get_connection().await?;
        let (mut send, mut recv) = conn.open_bi().await.map_err(TransportError::Connection)?;

        // Send handshake
        let handshake = protocol::encode_handshake(Capabilities::all());
        send.write_all(&handshake)
            .await
            .map_err(|e| TransportError::Protocol(format!("handshake write error: {e}")))?;

        // Send batch frame
        let batch = protocol::encode_flowfile_batch(&files);
        send.write_all(&batch)
            .await
            .map_err(|e| TransportError::Protocol(format!("batch write error: {e}")))?;

        // Send end-of-stream
        let eos = protocol::encode_end_of_stream();
        send.write_all(&eos)
            .await
            .map_err(|e| TransportError::Protocol(format!("eos write error: {e}")))?;

        send.finish()
            .map_err(|e| TransportError::Protocol(format!("stream finish error: {e}")))?;

        // Read response
        let response = recv.read_to_end(64 * 1024).await?;
        if response.len() >= 11 {
            let _handshake = protocol::decode_handshake(&response[..11])?;
        }

        self.return_connection(conn);

        info!(count = files.len(), "batch sent successfully");
        Ok(())
    }

    /// Close the client and all pooled connections.
    pub fn close(&self) {
        let mut pool = self.connections.lock();
        for conn in pool.drain(..) {
            conn.close(0u32.into(), b"client closing");
        }
        self.endpoint.close(0u32.into(), b"client closing");
    }
}

impl Drop for QuicClient {
    fn drop(&mut self) {
        self.close();
    }
}

/// Create a QuicClient configured for development (skip cert verification).
pub fn dev_client(remote_addr: SocketAddr) -> TransportResult<QuicClient> {
    let tls_config = tls::build_client_config(None, true)?;
    QuicClient::new(ClientConfig {
        remote_addr,
        server_name: "localhost".into(),
        tls_config: Arc::new(tls_config),
        max_connections: 4,
        batch_size: 100,
    })
}

/// Create a QuicClient that trusts a specific self-signed server certificate.
pub fn client_with_trusted_cert(
    remote_addr: SocketAddr,
    server_cert: &rustls::pki_types::CertificateDer<'static>,
    server_name: &str,
) -> TransportResult<QuicClient> {
    let tls_config = tls::build_client_config_with_trusted_cert(server_cert, None)?;
    QuicClient::new(ClientConfig {
        remote_addr,
        server_name: server_name.into(),
        tls_config: Arc::new(tls_config),
        max_connections: 4,
        batch_size: 100,
    })
}
