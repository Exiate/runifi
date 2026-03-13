//! QUIC server for receiving FlowFiles from remote RuniFi instances.
//!
//! The server listens on a configured address, accepts QUIC connections,
//! performs the protocol handshake, and receives FlowFile frames. Received
//! FlowFiles are placed into a bounded channel for the PullFlowFile processor
//! to consume.

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::error::{TransportError, TransportResult};
use crate::protocol::{self, AckStatus, Capabilities, DecodedFrame, WireFlowFile};
use crate::tls;

/// Configuration for the QUIC server.
#[derive(Debug)]
pub struct ServerConfig {
    /// Address to bind to.
    pub bind_addr: SocketAddr,
    /// TLS certificate and key.
    pub cert_key: tls::CertKeyPair,
    /// Maximum number of FlowFiles to buffer before back-pressuring senders.
    pub buffer_capacity: usize,
    /// Maximum concurrent connections.
    pub max_connections: u32,
}

/// A running QUIC server that receives FlowFiles.
pub struct QuicServer {
    /// Channel receiver for consumed FlowFiles.
    receiver: mpsc::Receiver<WireFlowFile>,
    /// Handle to shut down the server.
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    /// The local address the server is bound to.
    local_addr: SocketAddr,
}

impl QuicServer {
    /// Start the QUIC server, returning the server handle and local address.
    pub async fn start(config: ServerConfig) -> TransportResult<Self> {
        let server_tls = tls::build_server_config(&config.cert_key, None)?;
        let quinn_server_config = quinn::ServerConfig::with_crypto(Arc::new(
            quinn::crypto::rustls::QuicServerConfig::try_from(server_tls)
                .map_err(|e| TransportError::Tls(format!("quinn TLS config error: {e}")))?,
        ));

        let endpoint = quinn::Endpoint::server(quinn_server_config, config.bind_addr)
            .map_err(TransportError::Io)?;

        let local_addr = endpoint.local_addr().map_err(TransportError::Io)?;
        info!(addr = %local_addr, "QUIC server listening");

        let (tx, rx) = mpsc::channel(config.buffer_capacity);
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        // Spawn the accept loop
        tokio::spawn(accept_loop(endpoint, tx, shutdown_rx));

        Ok(Self {
            receiver: rx,
            shutdown_tx,
            local_addr,
        })
    }

    /// Get the local address the server is bound to.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Receive the next FlowFile. Returns `None` if the server has shut down.
    pub async fn recv(&mut self) -> Option<WireFlowFile> {
        self.receiver.recv().await
    }

    /// Try to receive a FlowFile without blocking.
    pub fn try_recv(&mut self) -> Option<WireFlowFile> {
        self.receiver.try_recv().ok()
    }

    /// Drain up to `max` FlowFiles from the receive buffer.
    pub fn drain(&mut self, max: usize) -> Vec<WireFlowFile> {
        let mut result = Vec::with_capacity(max);
        for _ in 0..max {
            match self.receiver.try_recv() {
                Ok(ff) => result.push(ff),
                Err(_) => break,
            }
        }
        result
    }

    /// Shut down the server.
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }
}

/// Accept loop: accepts incoming QUIC connections and spawns handlers.
async fn accept_loop(
    endpoint: quinn::Endpoint,
    tx: mpsc::Sender<WireFlowFile>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            incoming = endpoint.accept() => {
                match incoming {
                    Some(incoming) => {
                        let tx = tx.clone();
                        tokio::spawn(async move {
                            match incoming.await {
                                Ok(conn) => {
                                    if let Err(e) = handle_connection(conn, tx).await {
                                        warn!(error = %e, "connection handler error");
                                    }
                                }
                                Err(e) => {
                                    warn!(error = %e, "failed to accept connection");
                                }
                            }
                        });
                    }
                    None => {
                        info!("QUIC endpoint closed");
                        break;
                    }
                }
            }
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    info!("QUIC server shutting down");
                    endpoint.close(0u32.into(), b"shutdown");
                    break;
                }
            }
        }
    }
}

/// Handle a single QUIC connection: accept bidirectional streams.
async fn handle_connection(
    conn: quinn::Connection,
    tx: mpsc::Sender<WireFlowFile>,
) -> TransportResult<()> {
    let remote = conn.remote_address();
    debug!(remote = %remote, "accepted connection");

    loop {
        match conn.accept_bi().await {
            Ok((send, recv)) => {
                let tx = tx.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_stream(send, recv, tx).await {
                        warn!(error = %e, "stream handler error");
                    }
                });
            }
            Err(quinn::ConnectionError::ApplicationClosed(_)) => {
                debug!(remote = %remote, "connection closed by peer");
                break;
            }
            Err(e) => {
                warn!(remote = %remote, error = %e, "connection error");
                return Err(e.into());
            }
        }
    }

    Ok(())
}

/// Handle a single bidirectional QUIC stream.
///
/// Reads the handshake, then reads FlowFile frames, sending acks.
async fn handle_stream(
    mut send: quinn::SendStream,
    mut recv: quinn::RecvStream,
    tx: mpsc::Sender<WireFlowFile>,
) -> TransportResult<()> {
    // Read all data from the stream (bounded to 256MB for safety).
    let data = recv.read_to_end(256 * 1024 * 1024).await?;

    if data.is_empty() {
        return Ok(());
    }

    let mut cursor = data.as_slice();

    // First frame should be a handshake
    if cursor.is_empty() {
        return Ok(());
    }

    let frame_type = protocol::FrameType::from_u8(cursor[0])?;

    if frame_type == protocol::FrameType::Handshake {
        // Find handshake boundary (1 + 4 + 2 + 4 = 11 bytes)
        if cursor.len() < 11 {
            return Err(TransportError::Protocol("handshake too short".into()));
        }

        let handshake_data = &cursor[..11];
        let _handshake = protocol::decode_handshake(handshake_data)?;
        cursor = &cursor[11..];

        // Send handshake response
        let response = protocol::encode_handshake(Capabilities::all());
        send.write_all(&response).await.map_err(|e| {
            TransportError::Protocol(format!("failed to send handshake response: {e}"))
        })?;
    }

    // Process remaining frames
    while !cursor.is_empty() {
        let frame = protocol::decode_frame(cursor)?;
        match &frame {
            DecodedFrame::FlowFile(ff) => {
                let wire_ff = ff.clone();
                // Calculate consumed bytes
                let attr_size: usize = ff
                    .attributes
                    .iter()
                    .map(|(k, v)| 4 + k.len() + v.len())
                    .sum();
                let consumed = 1 + 2 + attr_size + 8 + ff.content.len() + 32;
                cursor = &cursor[consumed..];

                if tx.send(wire_ff).await.is_err() {
                    // Channel closed, send back-pressure
                    let bp = protocol::encode_ack(AckStatus::BackPressure, Some("buffer full"));
                    let _ = send.write_all(&bp).await;
                    break;
                }

                // Send ack
                let ack = protocol::encode_ack(AckStatus::Success, None);
                send.write_all(&ack)
                    .await
                    .map_err(|e| TransportError::Protocol(format!("failed to send ack: {e}")))?;
            }
            DecodedFrame::FlowFileBatch(files) => {
                // Calculate consumed bytes for the batch
                let mut batch_size = 1 + 4; // frame_type + count
                for ff in files {
                    let attr_size: usize = ff
                        .attributes
                        .iter()
                        .map(|(k, v)| 4 + k.len() + v.len())
                        .sum();
                    batch_size += 2 + attr_size + 8 + ff.content.len() + 32;
                }
                cursor = &cursor[batch_size..];

                for ff in files {
                    if tx.send(ff.clone()).await.is_err() {
                        let bp = protocol::encode_ack(AckStatus::BackPressure, Some("buffer full"));
                        let _ = send.write_all(&bp).await;
                        break;
                    }
                }

                let ack = protocol::encode_ack(AckStatus::Success, None);
                send.write_all(&ack).await.map_err(|e| {
                    TransportError::Protocol(format!("failed to send batch ack: {e}"))
                })?;
            }
            DecodedFrame::EndOfStream => {
                break;
            }
            other => {
                warn!(?other, "unexpected frame type in stream");
                let consumed = 1; // minimum advance
                cursor = &cursor[consumed..];
            }
        }
    }

    // Finish the send stream
    send.finish()
        .map_err(|e| TransportError::Protocol(format!("failed to finish send stream: {e}")))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tls::generate_self_signed;

    #[tokio::test]
    async fn server_starts_and_binds() {
        let cert_key = generate_self_signed(&["localhost"]).unwrap();
        let config = ServerConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            cert_key,
            buffer_capacity: 100,
            max_connections: 10,
        };

        let server = QuicServer::start(config).await.unwrap();
        let addr = server.local_addr();
        assert_ne!(addr.port(), 0);

        server.shutdown();
    }
}
