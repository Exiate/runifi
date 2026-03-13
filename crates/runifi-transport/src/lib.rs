//! RuniFi QUIC transport layer for site-to-site FlowFile transfers.
//!
//! This crate provides QUIC-based networking for transferring FlowFiles
//! between RuniFi instances. It includes:
//!
//! - **TLS**: Self-signed certificate generation and mTLS support
//! - **Protocol**: Wire format for FlowFile serialization with BLAKE3 integrity
//! - **Server**: QUIC server that receives FlowFiles from remote instances
//! - **Client**: QUIC client with connection pooling for sending FlowFiles
//! - **Transfer strategies**: Size-based selection of inline, chunked, or zero-copy

pub mod client;
pub mod error;
pub mod protocol;
pub mod server;
pub mod tls;
pub mod transfer;

// Re-export key types at crate root.
pub use client::{ClientConfig, QuicClient};
pub use error::{TransportError, TransportResult};
pub use protocol::WireFlowFile;
pub use server::{QuicServer, ServerConfig};
pub use tls::CertKeyPair;
