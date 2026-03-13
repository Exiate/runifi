//! PullFlowFile processor — receives FlowFiles from a remote RuniFi instance via QUIC.
//!
//! This processor starts a QUIC server and accepts incoming FlowFile
//! transfers from remote PushFlowFile processors. Received FlowFiles
//! are buffered and emitted on each trigger invocation.

use std::net::SocketAddr;

use runifi_plugin_api::REL_SUCCESS;
use runifi_plugin_api::context::ProcessContext;
use runifi_plugin_api::processor::{Processor, ProcessorDescriptor};
use runifi_plugin_api::property::PropertyDescriptor;
use runifi_plugin_api::relationship::Relationship;
use runifi_plugin_api::result::{PluginError, ProcessResult};
use runifi_plugin_api::session::ProcessSession;
use runifi_transport::{QuicServer, ServerConfig};

const PROP_LISTEN_HOST: PropertyDescriptor = PropertyDescriptor::new(
    "Listen Host",
    "Hostname or IP address to listen on for incoming connections",
)
.default_value("0.0.0.0");

const PROP_LISTEN_PORT: PropertyDescriptor = PropertyDescriptor::new(
    "Listen Port",
    "Port number to listen on for incoming QUIC connections",
)
.required()
.default_value("8443");

const PROP_BUFFER_SIZE: PropertyDescriptor = PropertyDescriptor::new(
    "Buffer Size",
    "Maximum number of FlowFiles to buffer before applying back-pressure",
)
.default_value("10000");

const PROP_MAX_BATCH: PropertyDescriptor = PropertyDescriptor::new(
    "Max Batch Size",
    "Maximum number of FlowFiles to emit per trigger invocation",
)
.default_value("1000");

/// Receives FlowFiles from remote RuniFi instances via QUIC transport.
pub struct PullFlowFile {
    server: Option<QuicServer>,
    runtime_handle: Option<tokio::runtime::Handle>,
}

impl PullFlowFile {
    pub fn new() -> Self {
        Self {
            server: None,
            runtime_handle: None,
        }
    }
}

impl Default for PullFlowFile {
    fn default() -> Self {
        Self::new()
    }
}

impl Processor for PullFlowFile {
    fn on_scheduled(&mut self, context: &dyn ProcessContext) -> ProcessResult {
        let host = context
            .get_property("Listen Host")
            .unwrap_or("0.0.0.0")
            .to_string();
        let port: u16 = context
            .get_property("Listen Port")
            .unwrap_or("8443")
            .parse()
            .map_err(|e| PluginError::ProcessingFailed(format!("invalid port: {e}")))?;
        let buffer_size: usize = context
            .get_property("Buffer Size")
            .unwrap_or("10000")
            .parse()
            .unwrap_or(10000);

        let addr: SocketAddr = format!("{host}:{port}")
            .parse()
            .map_err(|e| PluginError::ProcessingFailed(format!("invalid address: {e}")))?;

        // Capture the tokio runtime handle
        let handle = tokio::runtime::Handle::try_current()
            .map_err(|e| PluginError::ProcessingFailed(format!("no tokio runtime: {e}")))?;
        self.runtime_handle = Some(handle.clone());

        // Generate self-signed cert for the server
        let cert_key = runifi_transport::tls::generate_self_signed(&["localhost", &host])
            .map_err(|e| PluginError::ProcessingFailed(format!("cert generation failed: {e}")))?;

        let config = ServerConfig {
            bind_addr: addr,
            cert_key,
            buffer_capacity: buffer_size,
            max_connections: 100,
        };

        // Start the QUIC server
        let server = handle
            .block_on(QuicServer::start(config))
            .map_err(|e| PluginError::ProcessingFailed(format!("server start failed: {e}")))?;

        let actual_addr = server.local_addr();
        tracing::info!(
            addr = %actual_addr,
            "PullFlowFile scheduled — QUIC server listening"
        );

        self.server = Some(server);
        Ok(())
    }

    fn on_trigger(
        &mut self,
        context: &dyn ProcessContext,
        session: &mut dyn ProcessSession,
    ) -> ProcessResult {
        let server = self
            .server
            .as_mut()
            .ok_or_else(|| PluginError::ProcessingFailed("server not initialized".into()))?;

        let max_batch: usize = context
            .get_property("Max Batch Size")
            .unwrap_or("1000")
            .parse()
            .unwrap_or(1000);

        // Drain received FlowFiles from the server buffer
        let received = server.drain(max_batch);

        if received.is_empty() {
            session.commit();
            return Ok(());
        }

        tracing::debug!(count = received.len(), "received FlowFiles from remote");

        for wire_ff in received {
            // Create a new FlowFile in the session
            let mut flowfile = session.create();

            // Restore attributes
            for (key, val) in wire_ff.attributes {
                flowfile.set_attribute(key, val);
            }

            // Write content
            if !wire_ff.content.is_empty() {
                flowfile = session
                    .write_content(flowfile, wire_ff.content)
                    .map_err(|e| {
                        PluginError::ProcessingFailed(format!("write content failed: {e}"))
                    })?;
            }

            session.transfer(flowfile, &REL_SUCCESS);
        }

        session.commit();
        Ok(())
    }

    fn on_stopped(&mut self, _context: &dyn ProcessContext) {
        if let Some(server) = self.server.take() {
            server.shutdown();
            tracing::info!("PullFlowFile stopped — QUIC server shut down");
        }
    }

    fn relationships(&self) -> Vec<Relationship> {
        vec![REL_SUCCESS]
    }

    fn property_descriptors(&self) -> Vec<PropertyDescriptor> {
        vec![
            PROP_LISTEN_HOST,
            PROP_LISTEN_PORT,
            PROP_BUFFER_SIZE,
            PROP_MAX_BATCH,
        ]
    }
}

inventory::submit! {
    ProcessorDescriptor {
        type_name: "PullFlowFile",
        description: "Receives FlowFiles from a remote RuniFi instance via QUIC transport",
        factory: || Box::new(PullFlowFile::new()),
        tags: &["Networking", "Transport", "Site-to-Site"],
    }
}
