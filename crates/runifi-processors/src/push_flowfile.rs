//! PushFlowFile processor — sends FlowFiles to a remote RuniFi instance via QUIC.
//!
//! This processor connects to a remote QUIC server and pushes FlowFiles
//! from its input queue. It supports batching of small files (<=64KB)
//! for better throughput.

use std::net::SocketAddr;

use runifi_plugin_api::context::ProcessContext;
use runifi_plugin_api::processor::{Processor, ProcessorDescriptor};
use runifi_plugin_api::property::PropertyDescriptor;
use runifi_plugin_api::relationship::Relationship;
use runifi_plugin_api::result::{PluginError, ProcessResult};
use runifi_plugin_api::session::ProcessSession;
use runifi_plugin_api::{REL_FAILURE, REL_SUCCESS};
use runifi_transport::QuicClient;
use runifi_transport::protocol::WireFlowFile;
use runifi_transport::transfer::SMALL_FILE_THRESHOLD;

const PROP_REMOTE_HOST: PropertyDescriptor = PropertyDescriptor::new(
    "Remote Host",
    "Hostname or IP address of the remote RuniFi instance",
)
.required();

const PROP_REMOTE_PORT: PropertyDescriptor = PropertyDescriptor::new(
    "Remote Port",
    "Port number of the remote RuniFi QUIC server",
)
.required()
.default_value("8443");

const PROP_BATCH_SIZE: PropertyDescriptor = PropertyDescriptor::new(
    "Batch Size",
    "Maximum number of small FlowFiles to batch in a single transfer",
)
.default_value("100");

/// Sends FlowFiles to a remote RuniFi instance via QUIC transport.
pub struct PushFlowFile {
    client: Option<QuicClient>,
    runtime_handle: Option<tokio::runtime::Handle>,
}

impl PushFlowFile {
    pub fn new() -> Self {
        Self {
            client: None,
            runtime_handle: None,
        }
    }
}

impl Default for PushFlowFile {
    fn default() -> Self {
        Self::new()
    }
}

impl Processor for PushFlowFile {
    fn on_scheduled(&mut self, context: &dyn ProcessContext) -> ProcessResult {
        let host = context
            .get_property("Remote Host")
            .as_str()
            .map(|s| s.to_string())
            .ok_or(PluginError::PropertyRequired("Remote Host"))?;
        let port: u16 = context
            .get_property("Remote Port")
            .unwrap_or("8443")
            .parse()
            .map_err(|e| PluginError::ProcessingFailed(format!("invalid port: {e}")))?;

        let addr: SocketAddr = format!("{host}:{port}")
            .parse()
            .map_err(|e| PluginError::ProcessingFailed(format!("invalid address: {e}")))?;

        // Capture the tokio runtime handle for async bridging in on_trigger.
        let handle = tokio::runtime::Handle::try_current()
            .map_err(|e| PluginError::ProcessingFailed(format!("no tokio runtime: {e}")))?;
        self.runtime_handle = Some(handle);

        // Create a QUIC client with skip-verify for development.
        // Production deployments should use certificate-based verification.
        let client = runifi_transport::client::dev_client(addr)
            .map_err(|e| PluginError::ProcessingFailed(format!("client creation failed: {e}")))?;

        self.client = Some(client);

        tracing::info!(
            remote = %addr,
            "PushFlowFile scheduled — connected to remote endpoint"
        );

        Ok(())
    }

    fn on_trigger(
        &mut self,
        context: &dyn ProcessContext,
        session: &mut dyn ProcessSession,
    ) -> ProcessResult {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| PluginError::ProcessingFailed("client not initialized".into()))?;
        let handle = self
            .runtime_handle
            .as_ref()
            .ok_or_else(|| PluginError::ProcessingFailed("runtime handle not available".into()))?;

        let batch_size: usize = context
            .get_property("Batch Size")
            .unwrap_or("100")
            .parse()
            .unwrap_or(100);

        // Collect FlowFiles for batching or individual send
        let mut small_batch: Vec<(runifi_plugin_api::FlowFile, WireFlowFile)> = Vec::new();

        while let Some(flowfile) = session.get() {
            let content = match session.read_content(&flowfile) {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!(error = %e, "failed to read FlowFile content");
                    session.transfer(flowfile, &REL_FAILURE);
                    continue;
                }
            };

            let wire_ff = WireFlowFile::from_flowfile(&flowfile, content.clone());

            if content.len() as u64 <= SMALL_FILE_THRESHOLD && small_batch.len() < batch_size {
                small_batch.push((flowfile, wire_ff));

                if small_batch.len() >= batch_size {
                    // Flush the batch
                    let wire_files: Vec<WireFlowFile> =
                        small_batch.iter().map(|(_, w)| w.clone()).collect();
                    let result = handle.block_on(client.send_batch(wire_files));

                    match result {
                        Ok(()) => {
                            for (ff, _) in small_batch.drain(..) {
                                session.transfer(ff, &REL_SUCCESS);
                            }
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "batch send failed");
                            for (ff, _) in small_batch.drain(..) {
                                session.transfer(ff, &REL_FAILURE);
                            }
                        }
                    }
                }
            } else {
                // Large file: send individually
                let result = handle.block_on(client.send(wire_ff));
                match result {
                    Ok(()) => session.transfer(flowfile, &REL_SUCCESS),
                    Err(e) => {
                        tracing::warn!(error = %e, "send failed");
                        session.transfer(flowfile, &REL_FAILURE);
                    }
                }
            }
        }

        // Flush remaining small batch
        if !small_batch.is_empty() {
            let wire_files: Vec<WireFlowFile> =
                small_batch.iter().map(|(_, w)| w.clone()).collect();
            let result = handle.block_on(client.send_batch(wire_files));

            match result {
                Ok(()) => {
                    for (ff, _) in small_batch.drain(..) {
                        session.transfer(ff, &REL_SUCCESS);
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "batch send failed");
                    for (ff, _) in small_batch.drain(..) {
                        session.transfer(ff, &REL_FAILURE);
                    }
                }
            }
        }

        session.commit();
        Ok(())
    }

    fn on_stopped(&mut self, _context: &dyn ProcessContext) {
        if let Some(client) = self.client.take() {
            client.close();
            tracing::info!("PushFlowFile stopped — client closed");
        }
    }

    fn relationships(&self) -> Vec<Relationship> {
        vec![REL_SUCCESS, REL_FAILURE]
    }

    fn property_descriptors(&self) -> Vec<PropertyDescriptor> {
        vec![PROP_REMOTE_HOST, PROP_REMOTE_PORT, PROP_BATCH_SIZE]
    }
}

inventory::submit! {
    ProcessorDescriptor {
        type_name: "PushFlowFile",
        description: "Sends FlowFiles to a remote RuniFi instance via QUIC transport",
        factory: || Box::new(PushFlowFile::new()),
        tags: &["Networking", "Transport", "Site-to-Site"],
    }
}
