pub mod context;
pub mod flowfile;
pub mod processor;
pub mod property;
pub mod relationship;
pub mod result;
pub mod service;
pub mod session;
pub mod sink;
pub mod source;

// Re-export key types at crate root for convenience.
pub use context::ProcessContext;
pub use flowfile::{ContentClaim, FlowFile};
pub use processor::{Processor, ProcessorDescriptor};
pub use property::{PropertyDescriptor, PropertyValue};
pub use relationship::{REL_FAILURE, REL_ORIGINAL, REL_SUCCESS, Relationship};
pub use result::{PluginError, ProcessResult};
pub use service::{ControllerService, ControllerServiceDescriptor, ServiceLookup};
pub use session::ProcessSession;
pub use sink::{Sink, SinkDescriptor};
pub use source::{Source, SourceDescriptor};
