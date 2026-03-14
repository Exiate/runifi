pub mod context;
pub mod flowfile;
pub mod processor;
pub mod property;
pub mod record;
pub mod relationship;
pub mod reporting;
pub mod result;
pub mod service;
pub mod session;
pub mod sink;
pub mod source;
pub mod state;
pub mod validation;

// Re-export key types at crate root for convenience.
pub use context::ProcessContext;
pub use flowfile::{ContentClaim, FlowFile};
pub use processor::{Processor, ProcessorDescriptor};
pub use property::{PropertyDescriptor, PropertyValue};
pub use record::{
    Record, RecordFieldType, RecordReader, RecordSchema, RecordValue, RecordWriter, SchemaField,
};
pub use relationship::{REL_FAILURE, REL_ORIGINAL, REL_SUCCESS, Relationship};
pub use reporting::{ReportingContext, ReportingTask, ReportingTaskDescriptor};
pub use result::{PluginError, ProcessResult};
pub use service::{ControllerService, ControllerServiceDescriptor, ServiceLookup};
pub use session::ProcessSession;
pub use sink::{Sink, SinkDescriptor};
pub use source::{Source, SourceDescriptor};
pub use state::{StateManager, StateMap, StateScope, StatefulSpec};
pub use validation::ValidationResult;
