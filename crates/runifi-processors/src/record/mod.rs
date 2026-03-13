//! Record-oriented processing: readers, writers, schema registry, and processors.
//!
//! This module implements the record processing framework for RuniFi, enabling
//! structured data transformation without creating individual FlowFiles per record.

pub mod convert_record;
pub mod csv_reader;
pub mod csv_writer;
pub mod json_reader;
pub mod json_writer;
pub mod partition_record;
pub mod schema_registry;
pub mod update_record;
