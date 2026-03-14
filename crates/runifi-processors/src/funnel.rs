use runifi_plugin_api::REL_SUCCESS;
use runifi_plugin_api::context::ProcessContext;
use runifi_plugin_api::processor::{Processor, ProcessorDescriptor};
use runifi_plugin_api::property::PropertyDescriptor;
use runifi_plugin_api::relationship::Relationship;
use runifi_plugin_api::result::ProcessResult;
use runifi_plugin_api::session::ProcessSession;

/// A Funnel merges multiple incoming connections into a single output.
///
/// It acts as a pass-through: all FlowFiles received on any input connection
/// are forwarded to the "success" relationship. Funnels have no configurable
/// properties and are useful for combining flows from several sources before
/// routing to a single destination.
pub struct Funnel;

impl Default for Funnel {
    fn default() -> Self {
        Self
    }
}

impl Processor for Funnel {
    fn on_trigger(
        &mut self,
        _context: &dyn ProcessContext,
        session: &mut dyn ProcessSession,
    ) -> ProcessResult {
        // Forward all available FlowFiles to success.
        let batch = session.get_batch(256);
        if batch.is_empty() {
            // Nothing to do — yield back to scheduler.
            return Ok(());
        }

        for flowfile in batch {
            session.transfer(flowfile, &REL_SUCCESS);
        }

        session.commit();
        Ok(())
    }

    fn relationships(&self) -> Vec<Relationship> {
        vec![REL_SUCCESS]
    }

    fn property_descriptors(&self) -> Vec<PropertyDescriptor> {
        vec![]
    }
}

inventory::submit! {
    ProcessorDescriptor {
        type_name: "Funnel",
        description: "Merges multiple incoming connections into a single output",
        factory: || Box::new(Funnel),
        tags: &["Flow Control"],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use runifi_plugin_api::property::PropertyValue;

    struct TestContext;

    impl ProcessContext for TestContext {
        fn get_property(&self, _name: &str) -> PropertyValue {
            PropertyValue::Unset
        }
        fn name(&self) -> &str {
            "test-funnel"
        }
        fn id(&self) -> &str {
            "test-id"
        }
        fn yield_duration_ms(&self) -> u64 {
            1000
        }
    }

    struct CollectorSession {
        pending: Vec<runifi_plugin_api::FlowFile>,
        transferred: Vec<(runifi_plugin_api::FlowFile, &'static str)>,
        committed: bool,
    }

    impl CollectorSession {
        fn with_flowfiles(count: usize) -> Self {
            let pending = (1..=count as u64)
                .map(|id| runifi_plugin_api::FlowFile {
                    id,
                    attributes: Vec::new(),
                    content_claim: None,
                    size: 100,
                    created_at_nanos: 0,
                    lineage_start_id: id,
                    penalized_until_nanos: 0,
                })
                .collect();
            Self {
                pending,
                transferred: Vec::new(),
                committed: false,
            }
        }
    }

    impl ProcessSession for CollectorSession {
        fn get(&mut self) -> Option<runifi_plugin_api::FlowFile> {
            self.pending.pop()
        }
        fn get_batch(&mut self, max: usize) -> Vec<runifi_plugin_api::FlowFile> {
            let count = max.min(self.pending.len());
            self.pending.drain(..count).collect()
        }
        fn read_content(&self, _ff: &runifi_plugin_api::FlowFile) -> ProcessResult<Bytes> {
            Ok(Bytes::new())
        }
        fn write_content(
            &mut self,
            ff: runifi_plugin_api::FlowFile,
            _data: Bytes,
        ) -> ProcessResult<runifi_plugin_api::FlowFile> {
            Ok(ff)
        }
        fn create(&mut self) -> runifi_plugin_api::FlowFile {
            unimplemented!()
        }
        fn clone_flowfile(
            &mut self,
            _ff: &runifi_plugin_api::FlowFile,
        ) -> runifi_plugin_api::FlowFile {
            unimplemented!()
        }
        fn transfer(&mut self, ff: runifi_plugin_api::FlowFile, rel: &Relationship) {
            self.transferred.push((ff, rel.name));
        }
        fn remove(&mut self, _ff: runifi_plugin_api::FlowFile) {}
        fn penalize(&mut self, ff: runifi_plugin_api::FlowFile) -> runifi_plugin_api::FlowFile {
            ff
        }
        fn commit(&mut self) {
            self.committed = true;
        }
        fn rollback(&mut self) {}
    }

    #[test]
    fn funnel_forwards_all_flowfiles() {
        let mut funnel = Funnel;
        let ctx = TestContext;
        let mut session = CollectorSession::with_flowfiles(5);

        funnel.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 5);
        for (_, rel) in &session.transferred {
            assert_eq!(*rel, "success");
        }
        assert!(session.committed);
    }

    #[test]
    fn funnel_noop_on_empty_input() {
        let mut funnel = Funnel;
        let ctx = TestContext;
        let mut session = CollectorSession::with_flowfiles(0);

        funnel.on_trigger(&ctx, &mut session).unwrap();

        assert!(session.transferred.is_empty());
        assert!(!session.committed);
    }

    #[test]
    fn funnel_has_no_properties() {
        let funnel = Funnel;
        assert!(funnel.property_descriptors().is_empty());
    }

    #[test]
    fn funnel_has_success_relationship() {
        let funnel = Funnel;
        let rels = funnel.relationships();
        assert_eq!(rels.len(), 1);
        assert_eq!(rels[0].name, "success");
    }
}
