use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{DateTime, Utc};
use serde::Serialize;

/// Monotonic event ID generator.
static NEXT_EVENT_ID: AtomicU64 = AtomicU64::new(1);

fn next_event_id() -> u64 {
    NEXT_EVENT_ID.fetch_add(1, Ordering::Relaxed)
}

/// A single audit event capturing a security-relevant action.
///
/// Designed for compliance with SOC 2, HIPAA, and PCI-DSS audit trail
/// requirements. Each event records who did what, to which resource,
/// when, and whether the operation succeeded.
#[derive(Debug, Clone, Serialize)]
pub struct AuditEvent {
    pub timestamp: DateTime<Utc>,
    pub event_id: u64,
    pub action: AuditAction,
    pub target: AuditTarget,
    pub outcome: AuditOutcome,
    /// Identity of the actor. Currently "system" for all automated actions;
    /// will carry authenticated user identity when RBAC is added.
    pub actor: String,
    /// Optional human-readable details about the action.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,
}

impl AuditEvent {
    /// Build a new audit event with the current timestamp and a monotonic ID.
    pub fn new(
        action: AuditAction,
        target: AuditTarget,
        outcome: AuditOutcome,
        actor: impl Into<String>,
        details: Option<String>,
    ) -> Self {
        Self {
            timestamp: Utc::now(),
            event_id: next_event_id(),
            action,
            target,
            outcome,
            actor: actor.into(),
            details,
        }
    }

    /// Convenience: build a successful system event with no extra details.
    pub fn success(action: AuditAction, target: AuditTarget) -> Self {
        Self::new(action, target, AuditOutcome::Success, "system", None)
    }

    /// Convenience: build a successful system event with details.
    pub fn success_with_details(
        action: AuditAction,
        target: AuditTarget,
        details: impl Into<String>,
    ) -> Self {
        Self::new(
            action,
            target,
            AuditOutcome::Success,
            "system",
            Some(details.into()),
        )
    }

    /// Convenience: build a failure event.
    pub fn failure(action: AuditAction, target: AuditTarget, reason: impl Into<String>) -> Self {
        Self::new(
            action,
            target,
            AuditOutcome::Failure {
                reason: reason.into(),
            },
            "system",
            None,
        )
    }
}

/// The type of auditable action.
#[derive(Debug, Clone, Serialize)]
pub enum AuditAction {
    // Topology mutations
    ProcessorCreated,
    ProcessorRemoved,
    ConnectionCreated,
    ConnectionRemoved,
    // Lifecycle
    ProcessorStarted,
    ProcessorStopped,
    ProcessorPaused,
    ProcessorResumed,
    // Configuration
    ProcessorConfigured,
    // Data access
    QueueInspected,
    ContentDownloaded,
    QueueEmptied,
    // Controller services
    ServiceCreated,
    ServiceRemoved,
    ServiceEnabled,
    ServiceDisabled,
    ServiceConfigured,
    // Process groups
    ProcessGroupCreated,
    ProcessGroupUpdated,
    ProcessGroupRemoved,
    // System
    EngineStarted,
    EngineShutdown,
}

/// The resource targeted by an audit action.
#[derive(Debug, Clone, Serialize)]
pub struct AuditTarget {
    /// Resource type: "processor", "connection", "queue", "system".
    pub resource_type: String,
    /// Resource identifier: processor name, connection ID, etc.
    pub resource_id: String,
}

impl AuditTarget {
    pub fn processor(name: impl Into<String>) -> Self {
        Self {
            resource_type: "processor".to_string(),
            resource_id: name.into(),
        }
    }

    pub fn connection(id: impl Into<String>) -> Self {
        Self {
            resource_type: "connection".to_string(),
            resource_id: id.into(),
        }
    }

    pub fn service(name: impl Into<String>) -> Self {
        Self {
            resource_type: "service".to_string(),
            resource_id: name.into(),
        }
    }

    pub fn process_group(id: impl Into<String>) -> Self {
        Self {
            resource_type: "process_group".to_string(),
            resource_id: id.into(),
        }
    }

    pub fn queue(connection_id: impl Into<String>) -> Self {
        Self {
            resource_type: "queue".to_string(),
            resource_id: connection_id.into(),
        }
    }

    pub fn system() -> Self {
        Self {
            resource_type: "system".to_string(),
            resource_id: "engine".to_string(),
        }
    }
}

/// The outcome of an auditable action.
#[derive(Debug, Clone, Serialize)]
pub enum AuditOutcome {
    Success,
    Failure {
        reason: String,
    },
    /// Reserved for future RBAC integration.
    Denied {
        reason: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_audit_event_serializes_to_json() {
        let event = AuditEvent::success(
            AuditAction::ProcessorCreated,
            AuditTarget::processor("test-proc"),
        );

        let json = serde_json::to_string(&event).expect("serialize");
        assert!(json.contains("\"action\":\"ProcessorCreated\""));
        assert!(json.contains("\"resource_type\":\"processor\""));
        assert!(json.contains("\"resource_id\":\"test-proc\""));
        assert!(json.contains("\"outcome\":\"Success\""));
        assert!(json.contains("\"actor\":\"system\""));
        // details is None, should be omitted
        assert!(!json.contains("\"details\""));
    }

    #[test]
    fn test_audit_event_with_details_serializes() {
        let event = AuditEvent::success_with_details(
            AuditAction::EngineStarted,
            AuditTarget::system(),
            "nodes=3, connections=2",
        );

        let json = serde_json::to_string(&event).expect("serialize");
        assert!(json.contains("\"details\":\"nodes=3, connections=2\""));
    }

    #[test]
    fn test_failure_event_serializes() {
        let event = AuditEvent::failure(
            AuditAction::ProcessorRemoved,
            AuditTarget::processor("bad-proc"),
            "processor not found",
        );

        let json = serde_json::to_string(&event).expect("serialize");
        assert!(json.contains("\"Failure\""));
        assert!(json.contains("processor not found"));
    }

    #[test]
    fn test_event_ids_are_monotonic() {
        let e1 = AuditEvent::success(AuditAction::EngineStarted, AuditTarget::system());
        let e2 = AuditEvent::success(AuditAction::EngineShutdown, AuditTarget::system());
        assert!(e2.event_id > e1.event_id);
    }

    #[test]
    fn test_target_constructors() {
        let t = AuditTarget::processor("foo");
        assert_eq!(t.resource_type, "processor");
        assert_eq!(t.resource_id, "foo");

        let t = AuditTarget::connection("conn-1");
        assert_eq!(t.resource_type, "connection");

        let t = AuditTarget::queue("conn-2");
        assert_eq!(t.resource_type, "queue");

        let t = AuditTarget::system();
        assert_eq!(t.resource_type, "system");
    }
}
