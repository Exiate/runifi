//! Role-based access control (RBAC) types and permission checking.
//!
//! Defines the role hierarchy (`Admin > Operator > Viewer`) and the set of
//! permissions that each role grants. Route handlers use [`require_permission`]
//! to enforce access control after the auth middleware has attached a [`Role`]
//! to the request extensions.

use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

/// Roles ordered by privilege level (highest to lowest).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Role {
    /// Full control: flow mutations, config changes, operations, and viewing.
    Admin,
    /// Operational control: start/stop/pause processors, view, access content.
    Operator,
    /// Read-only: view flow state, metrics, bulletins.
    Viewer,
}

impl Role {
    /// Parse a role from a string (case-insensitive).
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "admin" => Some(Role::Admin),
            "operator" => Some(Role::Operator),
            "viewer" => Some(Role::Viewer),
            _ => None,
        }
    }

    /// Returns `true` if this role has the given permission.
    pub fn has_permission(self, permission: Permission) -> bool {
        match permission {
            Permission::ViewFlow => true, // All roles can view.
            Permission::OperateProcessors => matches!(self, Role::Admin | Role::Operator),
            Permission::AccessContent => matches!(self, Role::Admin | Role::Operator),
            Permission::ModifyFlow => matches!(self, Role::Admin),
            Permission::ManageConfig => matches!(self, Role::Admin),
        }
    }

    /// Return a human-readable name for this role.
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Admin => "admin",
            Role::Operator => "operator",
            Role::Viewer => "viewer",
        }
    }
}

/// Permissions that can be required by route handlers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Permission {
    /// View flow state, metrics, processors, connections, bulletins.
    /// Required role: Viewer or higher.
    ViewFlow,
    /// Start, stop, pause, resume processors; manage queue items.
    /// Required role: Operator or higher.
    OperateProcessors,
    /// Download FlowFile content, access provenance data.
    /// Required role: Operator or higher.
    AccessContent,
    /// Create, delete, or structurally modify processors and connections.
    /// Required role: Admin only.
    ModifyFlow,
    /// Change processor configuration properties.
    /// Required role: Admin only.
    ManageConfig,
}

impl Permission {
    /// Return a human-readable label for this permission.
    pub fn as_str(self) -> &'static str {
        match self {
            Permission::ViewFlow => "ViewFlow",
            Permission::OperateProcessors => "OperateProcessors",
            Permission::AccessContent => "AccessContent",
            Permission::ModifyFlow => "ModifyFlow",
            Permission::ManageConfig => "ManageConfig",
        }
    }

    /// Return the minimum role required for this permission.
    pub fn minimum_role(self) -> &'static str {
        match self {
            Permission::ViewFlow => "viewer",
            Permission::OperateProcessors => "operator",
            Permission::AccessContent => "operator",
            Permission::ModifyFlow => "admin",
            Permission::ManageConfig => "admin",
        }
    }
}

/// Check that the request has the required permission.
///
/// When auth is disabled, all requests are treated as `Admin`. When auth is
/// enabled, the [`Role`] must have been inserted into request extensions by
/// the auth middleware.
///
/// Returns `Ok(())` on success, or a 403 Forbidden response on failure.
pub fn require_permission(req: &Request, permission: Permission) -> Result<(), Box<Response>> {
    // If no role is attached, auth is disabled — treat as Admin.
    let role = req
        .extensions()
        .get::<Role>()
        .copied()
        .unwrap_or(Role::Admin);

    if role.has_permission(permission) {
        Ok(())
    } else {
        let body = serde_json::json!({
            "error": "Forbidden",
            "required_permission": permission.as_str(),
            "minimum_role": permission.minimum_role(),
        });
        Err(Box::new(
            (StatusCode::FORBIDDEN, axum::Json(body)).into_response(),
        ))
    }
}

// ---------------------------------------------------------------------------
// Permission middleware helpers
// ---------------------------------------------------------------------------

/// Axum middleware that requires `ViewFlow` permission (Viewer+).
pub async fn require_view_flow(req: Request, next: Next) -> Response {
    if let Err(resp) = require_permission(&req, Permission::ViewFlow) {
        return *resp;
    }
    next.run(req).await
}

/// Axum middleware that requires `OperateProcessors` permission (Operator+).
pub async fn require_operate_processors(req: Request, next: Next) -> Response {
    if let Err(resp) = require_permission(&req, Permission::OperateProcessors) {
        return *resp;
    }
    next.run(req).await
}

/// Axum middleware that requires `AccessContent` permission (Operator+).
pub async fn require_access_content(req: Request, next: Next) -> Response {
    if let Err(resp) = require_permission(&req, Permission::AccessContent) {
        return *resp;
    }
    next.run(req).await
}

/// Axum middleware that requires `ModifyFlow` permission (Admin only).
pub async fn require_modify_flow(req: Request, next: Next) -> Response {
    if let Err(resp) = require_permission(&req, Permission::ModifyFlow) {
        return *resp;
    }
    next.run(req).await
}

/// Axum middleware that requires `ManageConfig` permission (Admin only).
pub async fn require_manage_config(req: Request, next: Next) -> Response {
    if let Err(resp) = require_permission(&req, Permission::ManageConfig) {
        return *resp;
    }
    next.run(req).await
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_role_parse() {
        assert_eq!(Role::parse("admin"), Some(Role::Admin));
        assert_eq!(Role::parse("Admin"), Some(Role::Admin));
        assert_eq!(Role::parse("ADMIN"), Some(Role::Admin));
        assert_eq!(Role::parse("operator"), Some(Role::Operator));
        assert_eq!(Role::parse("Operator"), Some(Role::Operator));
        assert_eq!(Role::parse("viewer"), Some(Role::Viewer));
        assert_eq!(Role::parse("Viewer"), Some(Role::Viewer));
        assert_eq!(Role::parse("unknown"), None);
        assert_eq!(Role::parse(""), None);
    }

    #[test]
    fn test_role_as_str() {
        assert_eq!(Role::Admin.as_str(), "admin");
        assert_eq!(Role::Operator.as_str(), "operator");
        assert_eq!(Role::Viewer.as_str(), "viewer");
    }

    #[test]
    fn test_admin_has_all_permissions() {
        assert!(Role::Admin.has_permission(Permission::ViewFlow));
        assert!(Role::Admin.has_permission(Permission::OperateProcessors));
        assert!(Role::Admin.has_permission(Permission::AccessContent));
        assert!(Role::Admin.has_permission(Permission::ModifyFlow));
        assert!(Role::Admin.has_permission(Permission::ManageConfig));
    }

    #[test]
    fn test_operator_permissions() {
        assert!(Role::Operator.has_permission(Permission::ViewFlow));
        assert!(Role::Operator.has_permission(Permission::OperateProcessors));
        assert!(Role::Operator.has_permission(Permission::AccessContent));
        assert!(!Role::Operator.has_permission(Permission::ModifyFlow));
        assert!(!Role::Operator.has_permission(Permission::ManageConfig));
    }

    #[test]
    fn test_viewer_permissions() {
        assert!(Role::Viewer.has_permission(Permission::ViewFlow));
        assert!(!Role::Viewer.has_permission(Permission::OperateProcessors));
        assert!(!Role::Viewer.has_permission(Permission::AccessContent));
        assert!(!Role::Viewer.has_permission(Permission::ModifyFlow));
        assert!(!Role::Viewer.has_permission(Permission::ManageConfig));
    }

    #[test]
    fn test_permission_as_str() {
        assert_eq!(Permission::ViewFlow.as_str(), "ViewFlow");
        assert_eq!(Permission::OperateProcessors.as_str(), "OperateProcessors");
        assert_eq!(Permission::AccessContent.as_str(), "AccessContent");
        assert_eq!(Permission::ModifyFlow.as_str(), "ModifyFlow");
        assert_eq!(Permission::ManageConfig.as_str(), "ManageConfig");
    }

    #[test]
    fn test_permission_minimum_role() {
        assert_eq!(Permission::ViewFlow.minimum_role(), "viewer");
        assert_eq!(Permission::OperateProcessors.minimum_role(), "operator");
        assert_eq!(Permission::AccessContent.minimum_role(), "operator");
        assert_eq!(Permission::ModifyFlow.minimum_role(), "admin");
        assert_eq!(Permission::ManageConfig.minimum_role(), "admin");
    }

    #[test]
    fn test_require_permission_no_role_in_extensions() {
        // When no role is in extensions (auth disabled), defaults to Admin.
        let req = Request::builder().body(axum::body::Body::empty()).unwrap();
        assert!(require_permission(&req, Permission::ViewFlow).is_ok());
        assert!(require_permission(&req, Permission::ModifyFlow).is_ok());
        assert!(require_permission(&req, Permission::ManageConfig).is_ok());
    }

    #[test]
    fn test_require_permission_viewer_role() {
        let mut req = Request::builder().body(axum::body::Body::empty()).unwrap();
        req.extensions_mut().insert(Role::Viewer);

        assert!(require_permission(&req, Permission::ViewFlow).is_ok());
        assert!(require_permission(&req, Permission::ModifyFlow).is_err());
        assert!(require_permission(&req, Permission::OperateProcessors).is_err());
        assert!(require_permission(&req, Permission::AccessContent).is_err());
        assert!(require_permission(&req, Permission::ManageConfig).is_err());
    }

    #[test]
    fn test_require_permission_operator_role() {
        let mut req = Request::builder().body(axum::body::Body::empty()).unwrap();
        req.extensions_mut().insert(Role::Operator);

        assert!(require_permission(&req, Permission::ViewFlow).is_ok());
        assert!(require_permission(&req, Permission::OperateProcessors).is_ok());
        assert!(require_permission(&req, Permission::AccessContent).is_ok());
        assert!(require_permission(&req, Permission::ModifyFlow).is_err());
        assert!(require_permission(&req, Permission::ManageConfig).is_err());
    }

    #[test]
    fn test_require_permission_admin_role() {
        let mut req = Request::builder().body(axum::body::Body::empty()).unwrap();
        req.extensions_mut().insert(Role::Admin);

        assert!(require_permission(&req, Permission::ViewFlow).is_ok());
        assert!(require_permission(&req, Permission::OperateProcessors).is_ok());
        assert!(require_permission(&req, Permission::AccessContent).is_ok());
        assert!(require_permission(&req, Permission::ModifyFlow).is_ok());
        assert!(require_permission(&req, Permission::ManageConfig).is_ok());
    }
}
