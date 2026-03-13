//! API key authentication and CSRF protection middleware.
//!
//! When API keys are configured in `[api.security]`, all API endpoints require
//! a valid bearer token in the `Authorization` header or an `api_key` query
//! parameter. Dashboard static files (`/`, `/assets/*`) are always exempt from
//! authentication.
//!
//! CSRF protection uses the double-submit cookie pattern: mutating requests
//! (POST, PUT, DELETE) must include an `X-CSRF-Token` header whose value matches
//! the `runifi_csrf` cookie. Requests authenticated via bearer token header are
//! exempt from CSRF checks (tokens are not sent automatically by browsers).

use axum::extract::{Request, State};
use axum::http::{Method, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use rand::Rng;
use subtle::ConstantTimeEq;

use crate::rbac::Role;
use crate::state::ApiState;

/// Name of the CSRF cookie set on dashboard pages.
const CSRF_COOKIE_NAME: &str = "runifi_csrf";

/// Header the client must echo to prove it can read the cookie (same-origin).
const CSRF_HEADER_NAME: &str = "x-csrf-token";

/// Length of the random hex CSRF token (used in tests).
#[cfg(test)]
const CSRF_TOKEN_HEX_LEN: usize = 64; // 32 bytes -> 64 hex chars

// ---------------------------------------------------------------------------
// Public helpers
// ---------------------------------------------------------------------------

/// Returns `true` if the given request path is exempt from authentication.
///
/// Exempt paths are:
///   - `/` (dashboard index)
///   - `/assets/*` (dashboard static assets)
pub fn is_exempt_path(path: &str) -> bool {
    path == "/" || path.starts_with("/assets/")
}

/// Generate a cryptographically random CSRF token (hex-encoded).
pub fn generate_csrf_token() -> String {
    let mut bytes = [0u8; 32];
    rand::rng().fill(&mut bytes);
    hex_encode(&bytes)
}

// ---------------------------------------------------------------------------
// Authentication middleware
// ---------------------------------------------------------------------------

/// Axum middleware that enforces API key authentication and attaches the
/// caller's [`Role`] to request extensions for downstream permission checks.
///
/// If no API keys are configured, all requests are allowed through (no role
/// is attached — handlers default to `Admin`).
/// Dashboard static paths (`/`, `/assets/*`) are always exempt.
///
/// Valid authentication methods:
///   1. `Authorization: Bearer <key>` header
///   2. `?api_key=<key>` query parameter (useful for SSE EventSource)
pub async fn auth_middleware(
    State(state): State<ApiState>,
    mut req: Request,
    next: Next,
) -> Response {
    // If auth is not enabled, pass through (no role set — defaults to Admin).
    if !state.security.auth_enabled() {
        return next.run(req).await;
    }

    let path = req.uri().path().to_string();

    // Dashboard static assets are always exempt.
    if is_exempt_path(&path) {
        return next.run(req).await;
    }

    // Try to extract API key from Authorization header or query param.
    let provided_key = extract_api_key(&req);

    match provided_key {
        Some(ref key) if validate_api_key(key, &state.security) => {
            // Look up the role for this key and attach it to extensions.
            if let Some(role) = resolve_role(key, &state.security) {
                req.extensions_mut().insert(role);
            }
            next.run(req).await
        }
        _ => {
            let body = serde_json::json!({ "error": "Unauthorized" });
            (StatusCode::UNAUTHORIZED, axum::Json(body)).into_response()
        }
    }
}

// ---------------------------------------------------------------------------
// CSRF middleware
// ---------------------------------------------------------------------------

/// Axum middleware that enforces CSRF double-submit cookie protection.
///
/// For mutating HTTP methods (POST, PUT, DELETE), the request must include
/// both:
///   - A `runifi_csrf` cookie
///   - An `X-CSRF-Token` header with a value that matches the cookie
///
/// Requests that already carry a valid `Authorization: Bearer` header are
/// exempt from CSRF checks, because bearer tokens are never sent
/// automatically by browsers.
///
/// GET/HEAD/OPTIONS requests are always exempt (safe methods).
/// If auth is disabled entirely, CSRF is also disabled.
pub async fn csrf_middleware(State(state): State<ApiState>, req: Request, next: Next) -> Response {
    // CSRF only matters when auth is enabled.
    if !state.security.auth_enabled() {
        return next.run(req).await;
    }

    let method = req.method().clone();

    // Safe methods and exempt paths skip CSRF.
    if is_safe_method(&method) || is_exempt_path(req.uri().path()) {
        let mut response = next.run(req).await;
        // Set CSRF cookie on safe-method responses so the browser has it.
        ensure_csrf_cookie(&mut response);
        return response;
    }

    // If authenticated via Bearer header, skip CSRF (not browser-initiated).
    if has_bearer_auth(&req) {
        return next.run(req).await;
    }

    // For mutating requests without Bearer auth: enforce CSRF.
    let cookie_token = extract_csrf_cookie(&req);
    let header_token = req
        .headers()
        .get(CSRF_HEADER_NAME)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    match (cookie_token, header_token) {
        (Some(cookie), Some(hdr)) if constant_time_eq(&cookie, &hdr) => next.run(req).await,
        _ => {
            let body = serde_json::json!({ "error": "CSRF validation failed" });
            (StatusCode::FORBIDDEN, axum::Json(body)).into_response()
        }
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Extract the API key from the request (header or query param).
fn extract_api_key(req: &Request) -> Option<String> {
    // 1. Check Authorization: Bearer <key>
    if let Some(auth) = req.headers().get(header::AUTHORIZATION)
        && let Ok(value) = auth.to_str()
    {
        let trimmed = value.trim();
        if let Some(token) = trimmed.strip_prefix("Bearer ") {
            let token = token.trim();
            if !token.is_empty() {
                return Some(token.to_string());
            }
        }
    }

    // 2. Check ?api_key=<key> query parameter.
    if let Some(query) = req.uri().query() {
        for pair in query.split('&') {
            if let Some((key, value)) = pair.split_once('=')
                && key == "api_key"
            {
                // URL-decode the value (e.g. %20 -> space).
                let decoded = urlencoding::decode(value).unwrap_or_default();
                let decoded = decoded.trim();
                if !decoded.is_empty() {
                    return Some(decoded.to_string());
                }
            }
        }
    }

    None
}

/// Validate an API key against the configured keys using constant-time comparison.
fn validate_api_key(
    provided: &str,
    security: &runifi_core::config::flow_config::SecurityConfig,
) -> bool {
    let configured_keys = security.key_strings();
    let provided_bytes = provided.as_bytes();
    for key in configured_keys {
        if constant_time_eq(provided, key) && provided_bytes.len() == key.len() {
            return true;
        }
    }
    false
}

/// Resolve the [`Role`] for a validated API key.
///
/// Simple string keys (backward compat) are treated as `Admin`.
/// Structured keys use the configured role. Unknown role strings default to `Viewer`.
fn resolve_role(
    provided: &str,
    security: &runifi_core::config::flow_config::SecurityConfig,
) -> Option<Role> {
    let role_str = security.role_for_key(provided)?;
    Some(Role::parse(role_str).unwrap_or(Role::Viewer))
}

/// Constant-time string comparison to prevent timing attacks.
fn constant_time_eq(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.as_bytes().ct_eq(b.as_bytes()).into()
}

/// Returns `true` for HTTP methods that do not mutate state.
fn is_safe_method(method: &Method) -> bool {
    matches!(*method, Method::GET | Method::HEAD | Method::OPTIONS)
}

/// Returns `true` if the request has a Bearer Authorization header.
fn has_bearer_auth(req: &Request) -> bool {
    req.headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.trim().starts_with("Bearer "))
}

/// Extract the CSRF token from the `runifi_csrf` cookie.
fn extract_csrf_cookie(req: &Request) -> Option<String> {
    let cookie_header = req.headers().get(header::COOKIE)?;
    let cookie_str = cookie_header.to_str().ok()?;
    let prefix = format!("{}=", CSRF_COOKIE_NAME);
    for part in cookie_str.split(';') {
        let part = part.trim();
        if let Some(value) = part.strip_prefix(&prefix)
            && !value.is_empty()
        {
            return Some(value.to_string());
        }
    }
    None
}

/// Ensure the response has a CSRF cookie set.
fn ensure_csrf_cookie(response: &mut Response) {
    // Only set if not already present in the response.
    let has_csrf_cookie = response
        .headers()
        .get_all(header::SET_COOKIE)
        .iter()
        .any(|v| v.to_str().is_ok_and(|s| s.starts_with(CSRF_COOKIE_NAME)));

    if !has_csrf_cookie {
        let token = generate_csrf_token();
        let cookie_value = format!(
            "{}={}; Path=/; SameSite=Strict; HttpOnly",
            CSRF_COOKIE_NAME, token
        );
        if let Ok(hv) = cookie_value.parse() {
            response.headers_mut().append(header::SET_COOKIE, hv);
        }
    }
}

/// Hex-encode a byte slice.
fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exempt_paths() {
        assert!(is_exempt_path("/"));
        assert!(is_exempt_path("/assets/main.js"));
        assert!(is_exempt_path("/assets/css/style.css"));
        assert!(!is_exempt_path("/api/v1/processors"));
        assert!(!is_exempt_path("/api/v1/events"));
    }

    #[test]
    fn test_constant_time_eq() {
        assert!(constant_time_eq("abc123", "abc123"));
        assert!(!constant_time_eq("abc123", "abc124"));
        assert!(!constant_time_eq("abc123", "abc12"));
        assert!(!constant_time_eq("", "abc"));
        assert!(constant_time_eq("", ""));
    }

    #[test]
    fn test_validate_api_key_simple() {
        use runifi_core::config::flow_config::{ApiKeyEntry, SecurityConfig};
        let security = SecurityConfig {
            api_keys: vec![
                ApiKeyEntry::Simple("key-abc123".to_string()),
                ApiKeyEntry::Simple("key-def456".to_string()),
            ],
            ..SecurityConfig::default()
        };
        assert!(validate_api_key("key-abc123", &security));
        assert!(validate_api_key("key-def456", &security));
        assert!(!validate_api_key("key-invalid", &security));
        assert!(!validate_api_key("", &security));
        assert!(!validate_api_key("key-abc12", &security)); // partial match
    }

    #[test]
    fn test_validate_api_key_with_roles() {
        use runifi_core::config::flow_config::{ApiKeyEntry, ApiKeyWithRole, SecurityConfig};
        let security = SecurityConfig {
            api_keys: vec![
                ApiKeyEntry::WithRole(ApiKeyWithRole {
                    key: "admin-key".to_string(),
                    role: "admin".to_string(),
                }),
                ApiKeyEntry::WithRole(ApiKeyWithRole {
                    key: "viewer-key".to_string(),
                    role: "viewer".to_string(),
                }),
            ],
            ..SecurityConfig::default()
        };
        assert!(validate_api_key("admin-key", &security));
        assert!(validate_api_key("viewer-key", &security));
        assert!(!validate_api_key("unknown", &security));
    }

    #[test]
    fn test_resolve_role_simple_keys() {
        use runifi_core::config::flow_config::{ApiKeyEntry, SecurityConfig};
        let security = SecurityConfig {
            api_keys: vec![ApiKeyEntry::Simple("key-abc123".to_string())],
            ..SecurityConfig::default()
        };
        // Simple keys default to Admin.
        assert_eq!(resolve_role("key-abc123", &security), Some(Role::Admin));
        assert_eq!(resolve_role("unknown", &security), None);
    }

    #[test]
    fn test_resolve_role_with_roles() {
        use runifi_core::config::flow_config::{ApiKeyEntry, ApiKeyWithRole, SecurityConfig};
        let security = SecurityConfig {
            api_keys: vec![
                ApiKeyEntry::WithRole(ApiKeyWithRole {
                    key: "admin-key".to_string(),
                    role: "admin".to_string(),
                }),
                ApiKeyEntry::WithRole(ApiKeyWithRole {
                    key: "op-key".to_string(),
                    role: "operator".to_string(),
                }),
                ApiKeyEntry::WithRole(ApiKeyWithRole {
                    key: "view-key".to_string(),
                    role: "viewer".to_string(),
                }),
            ],
            ..SecurityConfig::default()
        };
        assert_eq!(resolve_role("admin-key", &security), Some(Role::Admin));
        assert_eq!(resolve_role("op-key", &security), Some(Role::Operator));
        assert_eq!(resolve_role("view-key", &security), Some(Role::Viewer));
        assert_eq!(resolve_role("missing", &security), None);
    }

    #[test]
    fn test_resolve_role_unknown_role_string() {
        use runifi_core::config::flow_config::{ApiKeyEntry, ApiKeyWithRole, SecurityConfig};
        let security = SecurityConfig {
            api_keys: vec![ApiKeyEntry::WithRole(ApiKeyWithRole {
                key: "bad-role-key".to_string(),
                role: "superadmin".to_string(),
            })],
            ..SecurityConfig::default()
        };
        // Unknown role strings default to Viewer for safety.
        assert_eq!(resolve_role("bad-role-key", &security), Some(Role::Viewer));
    }

    #[test]
    fn test_generate_csrf_token() {
        let token = generate_csrf_token();
        assert_eq!(token.len(), CSRF_TOKEN_HEX_LEN);
        // Should be valid hex.
        assert!(token.chars().all(|c| c.is_ascii_hexdigit()));
        // Two tokens should be different.
        let token2 = generate_csrf_token();
        assert_ne!(token, token2);
    }

    #[test]
    fn test_is_safe_method() {
        assert!(is_safe_method(&Method::GET));
        assert!(is_safe_method(&Method::HEAD));
        assert!(is_safe_method(&Method::OPTIONS));
        assert!(!is_safe_method(&Method::POST));
        assert!(!is_safe_method(&Method::PUT));
        assert!(!is_safe_method(&Method::DELETE));
    }

    #[test]
    fn test_hex_encode() {
        assert_eq!(hex_encode(&[0x00, 0xff, 0xab]), "00ffab");
        assert_eq!(hex_encode(&[]), "");
    }

    #[test]
    fn test_extract_csrf_cookie_exact_name() {
        // Should match "runifi_csrf=token" exactly, not "runifi_csrf_other=token".
        let mut req = Request::builder()
            .header(header::COOKIE, "runifi_csrf_other=bad; runifi_csrf=good")
            .body(axum::body::Body::empty())
            .unwrap();
        let token = extract_csrf_cookie(&req);
        assert_eq!(token.as_deref(), Some("good"));

        // Only the prefixed name should NOT match.
        req = Request::builder()
            .header(header::COOKIE, "runifi_csrf_other=bad")
            .body(axum::body::Body::empty())
            .unwrap();
        assert!(extract_csrf_cookie(&req).is_none());
    }

    #[test]
    fn test_extract_api_key_from_query() {
        // Basic query parameter.
        let req = Request::builder()
            .uri("/api/v1/events?api_key=my-secret")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(extract_api_key(&req).as_deref(), Some("my-secret"));

        // With other params before and after.
        let req = Request::builder()
            .uri("/api/v1/events?foo=bar&api_key=my-key&baz=qux")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(extract_api_key(&req).as_deref(), Some("my-key"));

        // URL-encoded value.
        let req = Request::builder()
            .uri("/api/v1/events?api_key=key%20with%20spaces")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(extract_api_key(&req).as_deref(), Some("key with spaces"));

        // No api_key param.
        let req = Request::builder()
            .uri("/api/v1/events?other=val")
            .body(axum::body::Body::empty())
            .unwrap();
        assert!(extract_api_key(&req).is_none());
    }

    #[test]
    fn test_extract_api_key_from_bearer() {
        let req = Request::builder()
            .header(header::AUTHORIZATION, "Bearer my-token")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(extract_api_key(&req).as_deref(), Some("my-token"));

        // Empty bearer should return None.
        let req = Request::builder()
            .header(header::AUTHORIZATION, "Bearer ")
            .body(axum::body::Body::empty())
            .unwrap();
        assert!(extract_api_key(&req).is_none());
    }
}
