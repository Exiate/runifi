pub mod auth;
mod dashboard;
mod dto;
mod error;
pub mod rbac;
mod routes;
mod state;

use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::sync::Arc;

use axum::Router;
use axum::extract::ConnectInfo;
use axum::http::{HeaderValue, Method};
use governor::{Quota, RateLimiter, clock::DefaultClock, state::keyed::DashMapStateStore};
use tower_http::cors::CorsLayer;
use tower_http::limit::RequestBodyLimitLayer;

use runifi_core::auth::chain::AuthProviderChain;
use runifi_core::auth::identity_mapper::IdentityMapper;
use runifi_core::auth::jwt::JwtConfig;
use runifi_core::auth::session::SessionManager;
use runifi_core::auth::store::UserStore;
use runifi_core::config::flow_config::{ApiConfig, AuthConfig};
use runifi_core::engine::handle::EngineHandle;
use runifi_core::registry::plugin_registry::PluginRegistry;
use runifi_core::versioning::FlowVersionStore;
use state::ApiState;

/// Bundle of auth infrastructure passed from the server to the API layer.
#[derive(Clone)]
pub struct AuthChainBundle {
    pub chain: Arc<AuthProviderChain>,
    pub mapper: Arc<IdentityMapper>,
    pub session_manager: Arc<SessionManager>,
}

/// Per-IP rate limiter type.
type IpRateLimiter =
    Arc<RateLimiter<std::net::IpAddr, DashMapStateStore<std::net::IpAddr>, DefaultClock>>;

/// Create the API router with all routes and security middleware.
pub fn create_router(handle: EngineHandle, api_config: &ApiConfig) -> Router {
    create_router_with_registry(handle, api_config, None, None, None, None, None, None)
}

/// Create the API router with an optional plugin registry for service creation.
#[allow(clippy::too_many_arguments)]
pub fn create_router_with_registry(
    handle: EngineHandle,
    api_config: &ApiConfig,
    plugin_registry: Option<Arc<PluginRegistry>>,
    user_store: Option<Arc<UserStore>>,
    jwt_config: Option<JwtConfig>,
    auth_config: Option<AuthConfig>,
    version_store: Option<Arc<FlowVersionStore>>,
    auth_bundle: Option<AuthChainBundle>,
) -> Router {
    let mut state = ApiState::with_config(
        handle,
        api_config.max_sse_connections,
        api_config.detailed_errors,
        api_config.security.clone(),
    );
    if let Some(registry) = plugin_registry {
        state.set_plugin_registry(registry);
    }
    if let Some(store) = user_store
        && let Some(jwt) = jwt_config
    {
        let ac = auth_config.unwrap_or_default();
        state.set_auth(store, jwt, ac);
    }
    if let Some(vs) = version_store {
        state.set_version_store(vs);
    }
    if let Some(bundle) = auth_bundle {
        state.set_auth_chain(bundle.chain, bundle.mapper, bundle.session_manager);
    }

    // -- CORS --
    let cors = build_cors_layer(&api_config.cors_allowed_origins);

    // -- Request body size limit --
    let body_limit = RequestBodyLimitLayer::new(api_config.max_request_body_bytes);

    // -- Rate limiting --
    let rate_limit_per_second = api_config.rate_limit_per_second;
    let rate_limiter: IpRateLimiter = Arc::new(RateLimiter::dashmap(Quota::per_second(
        NonZeroU32::new(rate_limit_per_second).unwrap_or(NonZeroU32::new(100).unwrap()),
    )));

    let rate_limiter_clone = rate_limiter.clone();

    Router::new()
        .merge(routes::system::routes())
        .merge(routes::processors::routes())
        .merge(routes::connections::routes())
        .merge(routes::flow::routes())
        .merge(routes::plugins::routes())
        .merge(routes::events::routes())
        .merge(routes::bulletins::routes())
        .merge(routes::services::routes())
        .merge(routes::auth_routes::routes())
        .merge(routes::users::routes())
        .merge(routes::user_groups::routes())
        .merge(routes::labels::routes())
        .merge(routes::provenance::routes())
        .merge(routes::reporting_tasks::routes())
        .merge(routes::versions::routes())
        .merge(routes::process_groups::routes())
        .merge(dashboard::routes())
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth::csrf_middleware,
        ))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth::auth_middleware,
        ))
        .layer(axum::middleware::from_fn(move |req, next| {
            let limiter = rate_limiter_clone.clone();
            rate_limit_middleware(limiter, req, next)
        }))
        .layer(body_limit)
        .layer(cors)
        .with_state(state)
}

/// Build a CORS layer from the configured allowed origins.
fn build_cors_layer(allowed_origins: &[String]) -> CorsLayer {
    let methods = vec![Method::GET, Method::POST, Method::PUT, Method::DELETE];
    let headers = vec![
        axum::http::header::CONTENT_TYPE,
        axum::http::header::AUTHORIZATION,
    ];

    if allowed_origins.is_empty() {
        // No CORS headers at all — same-origin only.
        CorsLayer::new()
            .allow_methods(methods)
            .allow_headers(headers)
    } else {
        let origins: Vec<HeaderValue> = allowed_origins
            .iter()
            .filter_map(|o| o.parse::<HeaderValue>().ok())
            .collect();

        CorsLayer::new()
            .allow_origin(origins)
            .allow_methods(methods)
            .allow_headers(headers)
    }
}

/// Rate limiting middleware that checks per-IP request rates.
async fn rate_limit_middleware(
    limiter: IpRateLimiter,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    // Extract client IP from ConnectInfo or fall back to X-Forwarded-For / peer addr.
    let ip = extract_client_ip(&req);

    if let Some(ip) = ip
        && limiter.check_key(&ip).is_err()
    {
        let body = serde_json::json!({ "error": "Too many requests" });
        return (axum::http::StatusCode::TOO_MANY_REQUESTS, axum::Json(body)).into_response();
    }

    next.run(req).await
}

/// Extract the client IP address from the request.
fn extract_client_ip(req: &axum::extract::Request) -> Option<std::net::IpAddr> {
    // Try X-Forwarded-For header first (for reverse proxies).
    if let Some(forwarded) = req.headers().get("x-forwarded-for")
        && let Ok(value) = forwarded.to_str()
        && let Some(first) = value.split(',').next()
        && let Ok(ip) = first.trim().parse()
    {
        return Some(ip);
    }

    // Try ConnectInfo extension (set by axum::serve for TCP listeners).
    req.extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ci| ci.0.ip())
}

use axum::response::IntoResponse;

/// Check security configuration and log appropriate warnings.
/// Returns an error if the configuration is invalid (auth over plain HTTP).
fn validate_security_config(api_config: &ApiConfig) -> Result<(), std::io::Error> {
    let security = &api_config.security;
    let bind_addr: std::net::IpAddr = api_config
        .bind_address
        .parse()
        .unwrap_or(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));

    let is_loopback = bind_addr.is_loopback();

    // Refuse to start with auth enabled over plain HTTP (non-loopback).
    // API keys sent in the clear over a network are a security risk.
    if security.auth_enabled() && !security.tls_enabled && !is_loopback {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Refusing to start: API key authentication is enabled but TLS is disabled \
             on a non-loopback address. Either enable TLS, bind to 127.0.0.1, or \
             remove API keys from the configuration.",
        ));
    }

    // Warn when binding to a non-loopback address without TLS.
    if !is_loopback && !security.tls_enabled {
        tracing::warn!(
            bind_address = %api_config.bind_address,
            "API server is binding to a non-loopback address without TLS. \
             Traffic will be unencrypted. Consider enabling TLS in [api.security]."
        );
    }

    // Warn when auth is enabled over loopback without TLS (informational).
    if security.auth_enabled() && !security.tls_enabled && is_loopback {
        tracing::info!(
            "API key authentication active over loopback (no TLS). \
             Acceptable for development; enable TLS for production."
        );
    }

    // Validate TLS config completeness.
    if security.tls_enabled && (security.tls_cert_path.is_none() || security.tls_key_path.is_none())
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "TLS is enabled but tls_cert_path and/or tls_key_path are not set.",
        ));
    }

    Ok(())
}

/// Start the API server on the given address. Runs until the future is dropped.
///
/// When TLS is configured, the server uses `axum-server` with rustls. Otherwise
/// it falls back to plain-text HTTP via `tokio::net::TcpListener`.
pub async fn start_api_server(handle: EngineHandle, api_config: &ApiConfig) -> std::io::Result<()> {
    start_api_server_with_registry(handle, api_config, None, None, None, None, None, None).await
}

/// Start the API server with optional plugin registry, auth, and versioning support.
#[allow(clippy::too_many_arguments)]
pub async fn start_api_server_with_registry(
    handle: EngineHandle,
    api_config: &ApiConfig,
    plugin_registry: Option<Arc<PluginRegistry>>,
    user_store: Option<Arc<UserStore>>,
    jwt_config: Option<JwtConfig>,
    auth_config: Option<AuthConfig>,
    version_store: Option<Arc<FlowVersionStore>>,
    auth_bundle: Option<AuthChainBundle>,
) -> std::io::Result<()> {
    // Validate security posture before binding.
    validate_security_config(api_config)?;

    let app = create_router_with_registry(
        handle,
        api_config,
        plugin_registry,
        user_store,
        jwt_config,
        auth_config,
        version_store,
        auth_bundle,
    );
    let addr: SocketAddr = format!("{}:{}", api_config.bind_address, api_config.port)
        .parse()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;

    let security = &api_config.security;

    if security.auth_enabled() {
        tracing::info!(
            key_count = security.api_keys.len(),
            "API key authentication enabled"
        );
    } else {
        tracing::warn!(
            "API key authentication is DISABLED. All endpoints are publicly accessible."
        );
    }

    if security.tls_enabled {
        let cert_path = security.tls_cert_path.as_deref().expect("validated above");
        let key_path = security.tls_key_path.as_deref().expect("validated above");

        tracing::info!(
            %addr,
            cert = %cert_path,
            "API server listening (HTTPS/TLS)"
        );

        let tls_config = axum_server::tls_rustls::RustlsConfig::from_pem_file(cert_path, key_path)
            .await
            .map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Failed to load TLS certificates: {}", e),
                )
            })?;

        axum_server::bind_rustls(addr, tls_config)
            .serve(app.into_make_service_with_connect_info::<SocketAddr>())
            .await
    } else {
        tracing::info!(%addr, "API server listening (HTTP)");

        let listener = tokio::net::TcpListener::bind(addr).await?;
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
    }
}
