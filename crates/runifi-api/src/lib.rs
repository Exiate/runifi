mod dashboard;
mod dto;
mod error;
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

use runifi_core::config::flow_config::ApiConfig;
use runifi_core::engine::handle::EngineHandle;
use state::ApiState;

/// Per-IP rate limiter type.
type IpRateLimiter = Arc<RateLimiter<std::net::IpAddr, DashMapStateStore<std::net::IpAddr>, DefaultClock>>;

/// Create the API router with all routes and security middleware.
pub fn create_router(handle: EngineHandle, api_config: &ApiConfig) -> Router {
    let state = ApiState::with_config(
        handle,
        api_config.max_sse_connections,
        api_config.detailed_errors,
    );

    // -- CORS --
    let cors = build_cors_layer(&api_config.cors_allowed_origins);

    // -- Request body size limit --
    let body_limit = RequestBodyLimitLayer::new(api_config.max_request_body_bytes);

    // -- Rate limiting --
    let rate_limit_per_second = api_config.rate_limit_per_second;
    let rate_limiter: IpRateLimiter = Arc::new(RateLimiter::dashmap(
        Quota::per_second(
            NonZeroU32::new(rate_limit_per_second).unwrap_or(NonZeroU32::new(100).unwrap()),
        ),
    ));

    let rate_limiter_clone = rate_limiter.clone();

    Router::new()
        .merge(routes::system::routes())
        .merge(routes::processors::routes())
        .merge(routes::connections::routes())
        .merge(routes::flow::routes())
        .merge(routes::plugins::routes())
        .merge(routes::events::routes())
        .merge(routes::bulletins::routes())
        .merge(dashboard::routes())
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
        return (
            axum::http::StatusCode::TOO_MANY_REQUESTS,
            axum::Json(body),
        )
            .into_response();
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

/// Start the API server on the given address. Runs until the future is dropped.
pub async fn start_api_server(
    handle: EngineHandle,
    api_config: &ApiConfig,
) -> std::io::Result<()> {
    let app = create_router(handle, api_config);
    let addr: SocketAddr = format!("{}:{}", api_config.bind_address, api_config.port)
        .parse()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;

    tracing::info!(%addr, "API server listening");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    // Use into_make_service_with_connect_info so ConnectInfo<SocketAddr> is available.
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
}
