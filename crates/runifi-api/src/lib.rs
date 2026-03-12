mod dashboard;
mod dto;
mod error;
mod routes;
mod state;

use std::net::SocketAddr;

use axum::Router;
use tower_http::cors::{Any, CorsLayer};

use runifi_core::engine::handle::EngineHandle;
use state::ApiState;

/// Create the API router with all routes.
pub fn create_router(handle: EngineHandle) -> Router {
    let state = ApiState::new(handle);
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    Router::new()
        .merge(routes::system::routes())
        .merge(routes::processors::routes())
        .merge(routes::connections::routes())
        .merge(routes::flow::routes())
        .merge(routes::plugins::routes())
        .merge(routes::events::routes())
        .merge(routes::bulletins::routes())
        .merge(dashboard::routes())
        .layer(cors)
        .with_state(state)
}

/// Start the API server on the given address. Runs until the future is dropped.
pub async fn start_api_server(
    handle: EngineHandle,
    bind_address: &str,
    port: u16,
) -> std::io::Result<()> {
    let app = create_router(handle);
    let addr: SocketAddr = format!("{}:{}", bind_address, port)
        .parse()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;

    tracing::info!(%addr, "API server listening");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await
}
