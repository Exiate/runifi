use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router, middleware};

use crate::dto::PluginResponse;
use crate::rbac;
use crate::state::ApiState;

pub fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/v1/plugins", get(list_plugins))
        .layer(middleware::from_fn(rbac::require_view_flow))
}

async fn list_plugins(State(state): State<ApiState>) -> Json<Vec<PluginResponse>> {
    let plugins: Vec<PluginResponse> = state
        .handle
        .plugin_types
        .iter()
        .map(|p| PluginResponse::from_kind(&p.type_name, p.kind))
        .collect();
    Json(plugins)
}
