use axum::extract::{Path, State};
use axum::routing::{delete, get, post};
use axum::{Json, Router, middleware};
use serde::Serialize;

use runifi_core::cluster::node::{ClusterRole, NodeState, NodeSummary};
use runifi_core::cluster::protocol::NodeMetricsSummary;

use crate::error::ApiError;
use crate::rbac;
use crate::state::ApiState;

pub fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/v1/cluster/nodes", get(list_nodes))
        .route("/api/v1/cluster/nodes/{id}", get(get_node))
        .route(
            "/api/v1/cluster/nodes/{id}/disconnect",
            post(disconnect_node),
        )
        .route("/api/v1/cluster/nodes/{id}/connect", post(connect_node))
        .route(
            "/api/v1/cluster/nodes/{id}/decommission",
            post(decommission_node),
        )
        .route("/api/v1/cluster/nodes/{id}", delete(force_remove_node))
        .route(
            "/api/v1/cluster/nodes/{id}/primary",
            post(designate_primary),
        )
        .route("/api/v1/cluster/status", get(cluster_status))
        .layer(middleware::from_fn(rbac::require_view_flow))
}

/// Response for listing all cluster nodes.
#[derive(Serialize)]
struct ClusterNodesResponse {
    nodes: Vec<NodeResponse>,
    connected_count: usize,
    total_count: usize,
    has_quorum: bool,
}

/// Detailed node response with all available info.
#[derive(Serialize)]
struct NodeResponse {
    id: String,
    address: String,
    state: NodeState,
    roles: Vec<ClusterRole>,
    missed_heartbeats: u32,
    flow_version: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    metrics: Option<NodeMetricsSummary>,
    #[serde(skip_serializing_if = "Option::is_none")]
    uptime_secs: Option<u64>,
}

impl From<NodeSummary> for NodeResponse {
    fn from(s: NodeSummary) -> Self {
        Self {
            id: s.id,
            address: s.address,
            state: s.state,
            roles: s.roles,
            missed_heartbeats: s.missed_heartbeats,
            flow_version: s.flow_version,
            metrics: s.metrics,
            uptime_secs: s.uptime_secs,
        }
    }
}

/// Cluster status summary response.
#[derive(Serialize)]
struct ClusterStatusResponse {
    enabled: bool,
    node_id: String,
    state: NodeState,
    roles: Vec<ClusterRole>,
    connected_count: usize,
    total_count: usize,
    flow_version: u64,
    election_term: u64,
    has_quorum: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    coordinator_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    primary_id: Option<String>,
}

/// `GET /api/v1/cluster/nodes` — list all cluster nodes with metrics.
async fn list_nodes(State(state): State<ApiState>) -> Result<Json<ClusterNodesResponse>, ApiError> {
    let coord = state
        .cluster_coordinator
        .as_ref()
        .ok_or(ApiError::ClusterNotEnabled)?;

    let summaries = coord.all_node_summaries();
    let connected_count = coord.connected_node_count();
    let total_count = coord.total_node_count();
    let has_quorum = coord.has_quorum();

    Ok(Json(ClusterNodesResponse {
        nodes: summaries.into_iter().map(NodeResponse::from).collect(),
        connected_count,
        total_count,
        has_quorum,
    }))
}

/// `GET /api/v1/cluster/nodes/{id}` — get detailed info for a specific node.
async fn get_node(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<NodeResponse>, ApiError> {
    let coord = state
        .cluster_coordinator
        .as_ref()
        .ok_or(ApiError::ClusterNotEnabled)?;

    let summary = coord
        .node_info(&id)
        .ok_or(ApiError::ClusterNodeNotFound(id))?;

    Ok(Json(NodeResponse::from(summary)))
}

/// `POST /api/v1/cluster/nodes/{id}/disconnect` — manually disconnect a node.
async fn disconnect_node(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let coord = state
        .cluster_coordinator
        .as_ref()
        .ok_or(ApiError::ClusterNotEnabled)?;

    coord
        .disconnect_node(&id)
        .await
        .map_err(|e| ApiError::ClusterError(e.to_string()))?;

    Ok(Json(serde_json::json!({
        "status": "disconnected",
        "node_id": id
    })))
}

/// `POST /api/v1/cluster/nodes/{id}/connect` — reconnect a previously disconnected node.
async fn connect_node(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let coord = state
        .cluster_coordinator
        .as_ref()
        .ok_or(ApiError::ClusterNotEnabled)?;

    coord
        .connect_node(&id)
        .await
        .map_err(|e| ApiError::ClusterError(e.to_string()))?;

    Ok(Json(serde_json::json!({
        "status": "connecting",
        "node_id": id
    })))
}

/// `POST /api/v1/cluster/nodes/{id}/decommission` — begin graceful decommission.
async fn decommission_node(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let coord = state
        .cluster_coordinator
        .as_ref()
        .ok_or(ApiError::ClusterNotEnabled)?;

    coord
        .decommission_node(&id)
        .await
        .map_err(|e| ApiError::ClusterError(e.to_string()))?;

    Ok(Json(serde_json::json!({
        "status": "decommissioning",
        "node_id": id
    })))
}

/// `DELETE /api/v1/cluster/nodes/{id}` — force-remove a stuck/disconnected node.
async fn force_remove_node(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let coord = state
        .cluster_coordinator
        .as_ref()
        .ok_or(ApiError::ClusterNotEnabled)?;

    coord
        .force_remove_node(&id)
        .map_err(|e| ApiError::ClusterError(e.to_string()))?;

    Ok(Json(serde_json::json!({
        "status": "removed",
        "node_id": id
    })))
}

/// `POST /api/v1/cluster/nodes/{id}/primary` — designate a node as primary.
async fn designate_primary(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let coord = state
        .cluster_coordinator
        .as_ref()
        .ok_or(ApiError::ClusterNotEnabled)?;

    coord
        .designate_primary(&id)
        .await
        .map_err(|e| ApiError::ClusterError(e.to_string()))?;

    Ok(Json(serde_json::json!({
        "status": "primary_designated",
        "node_id": id
    })))
}

/// `GET /api/v1/cluster/status` — cluster overview.
async fn cluster_status(
    State(state): State<ApiState>,
) -> Result<Json<ClusterStatusResponse>, ApiError> {
    let coord = state
        .cluster_coordinator
        .as_ref()
        .ok_or(ApiError::ClusterNotEnabled)?;

    let status = coord.status();

    Ok(Json(ClusterStatusResponse {
        enabled: status.enabled,
        node_id: status.node_id,
        state: status.state,
        roles: status.roles,
        connected_count: coord.connected_node_count(),
        total_count: coord.total_node_count(),
        flow_version: status.flow_version,
        election_term: status.election_term,
        has_quorum: coord.has_quorum(),
        coordinator_id: coord.coordinator_node_id(),
        primary_id: coord.primary_node_id(),
    }))
}
