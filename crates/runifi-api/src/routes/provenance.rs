use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router, middleware};

use runifi_core::repository::provenance_repo::{ProvenanceEventType, ProvenanceQuery};

use crate::dto::{
    ProvenanceEventResponse, ProvenanceLineageResponse, ProvenanceReplayResponse,
    ProvenanceSearchParams, ProvenanceSearchResponse,
};
use crate::error::ApiError;
use crate::rbac;
use crate::state::ApiState;

pub fn routes() -> Router<ApiState> {
    // Search and lineage endpoints — ViewFlow (Viewer+)
    let view_routes = Router::new()
        .route("/api/v1/provenance/search", get(search_provenance))
        .route("/api/v1/provenance/{flowfile_id}/lineage", get(get_lineage))
        .route("/api/v1/provenance/events/{event_id}", get(get_event))
        .layer(middleware::from_fn(rbac::require_view_flow));

    // Replay endpoint — OperateProcessors (Operator+)
    let operate_routes = Router::new()
        .route(
            "/api/v1/provenance/{event_id}/replay",
            post(replay_flowfile),
        )
        .layer(middleware::from_fn(rbac::require_operate_processors));

    view_routes.merge(operate_routes)
}

/// Search provenance events with optional filters.
///
/// Query parameters:
/// - `flowfile_id`: Filter by FlowFile ID
/// - `processor`: Filter by processor name
/// - `event_type`: Filter by event type (CREATE, SEND, RECEIVE, etc.)
/// - `start_time`: Filter events after this timestamp (ms since epoch)
/// - `end_time`: Filter events before this timestamp (ms since epoch)
/// - `max_results`: Maximum results (default 100)
/// - `offset`: Pagination offset (default 0)
async fn search_provenance(
    State(state): State<ApiState>,
    Query(params): Query<ProvenanceSearchParams>,
) -> Result<Json<ProvenanceSearchResponse>, ApiError> {
    let event_type = if let Some(ref et_str) = params.event_type {
        let et = ProvenanceEventType::parse(et_str).ok_or_else(|| {
            ApiError::BadRequest(format!(
                "Invalid event type '{}'. Valid types: CREATE, RECEIVE, SEND, CLONE, \
                 CONTENT_MODIFIED, ATTRIBUTES_MODIFIED, ROUTE, DROP",
                et_str
            ))
        })?;
        Some(et)
    } else {
        None
    };

    let max_results = params.max_results.unwrap_or(100).min(1000);
    let offset = params.offset.unwrap_or(0);

    let query = ProvenanceQuery {
        flowfile_id: params.flowfile_id,
        processor_name: params.processor,
        event_type,
        // Convert milliseconds to nanoseconds for internal query.
        start_time_nanos: params.start_time.map(|ms| ms * 1_000_000),
        end_time_nanos: params.end_time.map(|ms| ms * 1_000_000),
        max_results,
        offset,
    };

    let result = state.handle.provenance_repo.search(&query);

    let events: Vec<ProvenanceEventResponse> = result
        .events
        .iter()
        .map(ProvenanceEventResponse::from_event)
        .collect();

    Ok(Json(ProvenanceSearchResponse {
        events,
        total_count: result.total_count,
        offset,
        max_results,
    }))
}

/// Get the full lineage graph for a FlowFile.
///
/// Returns all provenance events that share the same lineage_start_id
/// as the given FlowFile, providing a complete picture of all ancestor
/// and descendant FlowFiles.
async fn get_lineage(
    State(state): State<ApiState>,
    Path(flowfile_id): Path<u64>,
) -> Result<Json<ProvenanceLineageResponse>, ApiError> {
    let events = state.handle.provenance_repo.get_lineage(flowfile_id);

    if events.is_empty() {
        return Err(ApiError::FlowFileNotFound(flowfile_id));
    }

    let lineage_start_id = events
        .first()
        .map(|e| e.lineage_start_id)
        .unwrap_or(flowfile_id);

    let event_responses: Vec<ProvenanceEventResponse> = events
        .iter()
        .map(ProvenanceEventResponse::from_event)
        .collect();

    Ok(Json(ProvenanceLineageResponse {
        flowfile_id,
        lineage_start_id,
        events: event_responses,
    }))
}

/// Get a single provenance event by ID.
async fn get_event(
    State(state): State<ApiState>,
    Path(event_id): Path<u64>,
) -> Result<Json<ProvenanceEventResponse>, ApiError> {
    let event = state
        .handle
        .provenance_repo
        .get_event(event_id)
        .ok_or(ApiError::ProvenanceEventNotFound(event_id))?;

    Ok(Json(ProvenanceEventResponse::from_event(&event)))
}

/// Replay a FlowFile from a specific provenance event.
///
/// This re-creates the FlowFile as it existed at the time of the provenance
/// event and re-injects it into the processor that produced the event.
/// The replayed FlowFile will have the same attributes as the original
/// but a new unique ID.
async fn replay_flowfile(
    State(state): State<ApiState>,
    Path(event_id): Path<u64>,
) -> Result<impl IntoResponse, ApiError> {
    let event = state
        .handle
        .provenance_repo
        .get_event(event_id)
        .ok_or(ApiError::ProvenanceEventNotFound(event_id))?;

    // Find the processor to replay into.
    let proc_info = state
        .handle
        .get_processor_info(&event.processor_name)
        .ok_or_else(|| ApiError::ProcessorNotFound(event.processor_name.clone()))?;

    // Check processor is running.
    let enabled = proc_info
        .metrics
        .enabled
        .load(std::sync::atomic::Ordering::Relaxed);
    if !enabled {
        return Err(ApiError::Conflict(format!(
            "Processor '{}' is not running. Start it before replaying.",
            event.processor_name
        )));
    }

    // Re-create the FlowFile with attributes from the provenance event.
    let mut replay_ff = runifi_plugin_api::FlowFile {
        id: 0, // Will be set after injection.
        attributes: event
            .attributes
            .iter()
            .map(|(k, v)| {
                (
                    std::sync::Arc::from(k.as_str()),
                    std::sync::Arc::from(v.as_str()),
                )
            })
            .collect(),
        content_claim: None,
        size: event.content_size,
        created_at_nanos: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64,
        lineage_start_id: event.lineage_start_id,
        penalized_until_nanos: 0,
    };

    // Find an input connection to the target processor and inject.
    let input_conns = proc_info.input_connections.read();
    if input_conns.is_empty() {
        return Err(ApiError::BadRequest(format!(
            "Processor '{}' has no input connections. Cannot replay.",
            event.processor_name
        )));
    }

    // Use the FlowFile ID from the connection for uniqueness.
    // We set a temporary ID; the processor will give it a new one on get().
    replay_ff.id = event.flowfile_id;

    let injected = input_conns[0].try_send(replay_ff).is_ok();

    if !injected {
        return Err(ApiError::Conflict(format!(
            "Input connection to processor '{}' is full. Try again later.",
            event.processor_name
        )));
    }

    Ok((
        StatusCode::ACCEPTED,
        Json(ProvenanceReplayResponse {
            status: "replaying".to_string(),
            event_id,
            flowfile_id: event.flowfile_id,
            processor_name: event.processor_name.clone(),
            message: format!(
                "FlowFile re-injected into processor '{}' from provenance event {}",
                event.processor_name, event_id
            ),
        }),
    )
        .into_response())
}
