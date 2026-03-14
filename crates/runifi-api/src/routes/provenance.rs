use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router, middleware};

use runifi_core::repository::provenance_repo::{ProvenanceEventType, ProvenanceQuery};

use crate::dto::{
    ProvenanceEventResponse, ProvenanceLineageResponse, ProvenanceReplayResponse,
    ProvenanceSearchParams, ProvenanceSearchResponse, ProvenanceStatsResponse,
};
use crate::error::ApiError;
use crate::rbac;
use crate::state::ApiState;

pub fn routes() -> Router<ApiState> {
    // Search, stats, lineage, and content endpoints — ViewFlow (Viewer+)
    let view_routes = Router::new()
        .route("/api/v1/provenance/search", get(search_provenance))
        .route("/api/v1/provenance/stats", get(get_stats))
        .route("/api/v1/provenance/{flowfile_id}/lineage", get(get_lineage))
        .route("/api/v1/provenance/events/{event_id}", get(get_event))
        .route(
            "/api/v1/provenance/events/{event_id}/content",
            get(download_content),
        )
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
                 CONTENT_MODIFIED, ATTRIBUTES_MODIFIED, ROUTE, DROP, FORK, JOIN, \
                 FETCH, EXPIRE, REPLAY, DOWNLOAD, ADDINFO",
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
        processor_type: params.processor_type,
        min_size: params.min_size,
        max_size: params.max_size,
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

/// Get provenance repository statistics.
async fn get_stats(
    State(state): State<ApiState>,
) -> Result<Json<ProvenanceStatsResponse>, ApiError> {
    let stats = state.handle.provenance_repo.stats();
    Ok(Json(ProvenanceStatsResponse {
        event_count: stats.event_count,
        oldest_timestamp_ms: stats.oldest_timestamp_nanos.map(|ns| ns / 1_000_000),
        newest_timestamp_ms: stats.newest_timestamp_nanos.map(|ns| ns / 1_000_000),
    }))
}

/// Download the content of a FlowFile at the time of a provenance event.
/// Returns 410 Gone if the content has been garbage collected.
async fn download_content(
    State(state): State<ApiState>,
    Path(event_id): Path<u64>,
) -> Result<impl IntoResponse, ApiError> {
    let event = state
        .handle
        .provenance_repo
        .get_event(event_id)
        .ok_or(ApiError::ProvenanceEventNotFound(event_id))?;

    let claim_id = event
        .content_claim_id
        .ok_or(ApiError::ContentGone(event_id))?;

    let claim = runifi_plugin_api::ContentClaim {
        resource_id: claim_id,
        offset: 0,
        length: event.content_size,
    };

    let content = state
        .handle
        .content_repo
        .read(&claim)
        .map_err(|_| ApiError::ContentGone(event_id))?;

    let headers = [
        (axum::http::header::CONTENT_TYPE, "application/octet-stream"),
        (
            axum::http::header::CONTENT_DISPOSITION,
            "attachment; filename=\"provenance-content.bin\"",
        ),
    ];

    Ok((StatusCode::OK, headers, content.to_vec()))
}

/// Replay a FlowFile from a specific provenance event.
///
/// Re-creates the FlowFile as it existed at the time of the provenance event
/// and re-injects it into the processor. Records a REPLAY provenance event.
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

    // Restore content claim if available.
    let content_claim = if let Some(claim_id) = event.content_claim_id {
        let probe = runifi_plugin_api::ContentClaim {
            resource_id: claim_id,
            offset: 0,
            length: event.content_size,
        };
        if state.handle.content_repo.read(&probe).is_ok() {
            Some(probe)
        } else {
            None
        }
    } else {
        None
    };

    // Re-create the FlowFile with attributes from the provenance event.
    let mut replay_ff = runifi_plugin_api::FlowFile {
        id: 0,
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
        content_claim,
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

    replay_ff.id = event.flowfile_id;
    let injected = input_conns[0].try_send(replay_ff).is_ok();

    if !injected {
        return Err(ApiError::Conflict(format!(
            "Input connection to processor '{}' is full. Try again later.",
            event.processor_name
        )));
    }

    // Record a REPLAY provenance event.
    let now_nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    let replay_event = runifi_core::repository::provenance_repo::ProvenanceEvent {
        event_id: 0,
        flowfile_id: event.flowfile_id,
        event_type: ProvenanceEventType::Replay,
        processor_name: event.processor_name.clone(),
        processor_type: event.processor_type.clone(),
        timestamp_nanos: now_nanos,
        attributes: event.attributes.clone(),
        content_size: event.content_size,
        lineage_start_id: event.lineage_start_id,
        relationship: None,
        source_flowfile_id: None,
        details: format!("Replayed from provenance event {event_id}"),
        parent_flowfile_ids: Vec::new(),
        child_flowfile_ids: Vec::new(),
        transit_uri: None,
        content_claim_id: event.content_claim_id,
        previous_attributes: Vec::new(),
    };
    state.handle.provenance_repo.record(replay_event);

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
