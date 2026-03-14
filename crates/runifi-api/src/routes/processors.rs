use std::collections::HashMap;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{delete as delete_method, get, post, put};
use axum::{Json, Router, middleware};
use serde::{Deserialize, Serialize};

use crate::dto::{
    CreateProcessorRequest, ProcessorConfigResponse, ProcessorConfigUpdateRequest,
    ProcessorDetailResponse, ProcessorResponse, RelationshipResponse, UpdatePositionRequest,
};
use crate::error::ApiError;
use crate::rbac;
use crate::state::ApiState;

pub fn routes() -> Router<ApiState> {
    // GET endpoints — ViewFlow (Viewer+)
    let view_routes = Router::new()
        .route("/api/v1/processors", get(list_processors))
        .route("/api/v1/processors/{name}", get(get_processor))
        .route(
            "/api/v1/processors/{name}/config",
            get(get_processor_config),
        )
        .route("/api/v1/processors/{name}/state", get(get_processor_state))
        .layer(middleware::from_fn(rbac::require_view_flow));

    // POST lifecycle endpoints — OperateProcessors (Operator+)
    let operate_routes = Router::new()
        .route(
            "/api/v1/processors/{name}/reset-circuit",
            post(reset_circuit),
        )
        .route("/api/v1/processors/{name}/stop", post(stop_processor))
        .route("/api/v1/processors/{name}/start", post(start_processor))
        .route("/api/v1/processors/{name}/pause", post(pause_processor))
        .route("/api/v1/processors/{name}/resume", post(resume_processor))
        .route("/api/v1/processors/{name}/disable", put(disable_processor))
        .route("/api/v1/processors/{name}/enable", put(enable_processor))
        .route("/api/v1/processors/{name}/validation", get(get_validation))
        .route(
            "/api/v1/processors/{name}/state",
            delete_method(clear_processor_state),
        )
        .layer(middleware::from_fn(rbac::require_operate_processors));

    // Flow mutation endpoints — ModifyFlow (Admin only)
    let modify_routes = Router::new()
        .route("/api/v1/processors", post(create_processor))
        .route("/api/v1/processors/{name}", delete_method(delete_processor))
        .route("/api/v1/processors/{name}/position", put(update_position))
        .layer(middleware::from_fn(rbac::require_modify_flow));

    // Config update endpoint — ManageConfig (Admin only)
    let config_routes = Router::new()
        .route(
            "/api/v1/processors/{name}/config",
            put(update_processor_config),
        )
        .layer(middleware::from_fn(rbac::require_manage_config));

    view_routes
        .merge(operate_routes)
        .merge(modify_routes)
        .merge(config_routes)
}

async fn list_processors(State(state): State<ApiState>) -> Json<Vec<ProcessorResponse>> {
    let processors: Vec<ProcessorResponse> = state
        .handle
        .processors
        .read()
        .iter()
        .map(ProcessorResponse::from_info)
        .collect();
    Json(processors)
}

async fn get_processor(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> Result<Json<ProcessorResponse>, ApiError> {
    let info = state
        .handle
        .get_processor_info(&name)
        .ok_or(ApiError::ProcessorNotFound(name))?;

    Ok(Json(ProcessorResponse::from_info(&info)))
}

fn validate_processor_name(name: &str) -> Result<(), ApiError> {
    if name.is_empty() {
        return Err(ApiError::BadRequest(
            "Processor name must not be empty".to_string(),
        ));
    }
    if name.len() > 128 {
        return Err(ApiError::BadRequest(
            "Processor name must not exceed 128 characters".to_string(),
        ));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(ApiError::BadRequest(
            "Processor name may only contain letters, digits, underscores, and hyphens".to_string(),
        ));
    }
    Ok(())
}

/// Validate properties against the processor type's property descriptors.
/// Rejects unknown property keys and invalid allowed values.
/// Required-property checks are deferred to start time (matching NiFi behavior:
/// create → configure → start).
fn validate_properties(
    properties: &std::collections::HashMap<String, String>,
    descriptors: &[runifi_plugin_api::PropertyDescriptor],
) -> Result<(), ApiError> {
    // Build a set of known property names.
    let known_names: std::collections::HashSet<&str> = descriptors.iter().map(|d| d.name).collect();

    // Reject unknown property keys.
    for key in properties.keys() {
        if !known_names.contains(key.as_str()) {
            return Err(ApiError::BadRequest(format!(
                "Unknown property '{}'. Valid properties: {:?}",
                key,
                known_names.iter().collect::<Vec<_>>()
            )));
        }
    }

    // Validate allowed values.
    for (key, value) in properties {
        if let Some(desc) = descriptors.iter().find(|d| d.name == key)
            && let Some(allowed) = desc.allowed_values
            && !allowed.iter().any(|v| *v == value)
        {
            return Err(ApiError::BadRequest(format!(
                "Invalid value '{}' for property '{}'. Allowed: {:?}",
                value, key, allowed
            )));
        }
    }

    Ok(())
}

/// Create a new processor instance at runtime.
async fn create_processor(
    State(state): State<ApiState>,
    Json(body): Json<CreateProcessorRequest>,
) -> Result<impl IntoResponse, ApiError> {
    // Validate the processor name before sending to the engine.
    validate_processor_name(&body.name)?;

    // Look up the processor type to get its property descriptors for validation.
    let plugin_types = &state.handle.plugin_types;
    let type_exists = plugin_types.iter().any(|p| p.type_name == body.type_name);
    if !type_exists {
        return Err(ApiError::BadRequest(format!(
            "Unknown processor type: {}. Check /api/v1/plugins for available types.",
            body.type_name
        )));
    }

    // Get property descriptors from an existing processor of the same type,
    // or validate after creation using the info that comes back.
    let descriptors: Vec<runifi_plugin_api::PropertyDescriptor> = state
        .handle
        .processors
        .read()
        .iter()
        .find(|p| p.type_name == body.type_name)
        .map(|p| {
            p.property_descriptors
                .iter()
                .map(|d| runifi_plugin_api::PropertyDescriptor {
                    name: Box::leak(d.name.clone().into_boxed_str()),
                    description: Box::leak(d.description.clone().into_boxed_str()),
                    required: d.required,
                    default_value: d
                        .default_value
                        .as_ref()
                        .map(|v| &*Box::leak(v.clone().into_boxed_str())),
                    sensitive: d.sensitive,
                    allowed_values: d.allowed_values.as_ref().map(|vals| {
                        let leaked: &'static [&'static str] = Box::leak(
                            vals.iter()
                                .map(|v| &*Box::leak(v.clone().into_boxed_str()))
                                .collect::<Vec<&'static str>>()
                                .into_boxed_slice(),
                        );
                        leaked
                    }),
                    expression_language_supported: d.expression_language_supported,
                })
                .collect()
        })
        .unwrap_or_default();

    // If we found descriptors, validate upfront.
    if !descriptors.is_empty() {
        validate_properties(&body.properties, &descriptors)?;
    }

    state
        .handle
        .add_processor(
            body.name.clone(),
            body.type_name.clone(),
            body.properties.clone(),
            body.scheduling_strategy.clone(),
            body.interval_ms,
        )
        .await
        .map_err(ApiError::from)?;

    // If we didn't have descriptors earlier (first processor of this type),
    // validate now and roll back if invalid.
    if descriptors.is_empty()
        && let Some(info) = state.handle.get_processor_info(&body.name)
        && !info.property_descriptors.is_empty()
    {
        let post_descriptors: Vec<runifi_plugin_api::PropertyDescriptor> = info
            .property_descriptors
            .iter()
            .map(|d| runifi_plugin_api::PropertyDescriptor {
                name: Box::leak(d.name.clone().into_boxed_str()),
                description: Box::leak(d.description.clone().into_boxed_str()),
                required: d.required,
                default_value: d
                    .default_value
                    .as_ref()
                    .map(|v| &*Box::leak(v.clone().into_boxed_str())),
                sensitive: d.sensitive,
                allowed_values: d.allowed_values.as_ref().map(|vals| {
                    let leaked: &'static [&'static str] = Box::leak(
                        vals.iter()
                            .map(|v| &*Box::leak(v.clone().into_boxed_str()))
                            .collect::<Vec<&'static str>>()
                            .into_boxed_slice(),
                    );
                    leaked
                }),
                expression_language_supported: d.expression_language_supported,
            })
            .collect();

        if let Err(e) = validate_properties(&body.properties, &post_descriptors) {
            // Roll back: remove the processor we just created.
            let _ = state.handle.remove_processor(body.name.clone()).await;
            return Err(e);
        }
    }

    // Store canvas position if provided.
    if let Some(pos) = body.position {
        state.handle.set_position(&body.name, pos.x, pos.y);
    }

    // Add to process group if specified.
    if let Some(ref group_id) = body.process_group_id {
        state
            .handle
            .add_processor_to_group(group_id, &body.name)
            .map_err(ApiError::BadRequest)?;
    }

    let info = state
        .handle
        .get_processor_info(&body.name)
        .ok_or_else(|| ApiError::ProcessorNotFound(body.name.clone()))?;

    let snapshot = info.metrics.snapshot();
    let state_str = snapshot.state.as_str().to_string();
    let scheduling_str = info.scheduling_display.clone();

    let relationships: Vec<RelationshipResponse> = info
        .relationships
        .iter()
        .map(|r| RelationshipResponse {
            name: r.name.clone(),
            description: r.description.clone(),
            auto_terminated: r.auto_terminated,
        })
        .collect();

    // Mask sensitive properties in the response.
    let raw_properties = info.properties.read().clone();
    let sensitive_names: std::collections::HashSet<&str> = info
        .property_descriptors
        .iter()
        .filter(|pd| pd.sensitive)
        .map(|pd| pd.name.as_str())
        .collect();
    let properties: std::collections::HashMap<String, String> = raw_properties
        .into_iter()
        .map(|(k, v)| {
            if sensitive_names.contains(k.as_str()) && !v.is_empty() {
                (k, "********".to_string())
            } else {
                (k, v)
            }
        })
        .collect();

    let detail = ProcessorDetailResponse {
        name: info.name.clone(),
        type_name: info.type_name.clone(),
        state: state_str,
        scheduling: scheduling_str,
        position: body.position,
        relationships,
        properties,
    };

    Ok((StatusCode::CREATED, Json(detail)).into_response())
}

async fn delete_processor(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    state
        .handle
        .remove_processor(name.clone())
        .await
        .map_err(ApiError::from)?;

    state.handle.positions.remove(&name);

    Ok(StatusCode::NO_CONTENT)
}

async fn update_position(
    State(state): State<ApiState>,
    Path(name): Path<String>,
    Json(body): Json<UpdatePositionRequest>,
) -> Result<impl IntoResponse, ApiError> {
    if state.handle.get_processor_info(&name).is_none() {
        return Err(ApiError::ProcessorNotFound(name));
    }

    state.handle.set_position(&name, body.x, body.y);

    Ok(Json(serde_json::json!({
        "processor": name,
        "position": { "x": body.x, "y": body.y },
    })))
}

async fn get_processor_config(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> Result<Json<ProcessorConfigResponse>, ApiError> {
    let info = state
        .handle
        .get_processor_info(&name)
        .ok_or(ApiError::ProcessorNotFound(name))?;

    Ok(Json(ProcessorConfigResponse::from_info(&info)))
}

async fn update_processor_config(
    State(state): State<ApiState>,
    Path(name): Path<String>,
    Json(body): Json<ProcessorConfigUpdateRequest>,
) -> Result<impl IntoResponse, ApiError> {
    state.handle.update_processor_config(
        &name,
        body.properties,
        body.penalty_duration_ms,
        body.yield_duration_ms,
        body.bulletin_level,
        body.concurrent_tasks,
        body.auto_terminated_relationships,
        body.comments,
    )?;

    Ok(Json(
        serde_json::json!({ "status": "updated", "processor": name }),
    ))
}

async fn reset_circuit(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    if state.handle.request_circuit_reset(&name) {
        Ok(Json(
            serde_json::json!({ "status": "reset_requested", "processor": name }),
        ))
    } else {
        Err(ApiError::ProcessorNotFound(name))
    }
}

async fn stop_processor(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    if state.handle.stop_processor(&name) {
        Ok(Json(
            serde_json::json!({ "status": "stopped", "processor": name }),
        ))
    } else {
        Err(ApiError::ProcessorNotFound(name))
    }
}

async fn start_processor(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    state.handle.start_processor(&name)?;
    Ok(Json(
        serde_json::json!({ "status": "started", "processor": name }),
    ))
}

async fn pause_processor(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    if state.handle.pause_processor(&name) {
        Ok(Json(
            serde_json::json!({ "status": "paused", "processor": name }),
        ))
    } else {
        Err(ApiError::ProcessorNotFound(name))
    }
}

async fn resume_processor(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    if state.handle.resume_processor(&name) {
        Ok(Json(
            serde_json::json!({ "status": "resumed", "processor": name }),
        ))
    } else {
        Err(ApiError::ProcessorNotFound(name))
    }
}

async fn disable_processor(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    state
        .handle
        .disable_processor(&name)
        .map_err(ApiError::ProcessorNotFound)?;
    Ok(Json(
        serde_json::json!({ "status": "disabled", "processor": name }),
    ))
}

async fn enable_processor(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    state
        .handle
        .enable_processor(&name)
        .map_err(ApiError::ProcessorNotFound)?;
    Ok(Json(
        serde_json::json!({ "status": "enabled", "processor": name }),
    ))
}

async fn get_validation(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let errors = state
        .handle
        .get_validation_errors(&name)
        .map_err(ApiError::ProcessorNotFound)?;
    Ok(Json(serde_json::json!({
        "valid": errors.is_empty(),
        "errors": errors,
    })))
}

// ── Processor State Endpoints ─────────────────────────────────────────────────

/// Query parameters for state endpoints.
#[derive(Debug, Deserialize)]
struct StateQuery {
    /// State scope: "local" (default) or "cluster".
    scope: Option<String>,
}

/// Response DTO for processor state.
#[derive(Debug, Serialize)]
struct ProcessorStateResponse {
    processor: String,
    scope: String,
    version: i64,
    state: HashMap<String, String>,
}

/// Parse the scope query parameter, defaulting to "local".
fn parse_scope(query: &StateQuery) -> Result<&str, ApiError> {
    let scope = query.scope.as_deref().unwrap_or("local");
    match scope {
        "local" | "cluster" => Ok(scope),
        other => Err(ApiError::BadRequest(format!(
            "Invalid scope '{}'. Must be 'local' or 'cluster'.",
            other
        ))),
    }
}

/// GET /api/v1/processors/{name}/state?scope=local
///
/// Returns the current state (key-value map + version) for the processor.
async fn get_processor_state(
    State(state): State<ApiState>,
    Path(name): Path<String>,
    Query(query): Query<StateQuery>,
) -> Result<Json<ProcessorStateResponse>, ApiError> {
    // Verify processor exists.
    let _info = state
        .handle
        .get_processor_info(&name)
        .ok_or(ApiError::ProcessorNotFound(name.clone()))?;

    let scope_str = parse_scope(&query)?;

    let provider =
        state.handle.state_provider.as_ref().ok_or_else(|| {
            ApiError::BadRequest("State management is not configured".to_string())
        })?;

    if scope_str == "cluster" {
        return Err(ApiError::BadRequest(
            "Cluster state scope is not yet implemented".to_string(),
        ));
    }

    let state_map = provider
        .get_state(&name)
        .map_err(|e| ApiError::BadRequest(format!("Failed to read processor state: {}", e)))?;

    Ok(Json(ProcessorStateResponse {
        processor: name,
        scope: scope_str.to_string(),
        version: state_map.version(),
        state: state_map.into_entries(),
    }))
}

/// DELETE /api/v1/processors/{name}/state?scope=local
///
/// Clears all state for the processor in the given scope.
async fn clear_processor_state(
    State(state): State<ApiState>,
    Path(name): Path<String>,
    Query(query): Query<StateQuery>,
) -> Result<impl IntoResponse, ApiError> {
    // Verify processor exists.
    let _info = state
        .handle
        .get_processor_info(&name)
        .ok_or(ApiError::ProcessorNotFound(name.clone()))?;

    let scope_str = parse_scope(&query)?;

    let provider =
        state.handle.state_provider.as_ref().ok_or_else(|| {
            ApiError::BadRequest("State management is not configured".to_string())
        })?;

    if scope_str == "cluster" {
        return Err(ApiError::BadRequest(
            "Cluster state scope is not yet implemented".to_string(),
        ));
    }

    provider
        .clear(&name)
        .map_err(|e| ApiError::BadRequest(format!("Failed to clear processor state: {}", e)))?;

    Ok(Json(serde_json::json!({
        "status": "cleared",
        "processor": name,
        "scope": scope_str,
    })))
}
