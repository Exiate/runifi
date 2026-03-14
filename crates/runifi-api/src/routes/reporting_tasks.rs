use std::collections::HashMap;
use std::sync::atomic::Ordering;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{delete as delete_method, get, post, put};
use axum::{Json, Router, middleware};

use runifi_core::engine::handle::PluginKind;
use runifi_core::engine::reporting_task_manager::{ReportingTaskInfo, ReportingTaskSchedule};

use crate::dto::{
    CreateReportingTaskRequest, ReportingTaskMetricsResponse,
    ReportingTaskPropertyDescriptorResponse, ReportingTaskResponse,
    UpdateReportingTaskConfigRequest,
};
use crate::error::ApiError;
use crate::rbac;
use crate::state::ApiState;

pub fn routes() -> Router<ApiState> {
    // GET endpoints — ViewFlow (Viewer+)
    let view_routes = Router::new()
        .route("/api/v1/reporting-tasks", get(list_reporting_tasks))
        .route("/api/v1/reporting-tasks/{name}", get(get_reporting_task))
        .layer(middleware::from_fn(rbac::require_view_flow));

    // Lifecycle endpoints — OperateProcessors (Operator+)
    let operate_routes = Router::new()
        .route(
            "/api/v1/reporting-tasks/{name}/start",
            post(start_reporting_task),
        )
        .route(
            "/api/v1/reporting-tasks/{name}/stop",
            post(stop_reporting_task),
        )
        .layer(middleware::from_fn(rbac::require_operate_processors));

    // Mutation endpoints — ModifyFlow (Admin only)
    let modify_routes = Router::new()
        .route("/api/v1/reporting-tasks", post(create_reporting_task))
        .route(
            "/api/v1/reporting-tasks/{name}",
            delete_method(delete_reporting_task),
        )
        .layer(middleware::from_fn(rbac::require_modify_flow));

    // Config update endpoint — ManageConfig (Admin only)
    let config_routes = Router::new()
        .route(
            "/api/v1/reporting-tasks/{name}/config",
            put(update_reporting_task_config),
        )
        .layer(middleware::from_fn(rbac::require_manage_config));

    view_routes
        .merge(operate_routes)
        .merge(modify_routes)
        .merge(config_routes)
}

fn task_to_response(info: &ReportingTaskInfo) -> ReportingTaskResponse {
    let sensitive_names: std::collections::HashSet<&str> = info
        .property_descriptors
        .iter()
        .filter(|pd| pd.sensitive)
        .map(|pd| pd.name.as_str())
        .collect();

    let properties: HashMap<String, String> = info
        .properties
        .iter()
        .map(|(k, v)| {
            if sensitive_names.contains(k.as_str()) && !v.is_empty() {
                (k.clone(), "********".to_string())
            } else {
                (k.clone(), v.clone())
            }
        })
        .collect();

    ReportingTaskResponse {
        name: info.name.clone(),
        type_name: info.type_name.clone(),
        state: info.state.as_str().to_string(),
        scheduling: info.scheduling_display.clone(),
        properties,
        property_descriptors: info
            .property_descriptors
            .iter()
            .map(|pd| ReportingTaskPropertyDescriptorResponse {
                name: pd.name.clone(),
                description: pd.description.clone(),
                required: pd.required,
                default_value: pd.default_value.clone(),
                sensitive: pd.sensitive,
            })
            .collect(),
        metrics: ReportingTaskMetricsResponse {
            total_invocations: info.metrics.total_invocations.load(Ordering::Relaxed),
            total_failures: info.metrics.total_failures.load(Ordering::Relaxed),
            consecutive_failures: info.metrics.consecutive_failures.load(Ordering::Relaxed),
            circuit_open: info.metrics.circuit_open.load(Ordering::Relaxed),
        },
    }
}

async fn list_reporting_tasks(State(state): State<ApiState>) -> Json<Vec<ReportingTaskResponse>> {
    let tasks = state.handle.list_reporting_tasks();
    let responses: Vec<ReportingTaskResponse> = tasks.iter().map(task_to_response).collect();
    Json(responses)
}

async fn get_reporting_task(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> Result<Json<ReportingTaskResponse>, ApiError> {
    let info = state
        .handle
        .get_reporting_task(&name)
        .ok_or(ApiError::ReportingTaskNotFound(name))?;
    Ok(Json(task_to_response(&info)))
}

fn validate_task_name(name: &str) -> Result<(), ApiError> {
    if name.is_empty() {
        return Err(ApiError::BadRequest(
            "Reporting task name must not be empty".to_string(),
        ));
    }
    if name.len() > 128 {
        return Err(ApiError::BadRequest(
            "Reporting task name must not exceed 128 characters".to_string(),
        ));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(ApiError::BadRequest(
            "Reporting task name may only contain letters, digits, underscores, and hyphens"
                .to_string(),
        ));
    }
    Ok(())
}

async fn create_reporting_task(
    State(state): State<ApiState>,
    Json(body): Json<CreateReportingTaskRequest>,
) -> Result<impl IntoResponse, ApiError> {
    validate_task_name(&body.name)?;

    // Verify the type exists.
    let type_exists = state
        .handle
        .plugin_types
        .iter()
        .any(|p| p.type_name == body.type_name && matches!(p.kind, PluginKind::ReportingTask));
    if !type_exists {
        return Err(ApiError::BadRequest(format!(
            "Unknown reporting task type: {}. Check /api/v1/plugins for available types.",
            body.type_name
        )));
    }

    // Create the task instance via the plugin registry.
    let task = state
        .create_reporting_task(&body.type_name)
        .ok_or_else(|| {
            ApiError::BadRequest(format!(
                "Failed to create reporting task of type: {}",
                body.type_name
            ))
        })?;

    // Parse scheduling.
    let schedule = match body.scheduling_strategy.as_str() {
        "timer" => ReportingTaskSchedule::Timer {
            interval_ms: body.interval_ms,
        },
        "cron" => {
            return Err(ApiError::BadRequest(
                "Cron scheduling is not yet supported. Use 'timer' strategy with interval_ms."
                    .to_string(),
            ));
        }
        other => {
            return Err(ApiError::BadRequest(format!(
                "Invalid scheduling strategy '{}'; must be 'timer'",
                other
            )));
        }
    };

    // Add to the manager.
    let mgr = state
        .handle
        .reporting_task_manager
        .as_ref()
        .ok_or(ApiError::EngineNotRunning)?;
    mgr.write().add_task(
        body.name.clone(),
        body.type_name.clone(),
        task,
        schedule,
        body.properties,
    )?;

    let info = mgr
        .read()
        .get_task(&body.name)
        .ok_or_else(|| ApiError::ReportingTaskNotFound(body.name.clone()))?;

    Ok((StatusCode::CREATED, Json(task_to_response(&info))).into_response())
}

async fn delete_reporting_task(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let mgr = state
        .handle
        .reporting_task_manager
        .as_ref()
        .ok_or(ApiError::EngineNotRunning)?;
    mgr.write().remove_task(&name)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn update_reporting_task_config(
    State(state): State<ApiState>,
    Path(name): Path<String>,
    Json(body): Json<UpdateReportingTaskConfigRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let mgr = state
        .handle
        .reporting_task_manager
        .as_ref()
        .ok_or(ApiError::EngineNotRunning)?;
    mgr.write().configure_task(&name, body.properties)?;

    Ok(Json(
        serde_json::json!({ "status": "configured", "reporting_task": name }),
    ))
}

async fn start_reporting_task(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let mgr = state
        .handle
        .reporting_task_manager
        .as_ref()
        .ok_or(ApiError::EngineNotRunning)?;
    mgr.write().start_task(
        &name,
        state.handle.processors.clone(),
        state.handle.connections.clone(),
    )?;

    Ok(Json(
        serde_json::json!({ "status": "started", "reporting_task": name }),
    ))
}

async fn stop_reporting_task(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let mgr = state
        .handle
        .reporting_task_manager
        .as_ref()
        .ok_or(ApiError::EngineNotRunning)?;

    // Extract cancel token and task handle without holding the lock across await.
    let (cancel_token, task_handle) = {
        let mut guard = mgr.write();
        guard.prepare_stop(&name)?
    };

    // Cancel and await outside the lock.
    if let Some(token) = cancel_token {
        token.cancel();
    }
    let recovered_task = if let Some(handle) = task_handle {
        match handle.await {
            Ok(task) => Some(task),
            Err(e) => {
                tracing::error!(task = %name, error = %e, "Reporting task join error");
                None
            }
        }
    } else {
        None
    };

    // Re-acquire lock to finalize state.
    mgr.write().finalize_stop(&name, recovered_task)?;

    Ok(Json(
        serde_json::json!({ "status": "stopped", "reporting_task": name }),
    ))
}
