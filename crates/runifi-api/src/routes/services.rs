use std::collections::HashMap;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{delete as delete_method, get, post, put};
use axum::{Json, Router, middleware};

use crate::dto::{
    CreateServiceRequest, ServicePropertyDescriptorResponse, ServiceResponse,
    UpdateServiceConfigRequest,
};
use crate::error::ApiError;
use crate::rbac;
use crate::state::ApiState;

pub fn routes() -> Router<ApiState> {
    // GET endpoints — ViewFlow (Viewer+)
    let view_routes = Router::new()
        .route("/api/v1/services", get(list_services))
        .route("/api/v1/services/{name}", get(get_service))
        .layer(middleware::from_fn(rbac::require_view_flow));

    // Lifecycle endpoints — OperateProcessors (Operator+)
    let operate_routes = Router::new()
        .route("/api/v1/services/{name}/enable", post(enable_service))
        .route("/api/v1/services/{name}/disable", post(disable_service))
        .layer(middleware::from_fn(rbac::require_operate_processors));

    // Mutation endpoints — ModifyFlow (Admin only)
    let modify_routes = Router::new()
        .route("/api/v1/services", post(create_service))
        .route("/api/v1/services/{name}", delete_method(delete_service))
        .layer(middleware::from_fn(rbac::require_modify_flow));

    // Config update endpoint — ManageConfig (Admin only)
    let config_routes = Router::new()
        .route("/api/v1/services/{name}/config", put(update_service_config))
        .layer(middleware::from_fn(rbac::require_manage_config));

    view_routes
        .merge(operate_routes)
        .merge(modify_routes)
        .merge(config_routes)
}

fn service_to_response(
    info: &runifi_core::registry::service_registry::ServiceInfo,
) -> ServiceResponse {
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

    ServiceResponse {
        name: info.name.clone(),
        type_name: info.type_name.clone(),
        state: info.state.as_str().to_string(),
        properties,
        property_descriptors: info
            .property_descriptors
            .iter()
            .map(|pd| ServicePropertyDescriptorResponse {
                name: pd.name.clone(),
                description: pd.description.clone(),
                required: pd.required,
                default_value: pd.default_value.clone(),
                sensitive: pd.sensitive,
            })
            .collect(),
        referencing_processors: info.referencing_processors.clone(),
    }
}

async fn list_services(State(state): State<ApiState>) -> Json<Vec<ServiceResponse>> {
    let services = state.handle.list_services();
    let responses: Vec<ServiceResponse> = services.iter().map(service_to_response).collect();
    Json(responses)
}

async fn get_service(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> Result<Json<ServiceResponse>, ApiError> {
    let info = state
        .handle
        .get_service_info(&name)
        .ok_or(ApiError::ServiceNotFound(name))?;
    Ok(Json(service_to_response(&info)))
}

fn validate_service_name(name: &str) -> Result<(), ApiError> {
    if name.is_empty() {
        return Err(ApiError::BadRequest(
            "Service name must not be empty".to_string(),
        ));
    }
    if name.len() > 128 {
        return Err(ApiError::BadRequest(
            "Service name must not exceed 128 characters".to_string(),
        ));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(ApiError::BadRequest(
            "Service name may only contain letters, digits, underscores, and hyphens".to_string(),
        ));
    }
    Ok(())
}

async fn create_service(
    State(state): State<ApiState>,
    Json(body): Json<CreateServiceRequest>,
) -> Result<impl IntoResponse, ApiError> {
    validate_service_name(&body.name)?;

    // Check if the service type exists in the plugin registry.
    let type_exists = state.handle.plugin_types.iter().any(|p| {
        p.type_name == body.type_name
            && matches!(p.kind, runifi_core::engine::handle::PluginKind::Service)
    });
    if !type_exists {
        return Err(ApiError::BadRequest(format!(
            "Unknown service type: {}. Check /api/v1/plugins for available types.",
            body.type_name
        )));
    }

    // Create the service instance via the registry in the engine handle.
    // We need to get the factory from plugin_types — but the handle's
    // add_service takes a Box<dyn ControllerService>. The factory is in the
    // PluginRegistry, not the PluginTypeInfo. So we need to create the service
    // instance first.
    //
    // The engine handle doesn't have direct access to the PluginRegistry,
    // so we create via the registry stored in ApiState.
    //
    // For now, we pass a pre-created instance through a registry lookup.
    // The ApiState would need access to the registry. Since we can't easily
    // add it now, we'll use the service type info to verify the type exists
    // and then create through the handle. But the handle's add_service
    // expects a pre-created Box<dyn ControllerService>.
    //
    // Solution: look up from the plugin_types field. Since we can't create
    // from PluginTypeInfo alone, we check the handle's service_registry.
    // Actually, we need to pass the PluginRegistry to ApiState or use
    // a different path. Let's use the PluginRegistry stored in the engine.
    //
    // The simplest approach: add a create_service method to EngineHandle
    // that takes type_name and uses the registry internally. But the engine
    // doesn't store the registry in the handle...
    //
    // For this initial implementation, we'll create the service via the
    // registry stored on ApiState (which we'll add).
    let service = state
        .create_controller_service(&body.type_name)
        .ok_or_else(|| {
            ApiError::BadRequest(format!(
                "Failed to create service of type: {}",
                body.type_name
            ))
        })?;

    state
        .handle
        .add_service(body.name.clone(), body.type_name.clone(), service)?;

    // Configure with properties if provided.
    if !body.properties.is_empty() {
        state
            .handle
            .configure_service(&body.name, body.properties)?;
    }

    let info = state
        .handle
        .get_service_info(&body.name)
        .ok_or_else(|| ApiError::ServiceNotFound(body.name.clone()))?;

    Ok((StatusCode::CREATED, Json(service_to_response(&info))).into_response())
}

async fn delete_service(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    state.handle.remove_service(&name)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn update_service_config(
    State(state): State<ApiState>,
    Path(name): Path<String>,
    Json(body): Json<UpdateServiceConfigRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    state.handle.configure_service(&name, body.properties)?;

    Ok(Json(
        serde_json::json!({ "status": "configured", "service": name }),
    ))
}

async fn enable_service(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    state.handle.enable_service(&name)?;

    Ok(Json(
        serde_json::json!({ "status": "enabled", "service": name }),
    ))
}

async fn disable_service(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    state.handle.disable_service(&name)?;

    Ok(Json(
        serde_json::json!({ "status": "disabled", "service": name }),
    ))
}
