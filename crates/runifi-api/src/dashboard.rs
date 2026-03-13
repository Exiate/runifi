use axum::Router;
use axum::extract::Path;
use axum::http::{StatusCode, header};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use include_dir::{Dir, include_dir};

use crate::state::ApiState;

static DASHBOARD_DIR: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/dashboard-dist");

pub fn routes() -> Router<ApiState> {
    Router::new()
        .route("/", get(index))
        // Catch-all for static assets under /assets/
        .route("/assets/{*path}", get(serve_asset))
}

async fn index() -> Response {
    match DASHBOARD_DIR.get_file("index.html") {
        Some(file) => Html(file.contents_utf8().unwrap_or("")).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn serve_asset(Path(path): Path<String>) -> Response {
    let asset_path = format!("assets/{path}");
    serve_static(&asset_path)
}

fn serve_static(path: &str) -> Response {
    let Some(file) = DASHBOARD_DIR.get_file(path) else {
        return StatusCode::NOT_FOUND.into_response();
    };

    let content_type = mime_for_path(path);
    let bytes = file.contents();
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, content_type)],
        bytes,
    )
        .into_response()
}

fn mime_for_path(path: &str) -> &'static str {
    if path.ends_with(".html") {
        "text/html; charset=utf-8"
    } else if path.ends_with(".js") || path.ends_with(".mjs") {
        "application/javascript; charset=utf-8"
    } else if path.ends_with(".css") {
        "text/css; charset=utf-8"
    } else if path.ends_with(".svg") {
        "image/svg+xml"
    } else if path.ends_with(".png") {
        "image/png"
    } else if path.ends_with(".ico") {
        "image/x-icon"
    } else if path.ends_with(".woff2") {
        "font/woff2"
    } else if path.ends_with(".woff") {
        "font/woff"
    } else {
        "application/octet-stream"
    }
}
