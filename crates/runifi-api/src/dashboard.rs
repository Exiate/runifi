use axum::Router;
use axum::http::{StatusCode, header};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use include_dir::{Dir, include_dir};

use crate::state::ApiState;

static DASHBOARD_DIR: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/dashboard");

pub fn routes() -> Router<ApiState> {
    Router::new()
        .route("/", get(index))
        .route("/style.css", get(style_css))
        .route("/app.js", get(app_js))
}

async fn index() -> Html<&'static str> {
    let file = DASHBOARD_DIR
        .get_file("index.html")
        .expect("index.html must exist in dashboard/");
    Html(file.contents_utf8().unwrap_or(""))
}

async fn style_css() -> Response {
    serve_static("style.css", "text/css")
}

async fn app_js() -> Response {
    serve_static("app.js", "application/javascript")
}

fn serve_static(path: &str, content_type: &str) -> Response {
    match DASHBOARD_DIR.get_file(path) {
        Some(file) => {
            let body = file.contents_utf8().unwrap_or("");
            (StatusCode::OK, [(header::CONTENT_TYPE, content_type)], body).into_response()
        }
        None => StatusCode::NOT_FOUND.into_response(),
    }
}
