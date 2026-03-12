use std::convert::Infallible;
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::routing::get;
use tokio_stream::Stream;
use tokio_stream::StreamExt as _;
use tokio_stream::wrappers::IntervalStream;

use crate::dto::{ConnectionResponse, ProcessorResponse, SseMetricsEvent};
use crate::state::ApiState;

pub fn routes() -> Router<ApiState> {
    Router::new().route("/api/v1/events", get(sse_events))
}

async fn sse_events(
    State(state): State<ApiState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let interval = tokio::time::interval(Duration::from_secs(1));
    let stream = IntervalStream::new(interval).map(move |_| {
        let handle = &state.handle;

        let processors: Vec<ProcessorResponse> = handle
            .processors
            .iter()
            .map(ProcessorResponse::from_info)
            .collect();

        let connections: Vec<ConnectionResponse> = handle
            .connections
            .iter()
            .map(|info| ConnectionResponse {
                id: info.id.clone(),
                source_name: info.source_name.clone(),
                relationship: info.relationship.clone(),
                dest_name: info.dest_name.clone(),
                queued_count: info.connection.count(),
                queued_bytes: info.connection.bytes(),
                back_pressured: info.connection.is_back_pressured(),
            })
            .collect();

        let event = SseMetricsEvent {
            uptime_secs: handle.started_at.elapsed().as_secs(),
            processors,
            connections,
        };

        let data = serde_json::to_string(&event).unwrap_or_default();
        Ok(Event::default().event("metrics").data(data))
    });

    Sse::new(stream).keep_alive(KeepAlive::default())
}
