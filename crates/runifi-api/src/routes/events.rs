use std::convert::Infallible;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::routing::get;
use tokio_stream::Stream;
use tokio_stream::StreamExt as _;
use tokio_stream::wrappers::IntervalStream;

use crate::dto::{BulletinResponse, ConnectionResponse, ProcessorResponse, SseMetricsEvent};
use crate::rbac;
use crate::state::ApiState;

pub fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/v1/events", get(sse_events))
        .layer(axum::middleware::from_fn(rbac::require_view_flow))
}

async fn sse_events(
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, (StatusCode, axum::Json<serde_json::Value>)> {
    let current = state.sse_connections.fetch_add(1, Ordering::Relaxed);
    if current >= state.max_sse_connections {
        // Undo the increment — we're not actually opening a connection.
        state.sse_connections.fetch_sub(1, Ordering::Relaxed);
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            axum::Json(serde_json::json!({
                "error": "Too many SSE connections"
            })),
        ));
    }

    let sse_counter = state.sse_connections.clone();

    let interval = tokio::time::interval(Duration::from_secs(1));
    let stream = IntervalStream::new(interval).map(move |_| {
        let handle = &state.handle;

        let processors: Vec<ProcessorResponse> = handle
            .processors
            .read()
            .iter()
            .map(ProcessorResponse::from_info)
            .collect();

        let connections: Vec<ConnectionResponse> = handle
            .connections
            .read()
            .iter()
            .map(|info| {
                // Extract load balance display fields.
                let (lb_strategy, lb_partition_attr, lb_compression) = {
                    match info.connection.load_balance_config() {
                        Some(config) => {
                            use runifi_core::cluster::load_balance::LoadBalanceStrategy;
                            let strategy_str = match &config.strategy {
                                LoadBalanceStrategy::DoNotLoadBalance => None,
                                LoadBalanceStrategy::RoundRobin => Some("round_robin".to_string()),
                                LoadBalanceStrategy::PartitionByAttribute { attribute } => {
                                    return ConnectionResponse {
                                        id: info.id.clone(),
                                        source_name: info.source_name.clone(),
                                        relationship: info.relationship.clone(),
                                        dest_name: info.dest_name.clone(),
                                        queued_count: info.connection.queue_count(),
                                        queued_bytes: info.connection.queue_size_bytes(),
                                        back_pressured: info.connection.is_back_pressured(),
                                        load_balance_strategy: Some(
                                            "partition_by_attribute".to_string(),
                                        ),
                                        load_balance_partition_attribute: Some(attribute.clone()),
                                        load_balance_compression: Some(config.compression),
                                    };
                                }
                                LoadBalanceStrategy::SingleNode { .. } => {
                                    Some("single_node".to_string())
                                }
                            };
                            let compression = if config.compression { Some(true) } else { None };
                            (strategy_str, None, compression)
                        }
                        None => (None, None, None),
                    }
                };
                ConnectionResponse {
                    id: info.id.clone(),
                    source_name: info.source_name.clone(),
                    relationship: info.relationship.clone(),
                    dest_name: info.dest_name.clone(),
                    queued_count: info.connection.queue_count(),
                    queued_bytes: info.connection.queue_size_bytes(),
                    back_pressured: info.connection.is_back_pressured(),
                    load_balance_strategy: lb_strategy,
                    load_balance_partition_attribute: lb_partition_attr,
                    load_balance_compression: lb_compression,
                }
            })
            .collect();

        let bulletins: Vec<BulletinResponse> = handle
            .bulletin_board
            .latest_per_processor()
            .into_values()
            .map(BulletinResponse::from)
            .collect();

        let event = SseMetricsEvent {
            uptime_secs: handle.started_at.elapsed().as_secs(),
            processors,
            connections,
            bulletins,
        };

        let data = serde_json::to_string(&event).unwrap_or_default();
        Ok::<_, Infallible>(Event::default().event("metrics").data(data))
    });

    // Wrap the stream so we decrement the counter when the client disconnects.
    let guarded_stream = SseGuardedStream {
        inner: Box::pin(stream),
        counter: sse_counter,
        decremented: false,
    };

    Ok(Sse::new(guarded_stream).keep_alive(KeepAlive::default()))
}

/// A stream wrapper that decrements the SSE connection counter on drop.
struct SseGuardedStream<S> {
    inner: std::pin::Pin<Box<S>>,
    counter: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    decremented: bool,
}

impl<S: Stream + Unpin> Stream for SseGuardedStream<S> {
    type Item = S::Item;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let result = self.inner.as_mut().poll_next(cx);
        if let std::task::Poll::Ready(None) = &result
            && !self.decremented
        {
            self.counter.fetch_sub(1, Ordering::Relaxed);
            self.decremented = true;
        }
        result
    }
}

impl<S> Drop for SseGuardedStream<S> {
    fn drop(&mut self) {
        if !self.decremented {
            self.counter.fetch_sub(1, Ordering::Relaxed);
            self.decremented = true;
        }
    }
}
