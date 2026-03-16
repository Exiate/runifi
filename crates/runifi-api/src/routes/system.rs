use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router, middleware};
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

use crate::dto::{
    ConnectionsSummary, ContentRepoInfo, CpuInfo, FlowFileRepoInfo, MemoryInfo, ProvenanceRepoInfo,
    RepositoryStorageInfo, RuntimeInfo, SystemResponse,
};
use crate::rbac;
use crate::state::ApiState;

pub fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/v1/system", get(get_system))
        .layer(middleware::from_fn(rbac::require_view_flow))
}

async fn get_system(State(state): State<ApiState>) -> Json<SystemResponse> {
    let handle = &state.handle;

    // System metrics via sysinfo
    let mut sys = System::new();
    sys.refresh_memory();
    let pid = Pid::from_u32(std::process::id());
    sys.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        true,
        ProcessRefreshKind::nothing().with_cpu().with_memory(),
    );

    let (resident_bytes, virtual_bytes, process_cpu) = sys
        .process(pid)
        .map(|p| (p.memory(), p.virtual_memory(), p.cpu_usage()))
        .unwrap_or((0, 0, 0.0));

    // Connection summary
    let connections = handle.connections.read();
    let mut total_queued_ff = 0usize;
    let mut total_queued_bytes = 0u64;
    let mut bp_count = 0usize;
    for conn in connections.iter() {
        total_queued_ff += conn.connection.queue_count();
        total_queued_bytes += conn.connection.queue_size_bytes();
        if conn.connection.is_back_pressured() {
            bp_count += 1;
        }
    }
    let connection_count = connections.len();
    drop(connections);

    // Processor aggregate metrics
    let processors = handle.processors.read();
    let processor_count = processors.len();
    let mut total_ff_processed = 0u64;
    let mut total_bytes_processed = 0u64;
    for p in processors.iter() {
        let snap = p.metrics.snapshot();
        total_ff_processed += snap.flowfiles_out;
        total_bytes_processed += snap.bytes_out;
    }
    drop(processors);

    // Repository stats
    let content_entry_count = handle.content_repo.entry_count();
    let content_total_bytes = handle.content_repo.total_bytes();
    let content_storage_type = handle.content_repo.storage_type().to_string();

    let ff_stats = handle.flowfile_repo.stats();

    let prov_stats = handle.provenance_repo.stats();

    Json(SystemResponse {
        flow_name: handle.flow_name.clone(),
        uptime_secs: handle.started_at.elapsed().as_secs(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        processor_count,
        connection_count,
        memory: MemoryInfo {
            resident_bytes,
            virtual_bytes,
            total_system_bytes: sys.total_memory(),
            used_system_bytes: sys.used_memory(),
        },
        cpu: CpuInfo {
            process_cpu_percent: process_cpu,
            available_cores: num_cpus_available(),
        },
        repositories: RepositoryStorageInfo {
            content: ContentRepoInfo {
                entry_count: content_entry_count,
                total_bytes: content_total_bytes,
                storage_type: content_storage_type,
            },
            flowfile: FlowFileRepoInfo {
                entry_count: ff_stats.entry_count,
                storage_bytes: ff_stats.storage_bytes,
                storage_type: ff_stats.storage_type.to_string(),
            },
            provenance: ProvenanceRepoInfo {
                event_count: prov_stats.event_count,
            },
        },
        runtime: RuntimeInfo {
            total_flowfiles_processed: total_ff_processed,
            total_bytes_processed,
        },
        connections_summary: ConnectionsSummary {
            total_queued_flowfiles: total_queued_ff,
            total_queued_bytes,
            back_pressured_count: bp_count,
        },
    })
}

fn num_cpus_available() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}
