# RuniFi — High-Performance Data Flow Engine

A Rust reimplementation of Apache NiFi, purpose-built for ultra-low-latency and high-throughput file transfers.

## Project Overview

RuniFi is a data flow engine that routes, transforms, and transfers files between systems. It targets three transfer profiles:
- **Micro**: 5KB files at 5000+/sec (sensor data, logs, events)
- **Standard**: 30MB files (photos, documents, media)
- **Bulk**: Multi-GB files (disk images, databases, backups)

Target platform: Linux (tested between two VMs).

## Architecture

```
runifi-plugin-api    (traits + data types — zero async deps, stable contract)
       ^
       |
runifi-core          (engine, scheduler, supervisor, repositories, session)
       ^
       |--- runifi-processors   (built-in: GenerateFlowFile, PutFile, GetFile, etc.)
       |--- runifi-transport    (QUIC transport, future phase)
       ^
       |
runifi-server        (binary: config loading, engine startup)
runifi-cli           (binary: management CLI)
```

Cargo workspace with six crates:

| Crate | Purpose |
|---|---|
| `runifi-plugin-api` | Stable plugin contract: Processor/Source/Sink traits, FlowFile, PropertyDescriptor, ProcessSession |
| `runifi-core` | Engine runtime: FlowEngine, ProcessorNode, Supervisor, ContentRepository, connections, registry |
| `runifi-processors` | Built-in processors: GenerateFlowFile, LogAttribute, GetFile, PutFile, RouteOnAttribute, UpdateAttribute |
| `runifi-transport` | QUIC/TCP transport, zero-copy IO, io_uring |
| `runifi-server` | Runtime binary, config loading, orchestration |
| `runifi-cli` | CLI tools for flow management and diagnostics |

### Key Concepts (from NiFi, adapted for Rust)
- **FlowFile**: Atomic data unit = attributes (`Vec<(Arc<str>, Arc<str>)>`) + content claim (reference to ContentRepository)
- **Processor**: Synchronous trait (`fn on_trigger`) — engine wraps in `spawn_blocking` + `catch_unwind` for fault isolation
- **Connection**: `crossbeam::channel::bounded` + atomic byte tracking + `tokio::sync::Notify` for back-pressure
- **Flow Engine**: DAG scheduler that wires processors and connections, one tokio task per processor
- **ProcessSession**: Transactional session — buffered until `commit()`, reverted on `rollback()`
- **ContentRepository**: Ref-counted content storage (in-memory via DashMap, zero-copy via Bytes::slice)
- **PluginRegistry**: Compile-time plugin discovery via `inventory` crate

### Plugin System
- Plugins are **synchronous** (no tokio dependency in plugin-api) — engine manages async
- Registration via `inventory::submit!(ProcessorDescriptor { ... })`
- Feature-gated in `runifi-processors`: `filesystem`, `routing`, `debug`
- Future: dynamic `.so` / WASM plugins (no async ABI issues)

### Fault Tolerance
- `ProcessorSupervisor` wraps every `on_trigger` in `catch_unwind`
- Exponential backoff on failure (100ms base, 30s cap)
- Circuit breaker trips after 5 consecutive failures → auto-disable

### Transfer Strategy (size-based)
- `<= 64KB` → InlineStream (batched in QUIC stream)
- `<= 100MB` → ChunkedStream (chunked streaming)
- `> 100MB` → ZeroCopy (mmap + sendfile/io_uring)

## Code Standards

### Rust Conventions
- Edition 2024, MSRV 1.85
- Use `thiserror` for library errors, `anyhow` for binary error handling
- Prefer `tokio::sync` primitives over `std::sync` in async contexts
- Use `tracing` (not `log`) for all instrumentation
- Zero-copy where possible: `bytes::Bytes`, `memmap2`, avoid unnecessary cloning
- All public APIs must be `Send + Sync`

### Performance Rules
- No allocations in hot paths — preallocate buffers
- Use `DashMap` over `Mutex<HashMap>` for concurrent maps
- Prefer channel-based communication over shared mutable state
- Benchmark before and after optimizations with `criterion`
- FlowFile IDs are `u64` (monotonic), not UUID — cheaper at 5000/sec
- Attributes are `Vec<(Arc<str>, Arc<str>)>` — cache-friendly for <16 attributes

### Testing
- Unit tests in `#[cfg(test)]` modules within each file
- Integration tests in `tests/` directory
- Benchmarks in `benches/` using `criterion`
- Run tests: `cargo test --workspace`
- Run clippy: `cargo clippy --workspace -- -D warnings`

### Git
- Branch from `main`
- Conventional commits: `feat:`, `fix:`, `perf:`, `refactor:`, `test:`, `docs:`
- Never commit `target/`, `.env`, or generated certs

## Building & Running

```bash
# Build all crates
cargo build --workspace

# Build release (optimized)
cargo build --workspace --release

# Run the server (with config)
cargo run -p runifi -- config/flow.toml

# Run CLI
cargo run -p runifi-cli

# Tests
cargo test --workspace

# Lint
cargo clippy --workspace -- -D warnings
cargo fmt --all -- --check
```

## Configuration

Flow definitions are in TOML format. See `config/flow.toml` for an example:

```toml
[flow]
name = "example-flow"

[[flow.processors]]
name = "generate-test-data"
type = "GenerateFlowFile"
[flow.processors.scheduling]
strategy = "timer"
interval_ms = 1000
[flow.processors.properties]
"File Size" = "5120"

[[flow.connections]]
source = "generate-test-data"
relationship = "success"
destination = "log-attributes"
```

## File Size Thresholds
These constants live in `crates/runifi-transport/src/transfer.rs`:
- `SMALL_FILE_THRESHOLD = 64KB`
- `LARGE_FILE_THRESHOLD = 100MB`
