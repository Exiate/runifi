[![CI](https://github.com/Exiate/runifi/actions/workflows/ci.yml/badge.svg)](https://github.com/Exiate/runifi/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

# RuniFi

A high-performance data flow engine built in Rust. Routes, transforms, and transfers files between systems with ultra-low latency and high throughput.

## Features

- **Micro transfers** — small files at high throughput (sensor data, logs, events)
- **Standard transfers** — medium files (photos, documents, media)
- **Bulk transfers** — large files (disk images, databases, backups)
- **Visual flow designer** — web dashboard on port 8080 with drag-and-drop canvas, process group navigation, and real-time metrics
- **Fault tolerance** — per-processor circuit breakers with exponential backoff
- **Plugin system** — 20+ built-in processors, extend with custom processors, sources, and sinks
- **Zero-copy I/O** — mmap, sendfile, and io_uring on Linux
- **QUIC transport** — encrypted, multiplexed transfers between nodes
- **Multi-node clustering** — dynamic membership, leader election, gossip protocol, and node health monitoring
- **Enterprise auth** — OIDC, LDAP, mTLS, API keys, and local providers with RBAC
- **Data provenance** — persistent lineage tracking with indexed search and replay
- **Expression language** — NiFi-compatible `${...}` syntax for dynamic property values
- **Flow versioning** — git-backed version control with diff support
- **Encrypted repositories** — AES-GCM encryption for content and WAL
- **Record-oriented processing** — CSV/JSON readers and writers with schema registry
- **Audit logging** — structured audit trail for all system operations
- **Reporting tasks** — Prometheus metrics export, log reporting, bulletin forwarding

## Architecture

```
runifi-plugin-api    traits + data types (stable contract, no async deps)
       |
runifi-core          engine, scheduler, supervisor, repositories, session,
       |             auth, clustering, expression language, provenance
       |
       |--- runifi-processors   built-in (20+): GenerateFlowFile, PutFile, GetFile, ...
       |--- runifi-transport    QUIC transport, zero-copy IO, io_uring
       |--- runifi-api          REST API, SSE events, embedded React dashboard
       |
runifi-server        binary: config loading, engine startup
runifi-cli           binary: management CLI
```

## Install

### From release packages

```bash
# RPM (RHEL, Fedora, Rocky)
sudo rpm -i runifi-<version>.x86_64.rpm

# Debian/Ubuntu
sudo dpkg -i runifi_<version>_amd64.deb

# Tarball
tar xzf runifi-<version>-linux-x86_64.tar.gz
sudo mv runifi /usr/local/bin/
```

### Enable and start the service

```bash
sudo systemctl enable --now runifi
```

### Build from source

Requires Rust 1.94+ and Linux.

```bash
git clone https://github.com/Exiate/runifi.git
cd runifi
cargo build --workspace --release
```

The server binary is at `target/release/runifi` and the CLI at `target/release/runifi-cli`.

## Configuration

Flows are defined in TOML. See [`config/examples/demo-pipeline.toml`](config/examples/demo-pipeline.toml) for a complete example.

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

### Run (blank canvas)

```bash
cargo run -p runifi
```

### Run with the demo pipeline

```bash
cargo run -p runifi -- config/examples/demo-pipeline.toml
```

## Performance

RuniFi's engine is designed for high throughput with minimal per-FlowFile overhead. Benchmark results on a single thread:

| Operation | Throughput |
|---|---|
| FlowFile creation | 250M/sec |
| ID generation (u64 atomic) | 606M/sec |
| Connection send+recv cycle | 20.8M/sec |
| Full pipeline step (create + write 5KB + read + transfer + commit) | 1.56M/sec |
| Pipeline with connection (end-to-end 5KB) | 133K/sec |
| Batch create (100 FlowFiles) | 5.28M FF/sec |
| Content read 5KB (zero-copy) | 34.8M/sec |

Run benchmarks yourself:

```bash
cargo bench --package runifi-core
```

## Web Dashboard

RuniFi includes a web dashboard at `http://localhost:8080` for monitoring and managing flows. The canvas view shows processors as nodes with connections between them, along with real-time metrics.

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md) for build instructions, code standards, and how to submit changes.

## License

Licensed under the [Apache License 2.0](LICENSE).

## Security

To report a vulnerability, see [SECURITY.md](SECURITY.md).
