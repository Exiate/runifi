[![CI](https://github.com/Exiate/runifi/actions/workflows/ci.yml/badge.svg)](https://github.com/Exiate/runifi/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

# RuniFi

A high-performance data flow engine built in Rust. Routes, transforms, and transfers files between systems with ultra-low latency and high throughput.

## Features

- **Micro transfers** — small files at high throughput (sensor data, logs, events)
- **Standard transfers** — medium files (photos, documents, media)
- **Bulk transfers** — large files (disk images, databases, backups)
- **Visual flow designer** — web dashboard on port 8080 with drag-and-drop canvas
- **Fault tolerance** — per-processor circuit breakers with exponential backoff
- **Plugin system** — extend with custom processors, sources, and sinks
- **Zero-copy I/O** — mmap, sendfile, and io_uring on Linux
- **QUIC transport** — encrypted, multiplexed transfers between nodes

## Architecture

```
runifi-plugin-api    traits + data types (stable contract, no async deps)
       |
runifi-core          engine, scheduler, supervisor, repositories, session
       |
       |--- runifi-processors   built-in: GenerateFlowFile, PutFile, GetFile, ...
       |--- runifi-transport    QUIC transport, zero-copy IO, io_uring
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

Requires Rust 1.85+ and Linux.

```bash
git clone https://github.com/Exiate/runifi.git
cd runifi
cargo build --workspace --release
```

The server binary is at `target/release/runifi` and the CLI at `target/release/runifi-cli`.

## Configuration

Flows are defined in TOML. See [`config/flow.toml`](config/flow.toml) for a complete example.

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

### Run with a config

```bash
cargo run -p runifi -- config/flow.toml
```

## Web Dashboard

RuniFi includes a web dashboard at `http://localhost:8080` for monitoring and managing flows. The canvas view shows processors as nodes with connections between them, along with real-time metrics.

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md) for build instructions, code standards, and how to submit changes.

## License

Licensed under the [Apache License 2.0](LICENSE).

## Security

To report a vulnerability, see [SECURITY.md](SECURITY.md).
