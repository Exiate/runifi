# Contributing to RuniFi

Thanks for your interest in contributing to RuniFi! This guide covers everything you need to get started.

## Prerequisites

- **Rust 1.85+** — install via [rustup](https://rustup.rs/)
- **Linux** — RuniFi targets Linux exclusively
- **Git** — for version control

## Getting Started

```bash
# Clone the repo
git clone https://github.com/Exiate/runifi.git
cd runifi

# Build all crates
cargo build --workspace

# Run tests
cargo test --workspace

# Run lints
cargo clippy --workspace -- -D warnings
cargo fmt --all -- --check
```

## Code Standards

### Rust Conventions

- Use `thiserror` for library errors, `anyhow` for binary error handling
- Use `tracing` (not `log`) for all instrumentation
- Prefer `tokio::sync` primitives over `std::sync` in async contexts
- Zero-copy where possible: `bytes::Bytes`, `memmap2`, avoid unnecessary cloning
- All public APIs must be `Send + Sync`

### Performance

- No allocations in hot paths — preallocate buffers
- Use `DashMap` over `Mutex<HashMap>` for concurrent maps
- Prefer channel-based communication over shared mutable state
- Benchmark before and after optimizations with `criterion`

### Commits

We use [Conventional Commits](https://www.conventionalcommits.org/):

- `feat:` — new features
- `fix:` — bug fixes
- `perf:` — performance improvements
- `refactor:` — code restructuring
- `test:` — adding or updating tests
- `docs:` — documentation changes

### Branching

- Branch from `main`
- Keep branches focused on a single change
- Never commit `target/`, `.env`, or generated certs

## Pull Requests

1. Fork the repo and create a branch from `main`
2. Make your changes and add tests where appropriate
3. Ensure all checks pass: `cargo test --workspace && cargo clippy --workspace -- -D warnings`
4. Open a pull request with a clear description of the change
5. Link any related issues

## Project Structure

| Crate | Purpose |
|---|---|
| `runifi-plugin-api` | Stable plugin contract: Processor/Source/Sink traits, FlowFile |
| `runifi-core` | Engine runtime: scheduler, supervisor, repositories, connections |
| `runifi-processors` | Built-in processors: GenerateFlowFile, LogAttribute, GetFile, PutFile |
| `runifi-transport` | QUIC/TCP transport, zero-copy IO |
| `runifi-server` | Runtime binary, config loading |
| `runifi-cli` | CLI tools for flow management |

## Questions?

Open an issue on [GitHub](https://github.com/Exiate/runifi/issues) if you have questions or need help getting started.
