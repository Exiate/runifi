# RuniFi development commands

# Build all crates (debug)
build:
    cargo build --workspace

# Build all crates (release)
release:
    cargo build --workspace --release

# Run all tests
test:
    cargo test --workspace

# Check formatting
fmt-check:
    cargo fmt --all -- --check

# Format code
fmt:
    cargo fmt --all

# Run clippy
lint:
    cargo clippy --workspace -- -D warnings

# Run all CI checks locally
ci: fmt-check lint test

# Run the server (blank canvas by default)
run:
    cargo run -p runifi

# Run the server with example pipeline
run-demo:
    cargo run -p runifi -- config/examples/demo-pipeline.toml

# Run the CLI
cli *ARGS:
    cargo run -p runifi-cli -- {{ ARGS }}

# Build .deb package (requires cargo-deb)
deb: release
    cargo deb -p runifi --no-build --no-strip

# Build .rpm package (requires cargo-generate-rpm)
rpm: release
    cargo generate-rpm -p crates/runifi-server

# Build all packages
package: deb rpm

# Clean build artifacts
clean:
    cargo clean
    rm -rf dist/out/
