use std::fs;
use std::path::Path;

fn main() {
    // Ensure dashboard-dist/ exists so include_dir! never panics in CI.
    // The real build step (`npm ci && npm run build` in dashboard-react/) will
    // overwrite these stubs with the compiled assets.
    let dist = Path::new(env!("CARGO_MANIFEST_DIR")).join("dashboard-dist");
    if !dist.exists() {
        fs::create_dir_all(dist.join("assets")).expect("failed to create dashboard-dist/assets/");
        fs::write(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("dashboard-dist")
                .join("index.html"),
            "<!doctype html><html><body>Dashboard not built</body></html>",
        )
        .expect("failed to write stub index.html");
        fs::write(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("dashboard-dist")
                .join("assets")
                .join("app.js"),
            "",
        )
        .expect("failed to write stub app.js");
        fs::write(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("dashboard-dist")
                .join("assets")
                .join("app.css"),
            "",
        )
        .expect("failed to write stub app.css");
    }

    // Re-run this script if the dist directory is removed.
    println!("cargo:rerun-if-changed=dashboard-dist/index.html");
}
