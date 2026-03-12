use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // TODO: Parse CLI args, connect to running RuniFi instance
    println!("runifi-cli v{}", env!("CARGO_PKG_VERSION"));
    Ok(())
}
