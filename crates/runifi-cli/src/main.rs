use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use runifi_core::config::property_encryption::{
    decrypt_property_value, encrypt_property_value, is_encrypted_value,
};
use runifi_core::engine::persistence;

/// RuniFi CLI — flow management, status, and diagnostics.
#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Secrets management commands.
    Secrets {
        #[command(subcommand)]
        action: SecretsAction,
    },
}

#[derive(Subcommand)]
enum SecretsAction {
    /// Rotate the encryption key for sensitive properties in a flow state file.
    ///
    /// Reads all encrypted ENC() values with the old key, re-encrypts with
    /// the new key, and writes the updated flow state atomically.
    RotateKey {
        /// Path to the runtime config directory containing flow.json.
        #[arg(long, default_value = "./data/conf")]
        conf_dir: PathBuf,

        /// Hex-encoded old 256-bit encryption key (64 hex characters).
        #[arg(long)]
        old_key_hex: String,

        /// Hex-encoded new 256-bit encryption key (64 hex characters).
        #[arg(long)]
        new_key_hex: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Secrets { action }) => match action {
            SecretsAction::RotateKey {
                conf_dir,
                old_key_hex,
                new_key_hex,
            } => rotate_key(&conf_dir, &old_key_hex, &new_key_hex)?,
        },
        None => {
            println!("runifi-cli v{}", env!("CARGO_PKG_VERSION"));
            println!("Use --help to see available commands.");
        }
    }

    Ok(())
}

fn rotate_key(conf_dir: &std::path::Path, old_key_hex: &str, new_key_hex: &str) -> Result<()> {
    let old_key = hex::decode(old_key_hex).context("Invalid hex for old key")?;
    let new_key = hex::decode(new_key_hex).context("Invalid hex for new key")?;

    if old_key.len() != 32 {
        anyhow::bail!("Old key must be exactly 32 bytes (64 hex characters)");
    }
    if new_key.len() != 32 {
        anyhow::bail!("New key must be exactly 32 bytes (64 hex characters)");
    }

    let mut state = persistence::load_runtime_flow(conf_dir)
        .context("Failed to load flow state")?
        .context("No flow state file found in the specified directory")?;

    let mut rotated_count = 0u32;

    for proc in &mut state.processors {
        for (prop_name, prop_value) in proc.properties.iter_mut() {
            if is_encrypted_value(prop_value) {
                let plaintext =
                    decrypt_property_value(prop_value, &old_key).with_context(|| {
                        format!(
                            "Failed to decrypt property '{}' on processor '{}'",
                            prop_name, proc.name
                        )
                    })?;
                *prop_value = encrypt_property_value(&plaintext, &new_key).with_context(|| {
                    format!(
                        "Failed to re-encrypt property '{}' on processor '{}'",
                        prop_name, proc.name
                    )
                })?;
                rotated_count += 1;
            }
        }
    }

    for svc in &mut state.services {
        for (prop_name, prop_value) in svc.properties.iter_mut() {
            if is_encrypted_value(prop_value) {
                let plaintext =
                    decrypt_property_value(prop_value, &old_key).with_context(|| {
                        format!(
                            "Failed to decrypt property '{}' on service '{}'",
                            prop_name, svc.name
                        )
                    })?;
                *prop_value = encrypt_property_value(&plaintext, &new_key).with_context(|| {
                    format!(
                        "Failed to re-encrypt property '{}' on service '{}'",
                        prop_name, svc.name
                    )
                })?;
                rotated_count += 1;
            }
        }
    }

    // Write atomically (matching persistence module pattern):
    // 1. Write to temp file, 2. fsync temp, 3. backup rename, 4. atomic rename, 5. fsync dir
    let json = serde_json::to_string_pretty(&state).context("Failed to serialize flow state")?;
    let tmp_path = conf_dir.join("flow.json.tmp");
    let flow_path = conf_dir.join("flow.json");
    let bak_path = conf_dir.join("flow.json.bak");

    {
        use std::io::Write;
        let mut file = std::fs::File::create(&tmp_path).context("Failed to create temp file")?;
        file.write_all(json.as_bytes())
            .context("Failed to write temp file")?;
        file.sync_all().context("Failed to fsync temp file")?;
    }

    if flow_path.exists() {
        let _ = std::fs::rename(&flow_path, &bak_path);
    }
    std::fs::rename(&tmp_path, &flow_path).context("Failed to rename temp file to flow.json")?;

    // Fsync the directory to ensure the rename metadata is durable.
    std::fs::File::open(conf_dir)
        .and_then(|f| f.sync_all())
        .context("Failed to fsync config directory")?;

    println!(
        "Key rotation complete: {} encrypted properties re-encrypted.",
        rotated_count
    );

    Ok(())
}
