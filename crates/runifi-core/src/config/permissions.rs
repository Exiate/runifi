use thiserror::Error;

/// Errors from config file permission checks.
#[derive(Debug, Error)]
pub enum PermissionError {
    #[error("config file is world-readable (mode {mode:#o}): {path}")]
    WorldReadable { path: String, mode: u32 },

    #[error("failed to read file metadata: {0}")]
    Metadata(String),
}

/// Check that a config file's permissions are appropriately restrictive.
///
/// - **Warning** if the file is group-readable (mode & 0o040 != 0).
/// - **Error** if the file is world-readable (mode & 0o004 != 0).
///
/// This check is only meaningful on Unix systems; it is a no-op on other
/// platforms.
///
/// Returns `Ok(())` if the permissions are acceptable (possibly with a
/// logged warning), or `Err` if the file is world-readable.
#[cfg(unix)]
pub fn check_config_permissions(path: &str) -> Result<(), PermissionError> {
    use std::os::unix::fs::PermissionsExt;

    let metadata = std::fs::metadata(path)
        .map_err(|e| PermissionError::Metadata(format!("{}: {}", path, e)))?;

    let mode = metadata.permissions().mode();

    if mode & 0o004 != 0 {
        return Err(PermissionError::WorldReadable {
            path: path.to_string(),
            mode,
        });
    }

    if mode & 0o040 != 0 {
        tracing::warn!(
            path = %path,
            mode = format!("{:#o}", mode),
            "Config file is group-readable; consider restricting to owner-only (chmod 600)"
        );
    }

    Ok(())
}

/// No-op on non-Unix platforms.
#[cfg(not(unix))]
pub fn check_config_permissions(_path: &str) -> Result<(), PermissionError> {
    Ok(())
}

#[cfg(test)]
#[cfg(unix)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    fn create_temp_config(mode: u32) -> tempfile::NamedTempFile {
        let file = tempfile::NamedTempFile::new().unwrap();
        let perms = fs::Permissions::from_mode(mode);
        fs::set_permissions(file.path(), perms).unwrap();
        file
    }

    #[test]
    fn owner_only_passes() {
        let file = create_temp_config(0o600);
        let result = check_config_permissions(file.path().to_str().unwrap());
        assert!(result.is_ok());
    }

    #[test]
    fn group_readable_warns_but_passes() {
        let file = create_temp_config(0o640);
        let result = check_config_permissions(file.path().to_str().unwrap());
        // Should succeed (only warns, does not error).
        assert!(result.is_ok());
    }

    #[test]
    fn world_readable_fails() {
        let file = create_temp_config(0o644);
        let result = check_config_permissions(file.path().to_str().unwrap());
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, PermissionError::WorldReadable { .. }));
    }

    #[test]
    fn nonexistent_file_fails() {
        let result = check_config_permissions("/tmp/runifi-nonexistent-file-xyz-99.toml");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, PermissionError::Metadata(_)));
    }

    #[test]
    fn owner_read_write_only_passes() {
        let file = create_temp_config(0o400);
        let result = check_config_permissions(file.path().to_str().unwrap());
        assert!(result.is_ok());
    }

    #[test]
    fn world_executable_but_not_readable_passes() {
        let file = create_temp_config(0o601);
        let result = check_config_permissions(file.path().to_str().unwrap());
        assert!(result.is_ok());
    }
}
