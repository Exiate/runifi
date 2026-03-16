//! DBCPConnectionPool — shared database connection pool configuration.
//!
//! Provides a controller service that holds database connection parameters.
//! Processors reference this service to obtain connection configuration
//! without duplicating JDBC-style settings across multiple processors.

use std::collections::HashMap;

use runifi_plugin_api::property::PropertyDescriptor;
use runifi_plugin_api::result::{PluginError, ProcessResult};
use runifi_plugin_api::service::{ControllerService, ControllerServiceDescriptor};

/// Database connection pool configuration service.
///
/// Holds connection parameters (URL, credentials, pool sizing) that processors
/// can reference. Actual driver integration is deferred to transport plugins.
pub struct DBCPConnectionPool {
    enabled: bool,
    connection_url: String,
    driver_type: String,
    username: Option<String>,
    password: Option<String>,
    max_total: u32,
    min_idle: u32,
    max_wait_ms: u64,
    validation_query: String,
}

const PROP_CONNECTION_URL: PropertyDescriptor = PropertyDescriptor::new(
    "Database Connection URL",
    "The JDBC-style connection URL for the database (e.g., postgresql://host:5432/db).",
)
.required()
.expression_language_supported();

const PROP_DRIVER_TYPE: PropertyDescriptor = PropertyDescriptor::new(
    "Database Driver Type",
    "The type of database driver to use.",
)
.default_value("postgresql")
.allowed_values(&["postgresql", "mysql", "sqlite"]);

const PROP_USERNAME: PropertyDescriptor =
    PropertyDescriptor::new("Database User", "The username for database authentication.")
        .expression_language_supported();

const PROP_PASSWORD: PropertyDescriptor = PropertyDescriptor::new(
    "Database Password",
    "The password for database authentication.",
)
.sensitive();

const PROP_MAX_TOTAL: PropertyDescriptor = PropertyDescriptor::new(
    "Max Total Connections",
    "The maximum number of connections in the pool.",
)
.default_value("10");

const PROP_MIN_IDLE: PropertyDescriptor = PropertyDescriptor::new(
    "Min Idle Connections",
    "The minimum number of idle connections to maintain in the pool.",
)
.default_value("0");

const PROP_MAX_WAIT: PropertyDescriptor = PropertyDescriptor::new(
    "Max Wait Time",
    "The maximum time in milliseconds to wait for a connection from the pool.",
)
.default_value("5000");

const PROP_VALIDATION_QUERY: PropertyDescriptor = PropertyDescriptor::new(
    "Validation Query",
    "SQL query used to validate connections before use.",
)
.default_value("SELECT 1");

impl DBCPConnectionPool {
    pub fn new() -> Self {
        Self {
            enabled: false,
            connection_url: String::new(),
            driver_type: "postgresql".to_string(),
            username: None,
            password: None,
            max_total: 10,
            min_idle: 0,
            max_wait_ms: 5000,
            validation_query: "SELECT 1".to_string(),
        }
    }

    /// Get the configured connection URL.
    pub fn connection_url(&self) -> &str {
        &self.connection_url
    }

    /// Get the configured driver type.
    pub fn driver_type(&self) -> &str {
        &self.driver_type
    }

    /// Get the configured username.
    pub fn username(&self) -> Option<&str> {
        self.username.as_deref()
    }

    /// Get the maximum total connections.
    pub fn max_total(&self) -> u32 {
        self.max_total
    }

    /// Get the minimum idle connections.
    pub fn min_idle(&self) -> u32 {
        self.min_idle
    }

    /// Get the max wait time in milliseconds.
    pub fn max_wait_ms(&self) -> u64 {
        self.max_wait_ms
    }

    /// Get the validation query.
    pub fn validation_query(&self) -> &str {
        &self.validation_query
    }

    fn parse_u32(value: &str, name: &str) -> ProcessResult<u32> {
        value.parse::<u32>().map_err(|_| {
            PluginError::ProcessingFailed(format!(
                "'{name}' must be a non-negative integer, got: {value}"
            ))
        })
    }

    fn parse_u64(value: &str, name: &str) -> ProcessResult<u64> {
        value.parse::<u64>().map_err(|_| {
            PluginError::ProcessingFailed(format!(
                "'{name}' must be a non-negative integer, got: {value}"
            ))
        })
    }
}

impl Default for DBCPConnectionPool {
    fn default() -> Self {
        Self::new()
    }
}

impl ControllerService for DBCPConnectionPool {
    fn on_configure(&mut self, properties: &HashMap<String, String>) -> ProcessResult {
        self.connection_url = properties
            .get("Database Connection URL")
            .cloned()
            .unwrap_or_default();
        self.driver_type = properties
            .get("Database Driver Type")
            .cloned()
            .unwrap_or_else(|| "postgresql".to_string());
        self.username = properties.get("Database User").cloned();
        self.password = properties.get("Database Password").cloned();

        if let Some(val) = properties.get("Max Total Connections") {
            self.max_total = Self::parse_u32(val, "Max Total Connections")?;
        }
        if let Some(val) = properties.get("Min Idle Connections") {
            self.min_idle = Self::parse_u32(val, "Min Idle Connections")?;
        }
        if let Some(val) = properties.get("Max Wait Time") {
            self.max_wait_ms = Self::parse_u64(val, "Max Wait Time")?;
        }
        if let Some(val) = properties.get("Validation Query") {
            self.validation_query = val.clone();
        }

        Ok(())
    }

    fn validate(&self) -> ProcessResult {
        if self.connection_url.is_empty() {
            return Err(PluginError::ProcessingFailed(
                "Database Connection URL is required".to_string(),
            ));
        }

        // Validate URL has a scheme.
        if !self.connection_url.contains("://") {
            return Err(PluginError::ProcessingFailed(format!(
                "Database Connection URL must include a scheme (e.g., postgresql://), got: {}",
                self.connection_url
            )));
        }

        if self.min_idle > self.max_total {
            return Err(PluginError::ProcessingFailed(format!(
                "Min Idle Connections ({}) cannot exceed Max Total Connections ({})",
                self.min_idle, self.max_total
            )));
        }

        Ok(())
    }

    fn enable(&mut self) -> ProcessResult {
        self.enabled = true;
        tracing::info!(
            url = %self.connection_url,
            driver = %self.driver_type,
            max_total = self.max_total,
            "DBCPConnectionPool enabled"
        );
        Ok(())
    }

    fn disable(&mut self) -> ProcessResult {
        self.enabled = false;
        tracing::info!("DBCPConnectionPool disabled");
        Ok(())
    }

    fn is_enabled(&self) -> bool {
        self.enabled
    }

    fn property_descriptors(&self) -> Vec<PropertyDescriptor> {
        vec![
            PROP_CONNECTION_URL,
            PROP_DRIVER_TYPE,
            PROP_USERNAME,
            PROP_PASSWORD,
            PROP_MAX_TOTAL,
            PROP_MIN_IDLE,
            PROP_MAX_WAIT,
            PROP_VALIDATION_QUERY,
        ]
    }
}

inventory::submit! {
    ControllerServiceDescriptor {
        type_name: "DBCPConnectionPool",
        description: "Provides shared database connection pool configuration for SQL processors",
        factory: || Box::new(DBCPConnectionPool::new()),
        tags: &["Database", "Connection Pool"],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn property_descriptors_returned() {
        let svc = DBCPConnectionPool::new();
        let descriptors = svc.property_descriptors();
        assert_eq!(descriptors.len(), 8);

        let names: Vec<&str> = descriptors.iter().map(|d| d.name).collect();
        assert!(names.contains(&"Database Connection URL"));
        assert!(names.contains(&"Database Driver Type"));
        assert!(names.contains(&"Database User"));
        assert!(names.contains(&"Database Password"));
        assert!(names.contains(&"Max Total Connections"));
    }

    #[test]
    fn password_is_sensitive() {
        let svc = DBCPConnectionPool::new();
        let descriptors = svc.property_descriptors();
        let password = descriptors
            .iter()
            .find(|d| d.name == "Database Password")
            .unwrap();
        assert!(password.sensitive);
    }

    #[test]
    fn connection_url_is_required() {
        let svc = DBCPConnectionPool::new();
        let descriptors = svc.property_descriptors();
        let url = descriptors
            .iter()
            .find(|d| d.name == "Database Connection URL")
            .unwrap();
        assert!(url.required);
    }

    #[test]
    fn lifecycle() {
        let mut svc = DBCPConnectionPool::new();
        assert!(!svc.is_enabled());

        let mut props = HashMap::new();
        props.insert(
            "Database Connection URL".to_string(),
            "postgresql://localhost:5432/test".to_string(),
        );
        svc.on_configure(&props).unwrap();
        svc.validate().unwrap();
        svc.enable().unwrap();
        assert!(svc.is_enabled());

        assert_eq!(svc.connection_url(), "postgresql://localhost:5432/test");
        assert_eq!(svc.driver_type(), "postgresql");
        assert_eq!(svc.max_total(), 10);
        assert_eq!(svc.min_idle(), 0);
        assert_eq!(svc.max_wait_ms(), 5000);

        svc.disable().unwrap();
        assert!(!svc.is_enabled());
    }

    #[test]
    fn validate_fails_without_url() {
        let svc = DBCPConnectionPool::new();
        assert!(svc.validate().is_err());
    }

    #[test]
    fn validate_fails_with_bad_url() {
        let mut svc = DBCPConnectionPool::new();
        let mut props = HashMap::new();
        props.insert(
            "Database Connection URL".to_string(),
            "not-a-url".to_string(),
        );
        svc.on_configure(&props).unwrap();
        assert!(svc.validate().is_err());
    }

    #[test]
    fn validate_fails_when_min_idle_exceeds_max() {
        let mut svc = DBCPConnectionPool::new();
        let mut props = HashMap::new();
        props.insert(
            "Database Connection URL".to_string(),
            "postgresql://localhost/db".to_string(),
        );
        props.insert("Max Total Connections".to_string(), "5".to_string());
        props.insert("Min Idle Connections".to_string(), "10".to_string());
        svc.on_configure(&props).unwrap();
        assert!(svc.validate().is_err());
    }

    #[test]
    fn configure_numeric_properties() {
        let mut svc = DBCPConnectionPool::new();
        let mut props = HashMap::new();
        props.insert(
            "Database Connection URL".to_string(),
            "postgresql://localhost/db".to_string(),
        );
        props.insert("Max Total Connections".to_string(), "20".to_string());
        props.insert("Min Idle Connections".to_string(), "5".to_string());
        props.insert("Max Wait Time".to_string(), "10000".to_string());
        svc.on_configure(&props).unwrap();

        assert_eq!(svc.max_total(), 20);
        assert_eq!(svc.min_idle(), 5);
        assert_eq!(svc.max_wait_ms(), 10000);
    }

    #[test]
    fn configure_rejects_invalid_numeric() {
        let mut svc = DBCPConnectionPool::new();
        let mut props = HashMap::new();
        props.insert(
            "Database Connection URL".to_string(),
            "postgresql://localhost/db".to_string(),
        );
        props.insert("Max Total Connections".to_string(), "abc".to_string());
        assert!(svc.on_configure(&props).is_err());
    }

    #[test]
    fn getters_return_configured_values() {
        let mut svc = DBCPConnectionPool::new();
        let mut props = HashMap::new();
        props.insert(
            "Database Connection URL".to_string(),
            "mysql://host/db".to_string(),
        );
        props.insert("Database Driver Type".to_string(), "mysql".to_string());
        props.insert("Database User".to_string(), "admin".to_string());
        props.insert(
            "Validation Query".to_string(),
            "SELECT 1 FROM dual".to_string(),
        );
        svc.on_configure(&props).unwrap();

        assert_eq!(svc.driver_type(), "mysql");
        assert_eq!(svc.username(), Some("admin"));
        assert_eq!(svc.validation_query(), "SELECT 1 FROM dual");
    }
}
