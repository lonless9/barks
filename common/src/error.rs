//! Error handling for the barks-common crate.

use thiserror::Error;

/// Common error type that abstracts over underlying library errors.
///
/// This enum provides structured error types with support for error chaining,
/// rich context, and diagnostic information.
#[derive(Error, Debug)]
pub enum CommonError {
    #[error("Cache operation failed: {message}")]
    CacheError {
        message: String,
        #[source]
        source: Option<anyhow::Error>,
    },

    #[error("Storage operation failed: {message}")]
    StorageError {
        message: String,
        #[source]
        source: Option<anyhow::Error>,
    },

    #[error("Serialization failed: {message}")]
    SerializationError {
        message: String,
        #[source]
        source: Option<anyhow::Error>,
    },

    #[error("Deserialization failed: {message}")]
    DeserializationError {
        message: String,
        #[source]
        source: Option<anyhow::Error>,
    },

    #[error("Invalid configuration: {message}")]
    ConfigurationError {
        message: String,
        #[source]
        source: Option<anyhow::Error>,
    },

    #[error("IO operation failed: {message}")]
    IoError {
        message: String,
        #[source]
        source: Option<anyhow::Error>,
    },

    #[error("Compression failed: {message}")]
    CompressionError {
        message: String,
        #[source]
        source: Option<anyhow::Error>,
    },

    #[error("Decompression failed: {message}")]
    DecompressionError {
        message: String,
        #[source]
        source: Option<anyhow::Error>,
    },

    #[error("Network operation failed: {message}")]
    NetworkError {
        message: String,
        #[source]
        source: Option<anyhow::Error>,
    },

    #[error("Timeout occurred: {message}")]
    TimeoutError {
        message: String,
        #[source]
        source: Option<anyhow::Error>,
    },

    #[error("Resource not found: {message}")]
    NotFoundError {
        message: String,
        #[source]
        source: Option<anyhow::Error>,
    },

    #[error("Permission denied: {message}")]
    PermissionError {
        message: String,
        #[source]
        source: Option<anyhow::Error>,
    },

    #[error("Resource exhausted: {message}")]
    ResourceExhaustedError {
        message: String,
        #[source]
        source: Option<anyhow::Error>,
    },

    #[error("Internal error: {message}")]
    InternalError {
        message: String,
        #[source]
        source: Option<anyhow::Error>,
    },
}

/// Result type alias for common operations.
pub type Result<T> = std::result::Result<T, CommonError>;

/// Error severity levels for categorizing errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorSeverity {
    /// Low severity - operation can continue with degraded functionality
    Low,
    /// Medium severity - operation should be retried or alternative approach used
    Medium,
    /// High severity - operation must be aborted but system can continue
    High,
    /// Critical severity - system integrity is at risk
    Critical,
}

/// Error category for grouping related error types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCategory {
    /// Infrastructure-related errors (storage, network, etc.)
    Infrastructure,
    /// Data processing errors (serialization, compression, etc.)
    DataProcessing,
    /// Configuration and setup errors
    Configuration,
    /// Resource management errors (memory, file handles, etc.)
    Resource,
    /// Security and permission errors
    Security,
    /// Internal logic errors
    Internal,
}

/// Trait for rich error diagnostics with context and suggestions.
pub trait Diagnose {
    /// Get the error severity level.
    fn severity(&self) -> ErrorSeverity;

    /// Get the error category.
    fn category(&self) -> ErrorCategory;

    /// Get additional context about the error.
    fn context(&self) -> Vec<String>;

    /// Get suggestions for resolving the error.
    fn suggestions(&self) -> Vec<String>;

    /// Check if the error is retryable.
    fn is_retryable(&self) -> bool;

    /// Get the recommended retry delay in milliseconds.
    fn retry_delay_ms(&self) -> Option<u64>;
}

impl CommonError {
    /// Create a cache error with a custom message.
    pub fn cache_error<S: Into<String>>(message: S) -> Self {
        Self::CacheError {
            message: message.into(),
            source: None,
        }
    }

    /// Create a cache error with a custom message and source error.
    pub fn cache_error_with_source<S: Into<String>, E: Into<anyhow::Error>>(
        message: S,
        source: E,
    ) -> Self {
        Self::CacheError {
            message: message.into(),
            source: Some(source.into()),
        }
    }

    /// Create a storage error with a custom message.
    pub fn storage_error<S: Into<String>>(message: S) -> Self {
        Self::StorageError {
            message: message.into(),
            source: None,
        }
    }

    /// Create a storage error with a custom message and source error.
    pub fn storage_error_with_source<S: Into<String>, E: Into<anyhow::Error>>(
        message: S,
        source: E,
    ) -> Self {
        Self::StorageError {
            message: message.into(),
            source: Some(source.into()),
        }
    }

    /// Create a serialization error with a custom message.
    pub fn serialization_error<S: Into<String>>(message: S) -> Self {
        Self::SerializationError {
            message: message.into(),
            source: None,
        }
    }

    /// Create a serialization error with a custom message and source error.
    pub fn serialization_error_with_source<S: Into<String>, E: Into<anyhow::Error>>(
        message: S,
        source: E,
    ) -> Self {
        Self::SerializationError {
            message: message.into(),
            source: Some(source.into()),
        }
    }

    /// Create a deserialization error with a custom message.
    pub fn deserialization_error<S: Into<String>>(message: S) -> Self {
        Self::DeserializationError {
            message: message.into(),
            source: None,
        }
    }

    /// Create a deserialization error with a custom message and source error.
    pub fn deserialization_error_with_source<S: Into<String>, E: Into<anyhow::Error>>(
        message: S,
        source: E,
    ) -> Self {
        Self::DeserializationError {
            message: message.into(),
            source: Some(source.into()),
        }
    }

    /// Create a configuration error with a custom message.
    pub fn configuration_error<S: Into<String>>(message: S) -> Self {
        Self::ConfigurationError {
            message: message.into(),
            source: None,
        }
    }

    /// Create a configuration error with a custom message and source error.
    pub fn configuration_error_with_source<S: Into<String>, E: Into<anyhow::Error>>(
        message: S,
        source: E,
    ) -> Self {
        Self::ConfigurationError {
            message: message.into(),
            source: Some(source.into()),
        }
    }

    /// Create an IO error with a custom message.
    pub fn io_error<S: Into<String>>(message: S) -> Self {
        Self::IoError {
            message: message.into(),
            source: None,
        }
    }

    /// Create an IO error with a custom message and source error.
    pub fn io_error_with_source<S: Into<String>, E: Into<anyhow::Error>>(
        message: S,
        source: E,
    ) -> Self {
        Self::IoError {
            message: message.into(),
            source: Some(source.into()),
        }
    }

    /// Create a compression error with a custom message.
    pub fn compression_error<S: Into<String>>(message: S) -> Self {
        Self::CompressionError {
            message: message.into(),
            source: None,
        }
    }

    /// Create a compression error with a custom message and source error.
    pub fn compression_error_with_source<S: Into<String>, E: Into<anyhow::Error>>(
        message: S,
        source: E,
    ) -> Self {
        Self::CompressionError {
            message: message.into(),
            source: Some(source.into()),
        }
    }

    /// Create a decompression error with a custom message.
    pub fn decompression_error<S: Into<String>>(message: S) -> Self {
        Self::DecompressionError {
            message: message.into(),
            source: None,
        }
    }

    /// Create a decompression error with a custom message and source error.
    pub fn decompression_error_with_source<S: Into<String>, E: Into<anyhow::Error>>(
        message: S,
        source: E,
    ) -> Self {
        Self::DecompressionError {
            message: message.into(),
            source: Some(source.into()),
        }
    }

    /// Create a network error with a custom message.
    pub fn network_error<S: Into<String>>(message: S) -> Self {
        Self::NetworkError {
            message: message.into(),
            source: None,
        }
    }

    /// Create a network error with a custom message and source error.
    pub fn network_error_with_source<S: Into<String>, E: Into<anyhow::Error>>(
        message: S,
        source: E,
    ) -> Self {
        Self::NetworkError {
            message: message.into(),
            source: Some(source.into()),
        }
    }

    /// Create a timeout error with a custom message.
    pub fn timeout_error<S: Into<String>>(message: S) -> Self {
        Self::TimeoutError {
            message: message.into(),
            source: None,
        }
    }

    /// Create a timeout error with a custom message and source error.
    pub fn timeout_error_with_source<S: Into<String>, E: Into<anyhow::Error>>(
        message: S,
        source: E,
    ) -> Self {
        Self::TimeoutError {
            message: message.into(),
            source: Some(source.into()),
        }
    }

    /// Create a not found error with a custom message.
    pub fn not_found_error<S: Into<String>>(message: S) -> Self {
        Self::NotFoundError {
            message: message.into(),
            source: None,
        }
    }

    /// Create a not found error with a custom message and source error.
    pub fn not_found_error_with_source<S: Into<String>, E: Into<anyhow::Error>>(
        message: S,
        source: E,
    ) -> Self {
        Self::NotFoundError {
            message: message.into(),
            source: Some(source.into()),
        }
    }

    /// Create a permission error with a custom message.
    pub fn permission_error<S: Into<String>>(message: S) -> Self {
        Self::PermissionError {
            message: message.into(),
            source: None,
        }
    }

    /// Create a permission error with a custom message and source error.
    pub fn permission_error_with_source<S: Into<String>, E: Into<anyhow::Error>>(
        message: S,
        source: E,
    ) -> Self {
        Self::PermissionError {
            message: message.into(),
            source: Some(source.into()),
        }
    }

    /// Create a resource exhausted error with a custom message.
    pub fn resource_exhausted_error<S: Into<String>>(message: S) -> Self {
        Self::ResourceExhaustedError {
            message: message.into(),
            source: None,
        }
    }

    /// Create a resource exhausted error with a custom message and source error.
    pub fn resource_exhausted_error_with_source<S: Into<String>, E: Into<anyhow::Error>>(
        message: S,
        source: E,
    ) -> Self {
        Self::ResourceExhaustedError {
            message: message.into(),
            source: Some(source.into()),
        }
    }

    /// Create an internal error with a custom message.
    pub fn internal_error<S: Into<String>>(message: S) -> Self {
        Self::InternalError {
            message: message.into(),
            source: None,
        }
    }

    /// Create an internal error with a custom message and source error.
    pub fn internal_error_with_source<S: Into<String>, E: Into<anyhow::Error>>(
        message: S,
        source: E,
    ) -> Self {
        Self::InternalError {
            message: message.into(),
            source: Some(source.into()),
        }
    }
}

impl Diagnose for CommonError {
    fn severity(&self) -> ErrorSeverity {
        match self {
            CommonError::CacheError { .. } => ErrorSeverity::Medium,
            CommonError::StorageError { .. } => ErrorSeverity::High,
            CommonError::SerializationError { .. } => ErrorSeverity::Medium,
            CommonError::DeserializationError { .. } => ErrorSeverity::Medium,
            CommonError::ConfigurationError { .. } => ErrorSeverity::High,
            CommonError::IoError { .. } => ErrorSeverity::Medium,
            CommonError::CompressionError { .. } => ErrorSeverity::Low,
            CommonError::DecompressionError { .. } => ErrorSeverity::Medium,
            CommonError::NetworkError { .. } => ErrorSeverity::Medium,
            CommonError::TimeoutError { .. } => ErrorSeverity::Medium,
            CommonError::NotFoundError { .. } => ErrorSeverity::Low,
            CommonError::PermissionError { .. } => ErrorSeverity::High,
            CommonError::ResourceExhaustedError { .. } => ErrorSeverity::High,
            CommonError::InternalError { .. } => ErrorSeverity::Critical,
        }
    }

    fn category(&self) -> ErrorCategory {
        match self {
            CommonError::CacheError { .. } => ErrorCategory::Infrastructure,
            CommonError::StorageError { .. } => ErrorCategory::Infrastructure,
            CommonError::SerializationError { .. } => ErrorCategory::DataProcessing,
            CommonError::DeserializationError { .. } => ErrorCategory::DataProcessing,
            CommonError::ConfigurationError { .. } => ErrorCategory::Configuration,
            CommonError::IoError { .. } => ErrorCategory::Infrastructure,
            CommonError::CompressionError { .. } => ErrorCategory::DataProcessing,
            CommonError::DecompressionError { .. } => ErrorCategory::DataProcessing,
            CommonError::NetworkError { .. } => ErrorCategory::Infrastructure,
            CommonError::TimeoutError { .. } => ErrorCategory::Infrastructure,
            CommonError::NotFoundError { .. } => ErrorCategory::Resource,
            CommonError::PermissionError { .. } => ErrorCategory::Security,
            CommonError::ResourceExhaustedError { .. } => ErrorCategory::Resource,
            CommonError::InternalError { .. } => ErrorCategory::Internal,
        }
    }

    fn context(&self) -> Vec<String> {
        let mut context = Vec::new();

        // Add error-specific context
        match self {
            CommonError::CacheError { message, .. } => {
                context.push(format!("Cache operation context: {}", message));
                context.push(
                    "This may indicate cache capacity issues or network problems".to_string(),
                );
            }
            CommonError::StorageError { message, .. } => {
                context.push(format!("Storage operation context: {}", message));
                context.push(
                    "This may indicate disk space, permissions, or database corruption issues"
                        .to_string(),
                );
            }
            CommonError::SerializationError { message, .. } => {
                context.push(format!("Serialization context: {}", message));
                context.push(
                    "This may indicate incompatible data types or version mismatches".to_string(),
                );
            }
            CommonError::DeserializationError { message, .. } => {
                context.push(format!("Deserialization context: {}", message));
                context.push("This may indicate corrupted data or format changes".to_string());
            }
            CommonError::ConfigurationError { message, .. } => {
                context.push(format!("Configuration context: {}", message));
                context
                    .push("This indicates invalid or missing configuration parameters".to_string());
            }
            CommonError::IoError { message, .. } => {
                context.push(format!("I/O operation context: {}", message));
                context.push(
                    "This may indicate file system issues or network connectivity problems"
                        .to_string(),
                );
            }
            CommonError::CompressionError { message, .. } => {
                context.push(format!("Compression context: {}", message));
                context.push(
                    "This may indicate invalid compression parameters or corrupted input"
                        .to_string(),
                );
            }
            CommonError::DecompressionError { message, .. } => {
                context.push(format!("Decompression context: {}", message));
                context.push(
                    "This may indicate corrupted compressed data or format mismatches".to_string(),
                );
            }
            CommonError::NetworkError { message, .. } => {
                context.push(format!("Network operation context: {}", message));
                context.push(
                    "This may indicate connectivity issues or service unavailability".to_string(),
                );
            }
            CommonError::TimeoutError { message, .. } => {
                context.push(format!("Timeout context: {}", message));
                context.push("This indicates an operation took longer than expected".to_string());
            }
            CommonError::NotFoundError { message, .. } => {
                context.push(format!("Resource not found context: {}", message));
                context.push("This indicates a requested resource does not exist".to_string());
            }
            CommonError::PermissionError { message, .. } => {
                context.push(format!("Permission context: {}", message));
                context.push(
                    "This indicates insufficient privileges for the requested operation"
                        .to_string(),
                );
            }
            CommonError::ResourceExhaustedError { message, .. } => {
                context.push(format!("Resource exhaustion context: {}", message));
                context.push(
                    "This indicates system resources (memory, disk, etc.) are exhausted"
                        .to_string(),
                );
            }
            CommonError::InternalError { message, .. } => {
                context.push(format!("Internal error context: {}", message));
                context.push("This indicates an unexpected internal condition".to_string());
            }
        }

        context
    }

    fn suggestions(&self) -> Vec<String> {
        match self {
            CommonError::CacheError { .. } => vec![
                "Check cache configuration and capacity settings".to_string(),
                "Verify network connectivity to cache servers".to_string(),
                "Consider implementing cache fallback mechanisms".to_string(),
            ],
            CommonError::StorageError { .. } => vec![
                "Check disk space and file system permissions".to_string(),
                "Verify database connectivity and integrity".to_string(),
                "Consider implementing storage redundancy".to_string(),
                "Check for storage service health and availability".to_string(),
            ],
            CommonError::SerializationError { .. } => vec![
                "Verify data type compatibility".to_string(),
                "Check serialization format versions".to_string(),
                "Ensure all required fields are present".to_string(),
            ],
            CommonError::DeserializationError { .. } => vec![
                "Verify data integrity and format".to_string(),
                "Check for version compatibility issues".to_string(),
                "Validate input data before deserialization".to_string(),
            ],
            CommonError::ConfigurationError { .. } => vec![
                "Review configuration file syntax and values".to_string(),
                "Check for missing required configuration parameters".to_string(),
                "Validate configuration against schema".to_string(),
                "Ensure configuration files are accessible".to_string(),
            ],
            CommonError::IoError { .. } => vec![
                "Check file system permissions and disk space".to_string(),
                "Verify file paths and accessibility".to_string(),
                "Check network connectivity for remote operations".to_string(),
            ],
            CommonError::CompressionError { .. } => vec![
                "Verify compression algorithm parameters".to_string(),
                "Check input data validity".to_string(),
                "Consider using different compression settings".to_string(),
            ],
            CommonError::DecompressionError { .. } => vec![
                "Verify compressed data integrity".to_string(),
                "Check compression format compatibility".to_string(),
                "Ensure complete data transfer".to_string(),
            ],
            CommonError::NetworkError { .. } => vec![
                "Check network connectivity and DNS resolution".to_string(),
                "Verify service endpoints and ports".to_string(),
                "Consider implementing retry mechanisms".to_string(),
                "Check firewall and security group settings".to_string(),
            ],
            CommonError::TimeoutError { .. } => vec![
                "Increase timeout values if appropriate".to_string(),
                "Check system performance and load".to_string(),
                "Consider implementing asynchronous operations".to_string(),
                "Verify network latency and bandwidth".to_string(),
            ],
            CommonError::NotFoundError { .. } => vec![
                "Verify resource path and identifier".to_string(),
                "Check if resource was moved or deleted".to_string(),
                "Ensure proper resource initialization".to_string(),
            ],
            CommonError::PermissionError { .. } => vec![
                "Check user permissions and access rights".to_string(),
                "Verify authentication credentials".to_string(),
                "Review security policies and roles".to_string(),
                "Ensure proper service account configuration".to_string(),
            ],
            CommonError::ResourceExhaustedError { .. } => vec![
                "Monitor and increase system resources".to_string(),
                "Implement resource cleanup and garbage collection".to_string(),
                "Consider scaling horizontally or vertically".to_string(),
                "Review resource usage patterns and optimization".to_string(),
            ],
            CommonError::InternalError { .. } => vec![
                "Report this issue to the development team".to_string(),
                "Check system logs for additional details".to_string(),
                "Consider restarting the affected service".to_string(),
                "Verify system integrity and dependencies".to_string(),
            ],
        }
    }

    fn is_retryable(&self) -> bool {
        match self {
            CommonError::CacheError { .. } => true,
            CommonError::StorageError { .. } => true,
            CommonError::SerializationError { .. } => false,
            CommonError::DeserializationError { .. } => false,
            CommonError::ConfigurationError { .. } => false,
            CommonError::IoError { .. } => true,
            CommonError::CompressionError { .. } => false,
            CommonError::DecompressionError { .. } => false,
            CommonError::NetworkError { .. } => true,
            CommonError::TimeoutError { .. } => true,
            CommonError::NotFoundError { .. } => false,
            CommonError::PermissionError { .. } => false,
            CommonError::ResourceExhaustedError { .. } => true,
            CommonError::InternalError { .. } => false,
        }
    }

    fn retry_delay_ms(&self) -> Option<u64> {
        if !self.is_retryable() {
            return None;
        }

        match self {
            CommonError::CacheError { .. } => Some(1000), // 1 second
            CommonError::StorageError { .. } => Some(2000), // 2 seconds
            CommonError::IoError { .. } => Some(1000),    // 1 second
            CommonError::NetworkError { .. } => Some(5000), // 5 seconds
            CommonError::TimeoutError { .. } => Some(10000), // 10 seconds
            CommonError::ResourceExhaustedError { .. } => Some(30000), // 30 seconds
            _ => None,
        }
    }
}

/// Conversion utilities for seamless integration between anyhow and CommonError.
pub mod conversion {
    use super::*;

    /// Convert anyhow::Error to CommonError with automatic categorization.
    pub fn from_anyhow(error: anyhow::Error) -> CommonError {
        let error_str = error.to_string().to_lowercase();

        // Try to categorize based on error message content
        // Order matters - more specific patterns should come first
        if error_str.contains("timeout") || error_str.contains("deadline") {
            CommonError::timeout_error_with_source("Operation timed out", error)
        } else if error_str.contains("not found") || error_str.contains("missing") {
            CommonError::not_found_error_with_source("Resource not found", error)
        } else if error_str.contains("permission")
            || error_str.contains("access")
            || error_str.contains("unauthorized")
        {
            CommonError::permission_error_with_source("Permission denied", error)
        } else if error_str.contains("cache") {
            CommonError::cache_error_with_source("Cache operation failed", error)
        } else if error_str.contains("storage")
            || error_str.contains("database")
            || error_str.contains("db")
        {
            CommonError::storage_error_with_source("Storage operation failed", error)
        } else if error_str.contains("serialize") || error_str.contains("serialization") {
            CommonError::serialization_error_with_source("Serialization failed", error)
        } else if error_str.contains("deserialize") || error_str.contains("deserialization") {
            CommonError::deserialization_error_with_source("Deserialization failed", error)
        } else if error_str.contains("config") || error_str.contains("configuration") {
            CommonError::configuration_error_with_source("Configuration error", error)
        } else if error_str.contains("compress") || error_str.contains("compression") {
            CommonError::compression_error_with_source("Compression failed", error)
        } else if error_str.contains("decompress") || error_str.contains("decompression") {
            CommonError::decompression_error_with_source("Decompression failed", error)
        } else if error_str.contains("network")
            || error_str.contains("connection")
            || error_str.contains("socket")
        {
            CommonError::network_error_with_source("Network operation failed", error)
        } else if error_str.contains("resource")
            || error_str.contains("memory")
            || error_str.contains("exhausted")
        {
            CommonError::resource_exhausted_error_with_source("Resource exhausted", error)
        } else if error_str.contains("io")
            || error_str.contains("file")
            || error_str.contains("directory")
        {
            CommonError::io_error_with_source("I/O operation failed", error)
        } else {
            CommonError::internal_error_with_source("Internal error", error)
        }
    }

    /// Convert Result<T, anyhow::Error> to Result<T, CommonError>.
    pub fn from_anyhow_result<T>(result: anyhow::Result<T>) -> Result<T> {
        result.map_err(from_anyhow)
    }
}

/// Context helpers for adding rich context to errors.
pub mod context {
    use super::*;

    /// Extension trait for adding context to Results.
    pub trait ErrorContext<T> {
        /// Add context to an error with automatic categorization.
        fn with_context<F>(self, f: F) -> Result<T>
        where
            F: FnOnce() -> String;

        /// Add context to an error with specific error type.
        fn with_cache_context<F>(self, f: F) -> Result<T>
        where
            F: FnOnce() -> String;

        /// Add context to an error with specific error type.
        fn with_storage_context<F>(self, f: F) -> Result<T>
        where
            F: FnOnce() -> String;

        /// Add context to an error with specific error type.
        fn with_io_context<F>(self, f: F) -> Result<T>
        where
            F: FnOnce() -> String;

        /// Add context to an error with specific error type.
        fn with_network_context<F>(self, f: F) -> Result<T>
        where
            F: FnOnce() -> String;
    }

    impl<T, E> ErrorContext<T> for std::result::Result<T, E>
    where
        E: Into<anyhow::Error>,
    {
        fn with_context<F>(self, f: F) -> Result<T>
        where
            F: FnOnce() -> String,
        {
            self.map_err(|e| {
                let context = f();
                conversion::from_anyhow(e.into().context(context))
            })
        }

        fn with_cache_context<F>(self, f: F) -> Result<T>
        where
            F: FnOnce() -> String,
        {
            self.map_err(|e| {
                let context = f();
                CommonError::cache_error_with_source(context, e.into())
            })
        }

        fn with_storage_context<F>(self, f: F) -> Result<T>
        where
            F: FnOnce() -> String,
        {
            self.map_err(|e| {
                let context = f();
                CommonError::storage_error_with_source(context, e.into())
            })
        }

        fn with_io_context<F>(self, f: F) -> Result<T>
        where
            F: FnOnce() -> String,
        {
            self.map_err(|e| {
                let context = f();
                CommonError::io_error_with_source(context, e.into())
            })
        }

        fn with_network_context<F>(self, f: F) -> Result<T>
        where
            F: FnOnce() -> String,
        {
            self.map_err(|e| {
                let context = f();
                CommonError::network_error_with_source(context, e.into())
            })
        }
    }
}

pub use context::ErrorContext;
/// Re-export commonly used types and traits.
pub use conversion::{from_anyhow, from_anyhow_result};

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;
    use std::error::Error;

    #[test]
    fn test_error_creation() {
        let cache_error = CommonError::cache_error("Cache failed");
        assert!(matches!(cache_error, CommonError::CacheError { .. }));

        let storage_error = CommonError::storage_error_with_source(
            "Storage failed",
            anyhow!("underlying storage error"),
        );
        assert!(matches!(storage_error, CommonError::StorageError { .. }));
    }

    #[test]
    fn test_diagnose_trait() {
        let cache_error = CommonError::cache_error("Cache failed");
        assert_eq!(cache_error.severity(), ErrorSeverity::Medium);
        assert_eq!(cache_error.category(), ErrorCategory::Infrastructure);
        assert!(cache_error.is_retryable());
        assert_eq!(cache_error.retry_delay_ms(), Some(1000));

        let config_error = CommonError::configuration_error("Invalid config");
        assert_eq!(config_error.severity(), ErrorSeverity::High);
        assert_eq!(config_error.category(), ErrorCategory::Configuration);
        assert!(!config_error.is_retryable());
        assert_eq!(config_error.retry_delay_ms(), None);

        let internal_error = CommonError::internal_error("Internal error");
        assert_eq!(internal_error.severity(), ErrorSeverity::Critical);
        assert_eq!(internal_error.category(), ErrorCategory::Internal);
        assert!(!internal_error.is_retryable());
    }

    #[test]
    fn test_context_and_suggestions() {
        let storage_error = CommonError::storage_error("Database connection failed");
        let context = storage_error.context();
        assert!(!context.is_empty());
        assert!(context[0].contains("Storage operation context"));

        let suggestions = storage_error.suggestions();
        assert!(!suggestions.is_empty());
        assert!(suggestions.iter().any(|s| s.contains("disk space")));
    }

    #[test]
    fn test_anyhow_conversion() {
        let anyhow_error = anyhow!("cache connection failed");
        let common_error = conversion::from_anyhow(anyhow_error);
        assert!(matches!(common_error, CommonError::CacheError { .. }));

        let anyhow_error = anyhow!("database storage error");
        let common_error = conversion::from_anyhow(anyhow_error);
        assert!(matches!(common_error, CommonError::StorageError { .. }));

        let anyhow_error = anyhow!("operation timeout exceeded");
        let common_error = conversion::from_anyhow(anyhow_error);
        assert!(matches!(common_error, CommonError::TimeoutError { .. }));
    }

    #[test]
    fn test_error_context_extension() {
        use context::ErrorContext;

        let result: std::result::Result<(), std::io::Error> = Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "file not found",
        ));

        let common_result =
            result.with_io_context(|| "Failed to read configuration file".to_string());
        assert!(common_result.is_err());
        assert!(matches!(
            common_result.unwrap_err(),
            CommonError::IoError { .. }
        ));

        let result2: std::result::Result<(), anyhow::Error> = Err(anyhow!("cache miss"));
        let common_result2 = result2.with_cache_context(|| "Cache lookup failed".to_string());
        assert!(common_result2.is_err());
        assert!(matches!(
            common_result2.unwrap_err(),
            CommonError::CacheError { .. }
        ));
    }

    #[test]
    fn test_error_chaining() {
        let root_cause = anyhow!("root cause error");
        let storage_error = CommonError::storage_error_with_source("Storage failed", root_cause);

        // Test that source error is preserved
        assert!(storage_error.source().is_some());

        // Test error display includes chain
        let error_string = format!("{}", storage_error);
        assert!(error_string.contains("Storage operation failed"));
    }

    #[test]
    fn test_all_error_variants() {
        let errors = vec![
            CommonError::cache_error("test"),
            CommonError::storage_error("test"),
            CommonError::serialization_error("test"),
            CommonError::deserialization_error("test"),
            CommonError::configuration_error("test"),
            CommonError::io_error("test"),
            CommonError::compression_error("test"),
            CommonError::decompression_error("test"),
            CommonError::network_error("test"),
            CommonError::timeout_error("test"),
            CommonError::not_found_error("test"),
            CommonError::permission_error("test"),
            CommonError::resource_exhausted_error("test"),
            CommonError::internal_error("test"),
        ];

        for error in errors {
            // Test that all errors implement required traits
            let _ = error.severity();
            let _ = error.category();
            let _ = error.context();
            let _ = error.suggestions();
            let _ = error.is_retryable();
            let _ = error.retry_delay_ms();

            // Test that all errors can be displayed
            let _ = format!("{}", error);
            let _ = format!("{:?}", error);
        }
    }

    #[test]
    fn test_error_severity_levels() {
        assert_eq!(
            CommonError::cache_error("test").severity(),
            ErrorSeverity::Medium
        );
        assert_eq!(
            CommonError::storage_error("test").severity(),
            ErrorSeverity::High
        );
        assert_eq!(
            CommonError::configuration_error("test").severity(),
            ErrorSeverity::High
        );
        assert_eq!(
            CommonError::internal_error("test").severity(),
            ErrorSeverity::Critical
        );
        assert_eq!(
            CommonError::not_found_error("test").severity(),
            ErrorSeverity::Low
        );
    }

    #[test]
    fn test_error_categories() {
        assert_eq!(
            CommonError::cache_error("test").category(),
            ErrorCategory::Infrastructure
        );
        assert_eq!(
            CommonError::serialization_error("test").category(),
            ErrorCategory::DataProcessing
        );
        assert_eq!(
            CommonError::configuration_error("test").category(),
            ErrorCategory::Configuration
        );
        assert_eq!(
            CommonError::permission_error("test").category(),
            ErrorCategory::Security
        );
        assert_eq!(
            CommonError::resource_exhausted_error("test").category(),
            ErrorCategory::Resource
        );
        assert_eq!(
            CommonError::internal_error("test").category(),
            ErrorCategory::Internal
        );
    }

    #[test]
    fn test_retry_behavior() {
        // Retryable errors
        assert!(CommonError::cache_error("test").is_retryable());
        assert!(CommonError::storage_error("test").is_retryable());
        assert!(CommonError::network_error("test").is_retryable());
        assert!(CommonError::timeout_error("test").is_retryable());
        assert!(CommonError::resource_exhausted_error("test").is_retryable());

        // Non-retryable errors
        assert!(!CommonError::serialization_error("test").is_retryable());
        assert!(!CommonError::configuration_error("test").is_retryable());
        assert!(!CommonError::permission_error("test").is_retryable());
        assert!(!CommonError::internal_error("test").is_retryable());
    }
}
