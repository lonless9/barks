//! Key-Value Store traits

use async_trait::async_trait;
use thiserror::Error;

/// Error types for KV store operations
#[derive(Error, Debug, Clone)]
pub enum KVStoreError {
    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Deserialization error: {0}")]
    DeserializationError(String),

    #[error("IO error: {0}")]
    IoError(String),

    #[error("Key not found: {0}")]
    KeyNotFound(String),

    #[error("Storage error: {0}")]
    StorageError(String),

    #[error("Persistence error: {0}")]
    PersistenceError(String),

    #[error("Configuration error: {0}")]
    ConfigurationError(String),
}

/// Result type for KV store operations
pub type KVStoreResult<T> = Result<T, KVStoreError>;

/// Trait for key-value store operations
#[async_trait]
pub trait KVStore: Send + Sync {
    type Key: Clone + Send + Sync;
    type Value: Clone + Send + Sync;

    /// Get a value by key
    async fn get(&self, key: &Self::Key) -> KVStoreResult<Option<Self::Value>>;

    /// Put a key-value pair
    async fn put(&self, key: &Self::Key, value: Self::Value) -> KVStoreResult<()>;

    /// Remove a key
    async fn remove(&self, key: &Self::Key) -> KVStoreResult<Option<Self::Value>>;

    /// Check if key exists
    async fn contains_key(&self, key: &Self::Key) -> KVStoreResult<bool>;

    /// Get all keys
    async fn keys(&self) -> KVStoreResult<Vec<Self::Key>>;

    /// Clear all entries
    async fn clear(&self) -> KVStoreResult<()>;

    /// Get store size
    async fn size(&self) -> KVStoreResult<usize>;
}

/// Trait for persistent key-value store
#[async_trait]
pub trait PersistentKVStore: KVStore {
    /// Persist store to disk
    async fn persist(&self) -> KVStoreResult<()>;

    /// Load store from disk
    async fn load(&mut self) -> KVStoreResult<()>;

    /// Get persistence path
    fn persistence_path(&self) -> Option<&str>;
}
