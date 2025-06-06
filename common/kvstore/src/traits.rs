//! Key-Value Store traits

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Trait for key-value store operations
#[async_trait]
pub trait KVStore: Send + Sync {
    type Key: Clone + Send + Sync;
    type Value: Clone + Send + Sync;

    /// Get a value by key
    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>>;

    /// Put a key-value pair
    async fn put(&self, key: Self::Key, value: Self::Value) -> Result<()>;

    /// Remove a key
    async fn remove(&self, key: &Self::Key) -> Result<Option<Self::Value>>;

    /// Check if key exists
    async fn contains_key(&self, key: &Self::Key) -> Result<bool>;

    /// Get all keys
    async fn keys(&self) -> Result<Vec<Self::Key>>;

    /// Clear all entries
    async fn clear(&self) -> Result<()>;

    /// Get store size
    async fn size(&self) -> Result<usize>;
}

/// Trait for serializable key-value store
#[async_trait]
pub trait SerializableKVStore: KVStore {
    /// Serialize store to bytes
    async fn serialize(&self) -> Result<Vec<u8>>;

    /// Deserialize store from bytes
    async fn deserialize(&mut self, data: &[u8]) -> Result<()>;
}

/// Trait for persistent key-value store
#[async_trait]
pub trait PersistentKVStore: KVStore {
    /// Persist store to disk
    async fn persist(&self) -> Result<()>;

    /// Load store from disk
    async fn load(&mut self) -> Result<()>;

    /// Get persistence path
    fn persistence_path(&self) -> Option<&str>;
}

/// Trait for distributed key-value store
#[async_trait]
pub trait DistributedKVStore: KVStore {
    /// Replicate data to other nodes
    async fn replicate(&self, nodes: &[String]) -> Result<()>;

    /// Sync with other nodes
    async fn sync(&self, nodes: &[String]) -> Result<()>;

    /// Get replication factor
    fn replication_factor(&self) -> usize;
}
