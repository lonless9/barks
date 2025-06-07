//! Key-Value Store implementations

use crate::traits::*;
use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// In-memory key-value store implementation
#[derive(Debug, Clone)]
pub struct MemoryKVStore<K, V> {
    data: Arc<RwLock<HashMap<K, V>>>,
}

impl<K, V> Default for MemoryKVStore<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> MemoryKVStore<K, V> {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl<K, V> KVStore for MemoryKVStore<K, V>
where
    K: Clone + Send + Sync + std::hash::Hash + Eq + 'static,
    V: Clone + Send + Sync + 'static,
{
    type Key = K;
    type Value = V;

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>> {
        let data = self.data.read().await;
        Ok(data.get(key).cloned())
    }

    async fn put(&self, key: Self::Key, value: Self::Value) -> Result<()> {
        let mut data = self.data.write().await;
        data.insert(key, value);
        Ok(())
    }

    async fn remove(&self, key: &Self::Key) -> Result<Option<Self::Value>> {
        let mut data = self.data.write().await;
        Ok(data.remove(key))
    }

    async fn contains_key(&self, key: &Self::Key) -> Result<bool> {
        let data = self.data.read().await;
        Ok(data.contains_key(key))
    }

    async fn keys(&self) -> Result<Vec<Self::Key>> {
        let data = self.data.read().await;
        Ok(data.keys().cloned().collect())
    }

    async fn clear(&self) -> Result<()> {
        let mut data = self.data.write().await;
        data.clear();
        Ok(())
    }

    async fn size(&self) -> Result<usize> {
        let data = self.data.read().await;
        Ok(data.len())
    }
}
