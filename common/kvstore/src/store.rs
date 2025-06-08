//! Key-Value Store implementations

use crate::traits::*;
use anyhow::Result;
use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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

/// File-based persistent key-value store implementation
#[derive(Debug)]
pub struct FileKVStore<K, V> {
    path: PathBuf,
    store: MemoryKVStore<K, V>,
}

impl<K, V> FileKVStore<K, V>
where
    K: Clone
        + Send
        + Sync
        + std::hash::Hash
        + Eq
        + Serialize
        + DeserializeOwned
        + bincode::Encode
        + bincode::Decode<()>
        + 'static,
    V: Clone
        + Send
        + Sync
        + Serialize
        + DeserializeOwned
        + bincode::Encode
        + bincode::Decode<()>
        + 'static,
{
    /// Creates a new `FileKVStore`. If the file at `path` exists, it will be loaded.
    /// Otherwise, an empty store is created.
    pub async fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        let mut store = Self {
            path: path_buf,
            store: MemoryKVStore::new(),
        };

        if store.path.exists() {
            store.load().await?;
        }

        Ok(store)
    }
}

#[async_trait]
impl<K, V> KVStore for FileKVStore<K, V>
where
    K: Clone
        + Send
        + Sync
        + std::hash::Hash
        + Eq
        + Serialize
        + DeserializeOwned
        + bincode::Encode
        + bincode::Decode<()>
        + 'static,
    V: Clone
        + Send
        + Sync
        + Serialize
        + DeserializeOwned
        + bincode::Encode
        + bincode::Decode<()>
        + 'static,
{
    type Key = K;
    type Value = V;

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>> {
        self.store.get(key).await
    }

    async fn put(&self, key: Self::Key, value: Self::Value) -> Result<()> {
        self.store.put(key, value).await
    }

    async fn remove(&self, key: &Self::Key) -> Result<Option<Self::Value>> {
        self.store.remove(key).await
    }

    async fn contains_key(&self, key: &Self::Key) -> Result<bool> {
        self.store.contains_key(key).await
    }

    async fn keys(&self) -> Result<Vec<Self::Key>> {
        self.store.keys().await
    }

    async fn clear(&self) -> Result<()> {
        self.store.clear().await
    }

    async fn size(&self) -> Result<usize> {
        self.store.size().await
    }
}

#[async_trait]
impl<K, V> PersistentKVStore for FileKVStore<K, V>
where
    K: Clone
        + Send
        + Sync
        + std::hash::Hash
        + Eq
        + Serialize
        + DeserializeOwned
        + bincode::Encode
        + bincode::Decode<()>
        + 'static,
    V: Clone
        + Send
        + Sync
        + Serialize
        + DeserializeOwned
        + bincode::Encode
        + bincode::Decode<()>
        + 'static,
{
    async fn persist(&self) -> Result<()> {
        let data_map = self.store.data.read().await;
        let bytes = bincode::encode_to_vec(&*data_map, bincode::config::standard())?;

        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let mut file = File::create(&self.path).await?;
        file.write_all(&bytes).await?;
        Ok(())
    }

    async fn load(&mut self) -> Result<()> {
        let mut file = File::open(&self.path).await?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;

        if buffer.is_empty() {
            // Handle empty file case, initialize with empty map
            *self.store.data.write().await = HashMap::new();
        } else {
            let (decoded, _): (HashMap<K, V>, _) =
                bincode::decode_from_slice(&buffer, bincode::config::standard())?;
            *self.store.data.write().await = decoded;
        }
        Ok(())
    }

    fn persistence_path(&self) -> Option<&str> {
        self.path.to_str()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_memory_kv_store() {
        let store = MemoryKVStore::<String, String>::new();

        // Test put and get
        store
            .put("key1".to_string(), "value1".to_string())
            .await
            .unwrap();
        assert_eq!(
            store.get(&"key1".to_string()).await.unwrap(),
            Some("value1".to_string())
        );
        assert_eq!(store.get(&"non_existent".to_string()).await.unwrap(), None);

        // Test size and contains_key
        assert_eq!(store.size().await.unwrap(), 1);
        assert!(store.contains_key(&"key1".to_string()).await.unwrap());
        assert!(
            !store
                .contains_key(&"non_existent".to_string())
                .await
                .unwrap()
        );

        // Test remove
        assert_eq!(
            store.remove(&"key1".to_string()).await.unwrap(),
            Some("value1".to_string())
        );
        assert_eq!(store.size().await.unwrap(), 0);
        assert!(!store.contains_key(&"key1".to_string()).await.unwrap());

        // Test keys and clear
        store
            .put("key_a".to_string(), "val_a".to_string())
            .await
            .unwrap();
        store
            .put("key_b".to_string(), "val_b".to_string())
            .await
            .unwrap();
        let mut keys = store.keys().await.unwrap();
        keys.sort();
        assert_eq!(keys, vec!["key_a".to_string(), "key_b".to_string()]);

        store.clear().await.unwrap();
        assert_eq!(store.size().await.unwrap(), 0);
        assert!(store.keys().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_file_kv_store_empty_and_new() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.db");

        // Test loading from a non-existent path
        let store1 = FileKVStore::<String, i32>::new(&path).await.unwrap();
        assert_eq!(store1.size().await.unwrap(), 0);
        store1.put("one".to_string(), 1).await.unwrap();
        store1.persist().await.unwrap();

        // Test loading from an existing file
        let store2 = FileKVStore::<String, i32>::new(&path).await.unwrap();
        assert_eq!(store2.get(&"one".to_string()).await.unwrap(), Some(1));

        // Test persisting an empty store and reloading
        store2.clear().await.unwrap();
        store2.persist().await.unwrap();
        let store3 = FileKVStore::<String, i32>::new(&path).await.unwrap();
        assert_eq!(store3.size().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_file_kv_store_persist_and_load() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        // Create and populate a store
        let store1 = FileKVStore::<String, i32>::new(&path).await.unwrap();
        store1.put("key1".to_string(), 100).await.unwrap();
        store1.put("key2".to_string(), 200).await.unwrap();
        assert_eq!(store1.size().await.unwrap(), 2);

        // Persist the store
        store1.persist().await.unwrap();

        // Create a new store from the same path
        let store2 = FileKVStore::<String, i32>::new(&path).await.unwrap();
        assert_eq!(store2.size().await.unwrap(), 2);
        assert_eq!(store2.get(&"key1".to_string()).await.unwrap(), Some(100));
        assert_eq!(store2.get(&"key2".to_string()).await.unwrap(), Some(200));

        // Modify the second store and persist
        store2.remove(&"key1".to_string()).await.unwrap();
        store2.put("key3".to_string(), 300).await.unwrap();
        store2.persist().await.unwrap();

        // Create a third store to verify the changes
        let store3 = FileKVStore::<String, i32>::new(&path).await.unwrap();
        assert_eq!(store3.size().await.unwrap(), 2);
        assert_eq!(store3.get(&"key1".to_string()).await.unwrap(), None);
        assert_eq!(store3.get(&"key2".to_string()).await.unwrap(), Some(200));
        assert_eq!(store3.get(&"key3".to_string()).await.unwrap(), Some(300));
    }

    #[tokio::test]
    async fn test_memory_kv_store_concurrency() {
        let store = Arc::new(MemoryKVStore::<String, usize>::new());
        let mut handles = Vec::new();

        // Spawn 10 tasks to write concurrently
        for i in 0..10 {
            let store_clone = Arc::clone(&store);
            handles.push(tokio::spawn(async move {
                for j in 0..100 {
                    let key = format!("key_{}_{}", i, j);
                    store_clone.put(key, i * 100 + j).await.unwrap();
                }
            }));
        }

        // Wait for all writers to finish
        for handle in handles {
            handle.await.unwrap();
        }

        // Verify the final state
        assert_eq!(store.size().await.unwrap(), 1000);

        // Verify one key from each task
        for i in 0..10 {
            let key = format!("key_{}_50", i);
            let value = store.get(&key).await.unwrap();
            assert_eq!(value, Some(i * 100 + 50));
        }
    }
}
