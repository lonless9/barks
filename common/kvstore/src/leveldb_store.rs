//! LevelDB-based key-value store implementation

use crate::traits::*;
use async_trait::async_trait;
use rusty_leveldb::{DB, LdbIterator, Options};
use serde::{Serialize, de::DeserializeOwned};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

/// LevelDB-based key-value store implementation
pub struct LevelDBKVStore<K, V> {
    /// The LevelDB database instance
    db: Arc<Mutex<DB>>,
    /// Path to the database
    path: PathBuf,
    /// Phantom data for key and value types
    _phantom: std::marker::PhantomData<(K, V)>,
}

impl<K, V> LevelDBKVStore<K, V>
where
    K: Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Create a new LevelDB store at the specified path
    pub fn new<P: AsRef<Path>>(path: P) -> KVStoreResult<Self> {
        let path = path.as_ref().to_path_buf();

        // Create directory if it doesn't exist
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| KVStoreError::IoError(format!("Failed to create directory: {}", e)))?;
        }

        let options = Options {
            create_if_missing: true,
            ..Default::default()
        };

        let db = DB::open(&path, options)
            .map_err(|e| KVStoreError::IoError(format!("Failed to open LevelDB: {}", e)))?;

        Ok(Self {
            db: Arc::new(Mutex::new(db)),
            path,
            _phantom: std::marker::PhantomData,
        })
    }

    /// Get the database path
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[async_trait]
impl<K, V> KVStore for LevelDBKVStore<K, V>
where
    K: Clone
        + Send
        + Sync
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

    async fn get(&self, key: &Self::Key) -> KVStoreResult<Option<Self::Value>> {
        let key_bytes = bincode::encode_to_vec(key, bincode::config::standard())
            .map_err(|e| KVStoreError::SerializationError(e.to_string()))?;

        let mut db = self.db.lock().await;

        match db.get(&key_bytes) {
            Some(value_bytes) => {
                let (value, _): (V, _) =
                    bincode::decode_from_slice(&value_bytes, bincode::config::standard())
                        .map_err(|e| KVStoreError::DeserializationError(e.to_string()))?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    async fn put(&self, key: &Self::Key, value: Self::Value) -> KVStoreResult<()> {
        let key_bytes = bincode::encode_to_vec(key, bincode::config::standard())
            .map_err(|e| KVStoreError::SerializationError(e.to_string()))?;

        let value_bytes = bincode::encode_to_vec(&value, bincode::config::standard())
            .map_err(|e| KVStoreError::SerializationError(e.to_string()))?;

        let mut db = self.db.lock().await;

        db.put(&key_bytes, &value_bytes)
            .map_err(|e| KVStoreError::IoError(format!("LevelDB put error: {}", e)))?;

        Ok(())
    }

    async fn remove(&self, key: &Self::Key) -> KVStoreResult<Option<Self::Value>> {
        // First get the current value
        let current_value = self.get(key).await?;

        if current_value.is_some() {
            let key_bytes = bincode::encode_to_vec(key, bincode::config::standard())
                .map_err(|e| KVStoreError::SerializationError(e.to_string()))?;

            let mut db = self.db.lock().await;

            db.delete(&key_bytes)
                .map_err(|e| KVStoreError::IoError(format!("LevelDB delete error: {}", e)))?;
        }

        Ok(current_value)
    }

    async fn contains_key(&self, key: &Self::Key) -> KVStoreResult<bool> {
        let key_bytes = bincode::encode_to_vec(key, bincode::config::standard())
            .map_err(|e| KVStoreError::SerializationError(e.to_string()))?;

        let mut db = self.db.lock().await;

        match db.get(&key_bytes) {
            Some(_) => Ok(true),
            None => Ok(false),
        }
    }

    async fn clear(&self) -> KVStoreResult<()> {
        // LevelDB doesn't have a direct clear operation
        // We need to iterate through all keys and delete them
        // For now, we'll implement a simple approach by dropping and recreating the database

        let path = self.path.clone();

        // Close the current database
        drop(self.db.lock().await);

        // Remove the database directory
        if path.exists() {
            std::fs::remove_dir_all(&path).map_err(|e| {
                KVStoreError::IoError(format!("Failed to remove database directory: {}", e))
            })?;
        }

        // Recreate the database
        let options = Options {
            create_if_missing: true,
            ..Default::default()
        };

        let new_db = DB::open(&path, options)
            .map_err(|e| KVStoreError::IoError(format!("Failed to recreate LevelDB: {}", e)))?;

        // Replace the database instance
        *self.db.lock().await = new_db;

        Ok(())
    }

    async fn keys(&self) -> KVStoreResult<Vec<Self::Key>> {
        let mut db = self.db.lock().await;
        let mut keys = Vec::new();

        // Create an iterator to get all keys
        let mut iter = db
            .new_iter()
            .map_err(|e| KVStoreError::IoError(format!("Failed to create iterator: {}", e)))?;
        iter.seek_to_first();

        while iter.valid() {
            let mut key_bytes = Vec::new();
            let mut value_bytes = Vec::new();
            if iter.current(&mut key_bytes, &mut value_bytes) {
                let (key, _): (K, _) =
                    bincode::decode_from_slice(&key_bytes, bincode::config::standard())
                        .map_err(|e| KVStoreError::DeserializationError(e.to_string()))?;
                keys.push(key);
            }
            iter.advance();
        }

        Ok(keys)
    }

    async fn size(&self) -> KVStoreResult<usize> {
        let mut db = self.db.lock().await;
        let mut count = 0;

        // Create an iterator to count all keys
        let mut iter = db
            .new_iter()
            .map_err(|e| KVStoreError::IoError(format!("Failed to create iterator: {}", e)))?;
        iter.seek_to_first();

        while iter.valid() {
            count += 1;
            iter.advance();
        }

        Ok(count)
    }
}

#[async_trait]
impl<K, V> PersistentKVStore for LevelDBKVStore<K, V>
where
    K: Clone
        + Send
        + Sync
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
    async fn persist(&self) -> KVStoreResult<()> {
        // LevelDB automatically persists data, so this is a no-op
        // We could potentially force a sync here if needed
        Ok(())
    }

    async fn load(&mut self) -> KVStoreResult<()> {
        // LevelDB automatically loads data on open, so this is a no-op
        Ok(())
    }

    fn persistence_path(&self) -> Option<&str> {
        self.path.to_str()
    }
}

impl<K, V> Clone for LevelDBKVStore<K, V>
where
    K: Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            db: Arc::clone(&self.db),
            path: self.path.clone(),
            _phantom: std::marker::PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_leveldb_basic_operations() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test_db");

        let store: LevelDBKVStore<String, i32> = LevelDBKVStore::new(&db_path).unwrap();

        // Test put and get
        store.put(&"key1".to_string(), 42).await.unwrap();
        assert_eq!(store.get(&"key1".to_string()).await.unwrap(), Some(42));
        assert_eq!(store.get(&"nonexistent".to_string()).await.unwrap(), None);

        // Test contains_key
        assert!(store.contains_key(&"key1".to_string()).await.unwrap());
        assert!(
            !store
                .contains_key(&"nonexistent".to_string())
                .await
                .unwrap()
        );

        // Test size and keys
        assert_eq!(store.size().await.unwrap(), 1);
        let keys = store.keys().await.unwrap();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0], "key1".to_string());

        // Test remove
        assert_eq!(store.remove(&"key1".to_string()).await.unwrap(), Some(42));
        assert_eq!(store.remove(&"key1".to_string()).await.unwrap(), None);
        assert_eq!(store.size().await.unwrap(), 0);
        let keys = store.keys().await.unwrap();
        assert!(keys.is_empty());
    }

    #[tokio::test]
    async fn test_leveldb_persistence() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test_persistence_db");

        // Create store and add data
        {
            let store: LevelDBKVStore<String, String> = LevelDBKVStore::new(&db_path).unwrap();
            store
                .put(
                    &"persistent_key".to_string(),
                    "persistent_value".to_string(),
                )
                .await
                .unwrap();
        }

        // Reopen store and verify data persists
        {
            let store: LevelDBKVStore<String, String> = LevelDBKVStore::new(&db_path).unwrap();
            assert_eq!(
                store.get(&"persistent_key".to_string()).await.unwrap(),
                Some("persistent_value".to_string())
            );
        }
    }

    #[tokio::test]
    async fn test_leveldb_clear() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test_clear_db");

        let store: LevelDBKVStore<String, i32> = LevelDBKVStore::new(&db_path).unwrap();

        // Add some data
        store.put(&"key1".to_string(), 1).await.unwrap();
        store.put(&"key2".to_string(), 2).await.unwrap();
        assert_eq!(store.size().await.unwrap(), 2);

        // Clear and verify
        store.clear().await.unwrap();
        assert_eq!(store.size().await.unwrap(), 0);
        let keys = store.keys().await.unwrap();
        assert!(keys.is_empty());
    }
}
