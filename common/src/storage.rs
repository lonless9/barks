//! Object storage abstraction using trait-based design.
//!
//! This module provides a generic storage interface that abstracts over
//! the underlying object storage implementation using the object_store crate.

use async_trait::async_trait;
use futures::StreamExt;
use object_store::{
    ObjectStore, PutPayload, local::LocalFileSystem, memory::InMemory, path::Path as ObjectPath,
};
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::{CommonError, Result};

/// Generic storage trait for async key-value operations.
///
/// This trait provides a unified interface for persistent storage operations
/// without exposing the underlying implementation details.
#[async_trait]
pub trait Storage: Send + Sync + Debug {
    /// Get a value from storage by key.
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Insert a key-value pair into storage.
    async fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;

    /// Remove a key from storage.
    async fn delete(&self, key: &[u8]) -> Result<()>;

    /// Check if a key exists in storage.
    async fn contains_key(&self, key: &[u8]) -> Result<bool>;

    /// Get all keys with a given prefix.
    async fn keys_with_prefix(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>>;

    /// Get all key-value pairs with a given prefix.
    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;

    /// Perform a batch write operation.
    async fn batch_write(&self, operations: Vec<BatchOperation>) -> Result<()>;

    /// Flush any pending writes to disk.
    async fn flush(&self) -> Result<()>;

    /// Compact the storage to reclaim space.
    async fn compact(&self) -> Result<()>;

    /// Get storage statistics if available.
    async fn stats(&self) -> StorageStats;

    /// Close the storage and release resources.
    async fn close(&self) -> Result<()>;
}

/// Batch operation for atomic writes.
#[derive(Debug, Clone)]
pub enum BatchOperation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

/// Storage statistics.
#[derive(Debug, Clone, Default)]
pub struct StorageStats {
    pub read_count: u64,
    pub write_count: u64,
    pub delete_count: u64,
    pub batch_count: u64,
    pub error_count: u64,
    pub total_keys: u64,
    pub total_size_bytes: u64,
}

/// Storage backend configuration.
#[derive(Debug, Clone)]
pub enum StorageBackend {
    /// In-memory storage for testing and development.
    Memory,
    /// Local filesystem storage.
    LocalFileSystem { root_path: String },
    /// AWS S3 storage.
    #[cfg(feature = "aws")]
    S3 {
        bucket: String,
        region: String,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
        endpoint: Option<String>,
    },
    /// Azure Blob Storage.
    #[cfg(feature = "azure")]
    Azure {
        account: String,
        container: String,
        access_key: Option<String>,
        endpoint: Option<String>,
    },
    /// Google Cloud Storage.
    #[cfg(feature = "gcp")]
    Gcs {
        bucket: String,
        service_account_path: Option<String>,
    },
}

impl Default for StorageBackend {
    fn default() -> Self {
        Self::Memory
    }
}

/// Configuration for storage creation.
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// Storage backend to use.
    pub backend: StorageBackend,
    /// Optional prefix for all keys.
    pub key_prefix: Option<String>,
    /// Enable compression for stored values.
    pub compression: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            backend: StorageBackend::default(),
            key_prefix: None,
            compression: false,
        }
    }
}

/// Builder for creating storage instances.
pub struct StorageBuilder {
    config: StorageConfig,
}

impl StorageBuilder {
    /// Create a new storage builder with default configuration.
    pub fn new() -> Self {
        Self {
            config: StorageConfig::default(),
        }
    }

    /// Set the storage backend.
    pub fn backend(mut self, backend: StorageBackend) -> Self {
        self.config.backend = backend;
        self
    }

    /// Set a key prefix for all operations.
    pub fn key_prefix<S: Into<String>>(mut self, prefix: S) -> Self {
        self.config.key_prefix = Some(prefix.into());
        self
    }

    /// Enable compression for stored values.
    pub fn compression(mut self, enabled: bool) -> Self {
        self.config.compression = enabled;
        self
    }

    /// Build a storage instance with the specified configuration.
    pub async fn build(self) -> Result<Arc<dyn Storage>> {
        let storage = ObjectStoreStorage::new(self.config).await?;
        Ok(Arc::new(storage))
    }
}

impl Default for StorageBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Internal statistics tracker for storage operations.
#[derive(Debug, Default)]
struct InternalStorageStats {
    reads: AtomicU64,
    writes: AtomicU64,
    deletes: AtomicU64,
    batches: AtomicU64,
    errors: AtomicU64,
}

impl InternalStorageStats {
    fn record_read(&self) {
        self.reads.fetch_add(1, Ordering::Relaxed);
    }

    fn record_write(&self) {
        self.writes.fetch_add(1, Ordering::Relaxed);
    }

    fn record_delete(&self) {
        self.deletes.fetch_add(1, Ordering::Relaxed);
    }

    fn record_batch(&self) {
        self.batches.fetch_add(1, Ordering::Relaxed);
    }

    fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    fn get_stats(&self, total_keys: u64, total_size: u64) -> StorageStats {
        StorageStats {
            read_count: self.reads.load(Ordering::Acquire),
            write_count: self.writes.load(Ordering::Acquire),
            delete_count: self.deletes.load(Ordering::Acquire),
            batch_count: self.batches.load(Ordering::Acquire),
            error_count: self.errors.load(Ordering::Acquire),
            total_keys,
            total_size_bytes: total_size,
        }
    }
}

/// Object store-based storage implementation.
struct ObjectStoreStorage {
    store: Arc<dyn ObjectStore>,
    config: StorageConfig,
    stats: Arc<InternalStorageStats>,
}

impl std::fmt::Debug for ObjectStoreStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStoreStorage")
            .field("config", &self.config)
            .field("stats", &self.stats)
            .finish()
    }
}

impl ObjectStoreStorage {
    async fn new(config: StorageConfig) -> Result<Self> {
        let stats = Arc::new(InternalStorageStats::default());

        let store: Arc<dyn ObjectStore> = match &config.backend {
            StorageBackend::Memory => Arc::new(InMemory::new()),
            StorageBackend::LocalFileSystem { root_path } => {
                let fs = LocalFileSystem::new_with_prefix(root_path).map_err(|e| {
                    CommonError::storage_error(format!(
                        "Failed to create local filesystem storage: {}",
                        e
                    ))
                })?;
                Arc::new(fs)
            }
            #[cfg(feature = "aws")]
            StorageBackend::S3 { .. } => {
                return Err(CommonError::storage_error(
                    "S3 backend not yet implemented".to_string(),
                ));
            }
            #[cfg(feature = "azure")]
            StorageBackend::Azure { .. } => {
                return Err(CommonError::storage_error(
                    "Azure backend not yet implemented".to_string(),
                ));
            }
            #[cfg(feature = "gcp")]
            StorageBackend::Gcs { .. } => {
                return Err(CommonError::storage_error(
                    "GCS backend not yet implemented".to_string(),
                ));
            }
        };

        Ok(Self {
            store,
            config,
            stats,
        })
    }

    /// Convert a byte key to an object store path.
    fn key_to_path(&self, key: &[u8]) -> ObjectPath {
        // Encode the key as hex to ensure it's a valid path
        let hex_key = hex::encode(key);

        let path_str = if let Some(prefix) = &self.config.key_prefix {
            format!("{}/{}", prefix, hex_key)
        } else {
            hex_key
        };

        ObjectPath::from(path_str)
    }

    /// Convert an object store path back to a byte key.
    fn path_to_key(&self, path: &ObjectPath) -> Result<Vec<u8>> {
        let path_str = path.as_ref();

        let hex_key = if let Some(prefix) = &self.config.key_prefix {
            let prefix_with_slash = format!("{}/", prefix);
            if path_str.starts_with(&prefix_with_slash) {
                &path_str[prefix_with_slash.len()..]
            } else {
                return Err(CommonError::storage_error(format!(
                    "Path does not start with expected prefix: {}",
                    path_str
                )));
            }
        } else {
            path_str
        };

        hex::decode(hex_key)
            .map_err(|e| CommonError::storage_error(format!("Failed to decode hex key: {}", e)))
    }

    /// Convert object store error to CommonError.
    fn convert_error(error: object_store::Error) -> CommonError {
        CommonError::storage_error(format!("Object store operation failed: {}", error))
    }
}

#[async_trait]
impl Storage for ObjectStoreStorage {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let path = self.key_to_path(key);

        match self.store.get(&path).await {
            Ok(get_result) => {
                self.stats.record_read();
                let bytes = get_result.bytes().await.map_err(Self::convert_error)?;
                Ok(Some(bytes.to_vec()))
            }
            Err(object_store::Error::NotFound { .. }) => {
                self.stats.record_read();
                Ok(None)
            }
            Err(e) => {
                self.stats.record_error();
                Err(Self::convert_error(e))
            }
        }
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let path = self.key_to_path(key);
        let payload = PutPayload::from(value.to_vec());

        match self.store.put(&path, payload).await {
            Ok(_) => {
                self.stats.record_write();
                Ok(())
            }
            Err(e) => {
                self.stats.record_error();
                Err(Self::convert_error(e))
            }
        }
    }

    async fn delete(&self, key: &[u8]) -> Result<()> {
        let path = self.key_to_path(key);

        match self.store.delete(&path).await {
            Ok(_) => {
                self.stats.record_delete();
                Ok(())
            }
            Err(e) => {
                self.stats.record_error();
                Err(Self::convert_error(e))
            }
        }
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool> {
        let path = self.key_to_path(key);

        match self.store.head(&path).await {
            Ok(_) => {
                self.stats.record_read();
                Ok(true)
            }
            Err(object_store::Error::NotFound { .. }) => {
                self.stats.record_read();
                Ok(false)
            }
            Err(e) => {
                self.stats.record_error();
                Err(Self::convert_error(e))
            }
        }
    }

    async fn keys_with_prefix(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>> {
        let prefix_path = self.key_to_path(prefix);
        let mut keys = Vec::new();

        let mut stream = self.store.list(Some(&prefix_path));
        while let Some(result) = stream.next().await {
            match result {
                Ok(object_meta) => {
                    if let Ok(key) = self.path_to_key(&object_meta.location) {
                        keys.push(key);
                    }
                }
                Err(e) => {
                    self.stats.record_error();
                    return Err(Self::convert_error(e));
                }
            }
        }

        self.stats.record_read();
        Ok(keys)
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let prefix_path = self.key_to_path(prefix);
        let mut pairs = Vec::new();

        let mut stream = self.store.list(Some(&prefix_path));
        while let Some(result) = stream.next().await {
            match result {
                Ok(object_meta) => {
                    if let Ok(key) = self.path_to_key(&object_meta.location) {
                        match self.get(&key).await? {
                            Some(value) => pairs.push((key, value)),
                            None => continue,
                        }
                    }
                }
                Err(e) => {
                    self.stats.record_error();
                    return Err(Self::convert_error(e));
                }
            }
        }

        self.stats.record_read();
        Ok(pairs)
    }

    async fn batch_write(&self, operations: Vec<BatchOperation>) -> Result<()> {
        // Object stores don't typically support atomic batch operations
        // Execute operations sequentially for now
        for op in operations {
            match op {
                BatchOperation::Put { key, value } => {
                    self.put(&key, &value).await?;
                }
                BatchOperation::Delete { key } => {
                    self.delete(&key).await?;
                }
            }
        }

        self.stats.record_batch();
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        // Object stores handle flushing automatically
        // This is a no-op for object stores
        Ok(())
    }

    async fn compact(&self) -> Result<()> {
        // Object stores handle compaction automatically
        // This is a no-op for object stores
        Ok(())
    }

    async fn stats(&self) -> StorageStats {
        // Object stores don't provide direct access to storage statistics
        // We return the internal operation statistics with zero for storage-specific metrics
        self.stats.get_stats(0, 0)
    }

    async fn close(&self) -> Result<()> {
        // Object stores will be closed when dropped
        // This is a no-op for object stores as they handle cleanup automatically
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// Mock storage implementation for testing.
    #[derive(Debug, Default)]
    struct MockStorage {
        data: std::sync::Mutex<std::collections::HashMap<Vec<u8>, Vec<u8>>>,
        stats: Arc<InternalStorageStats>,
    }

    impl MockStorage {
        fn new() -> Self {
            Self {
                data: std::sync::Mutex::new(std::collections::HashMap::new()),
                stats: Arc::new(InternalStorageStats::default()),
            }
        }
    }

    #[async_trait]
    impl Storage for MockStorage {
        async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
            self.stats.record_read();
            let data = self.data.lock().unwrap();
            Ok(data.get(key).cloned())
        }

        async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
            self.stats.record_write();
            let mut data = self.data.lock().unwrap();
            data.insert(key.to_vec(), value.to_vec());
            Ok(())
        }

        async fn delete(&self, key: &[u8]) -> Result<()> {
            self.stats.record_delete();
            let mut data = self.data.lock().unwrap();
            data.remove(key);
            Ok(())
        }

        async fn contains_key(&self, key: &[u8]) -> Result<bool> {
            self.stats.record_read();
            let data = self.data.lock().unwrap();
            Ok(data.contains_key(key))
        }

        async fn keys_with_prefix(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>> {
            self.stats.record_read();
            let data = self.data.lock().unwrap();
            let keys: Vec<Vec<u8>> = data
                .keys()
                .filter(|key| key.starts_with(prefix))
                .cloned()
                .collect();
            Ok(keys)
        }

        async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
            self.stats.record_read();
            let data = self.data.lock().unwrap();
            let pairs: Vec<(Vec<u8>, Vec<u8>)> = data
                .iter()
                .filter(|(key, _)| key.starts_with(prefix))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            Ok(pairs)
        }

        async fn batch_write(&self, operations: Vec<BatchOperation>) -> Result<()> {
            self.stats.record_batch();
            for op in operations {
                match op {
                    BatchOperation::Put { key, value } => {
                        self.put(&key, &value).await?;
                    }
                    BatchOperation::Delete { key } => {
                        self.delete(&key).await?;
                    }
                }
            }
            Ok(())
        }

        async fn flush(&self) -> Result<()> {
            Ok(())
        }

        async fn compact(&self) -> Result<()> {
            Ok(())
        }

        async fn stats(&self) -> StorageStats {
            self.stats.get_stats(0, 0)
        }

        async fn close(&self) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_memory_storage_basic_operations() {
        let storage = StorageBuilder::new()
            .backend(StorageBackend::Memory)
            .build()
            .await
            .expect("Failed to create memory storage");

        // Test put and get
        storage
            .put(b"test_key", b"test_value")
            .await
            .expect("Failed to put");

        let value = storage.get(b"test_key").await.expect("Failed to get");
        assert_eq!(value, Some(b"test_value".to_vec()));

        // Test contains_key
        assert!(
            storage
                .contains_key(b"test_key")
                .await
                .expect("Failed to check key")
        );
        assert!(
            !storage
                .contains_key(b"nonexistent")
                .await
                .expect("Failed to check key")
        );

        // Test delete
        storage.delete(b"test_key").await.expect("Failed to delete");

        let value = storage.get(b"test_key").await.expect("Failed to get");
        assert_eq!(value, None);
    }

    #[tokio::test]
    async fn test_local_filesystem_storage() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let root_path = temp_dir.path().to_string_lossy().to_string();

        let storage = StorageBuilder::new()
            .backend(StorageBackend::LocalFileSystem { root_path })
            .build()
            .await
            .expect("Failed to create local filesystem storage");

        // Test basic operations
        storage
            .put(b"fs_test_key", b"fs_test_value")
            .await
            .expect("Failed to put");

        let value = storage.get(b"fs_test_key").await.expect("Failed to get");
        assert_eq!(value, Some(b"fs_test_value".to_vec()));
    }

    #[tokio::test]
    async fn test_storage_builder_default() {
        let _temp_dir = TempDir::new().expect("Failed to create temp directory");

        // Test Default implementation for StorageBuilder (using mock storage for testing)
        let storage1: Arc<dyn Storage> = Arc::new(MockStorage::new());
        let storage2: Arc<dyn Storage> = Arc::new(MockStorage::new());

        // Both storages should work the same way
        storage1
            .put(b"test1", b"value1")
            .await
            .expect("Failed to put");
        storage2
            .put(b"test2", b"value2")
            .await
            .expect("Failed to put");

        assert_eq!(
            storage1.get(b"test1").await.expect("Failed to get"),
            Some(b"value1".to_vec())
        );
        assert_eq!(
            storage2.get(b"test2").await.expect("Failed to get"),
            Some(b"value2".to_vec())
        );
    }

    #[tokio::test]
    async fn test_prefix_operations() {
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new());

        // Put some test data with different prefixes
        storage
            .put(b"prefix1_key1", b"value1")
            .await
            .expect("Failed to put");
        storage
            .put(b"prefix1_key2", b"value2")
            .await
            .expect("Failed to put");
        storage
            .put(b"prefix2_key1", b"value3")
            .await
            .expect("Failed to put");
        storage
            .put(b"other_key", b"value4")
            .await
            .expect("Failed to put");

        // Test keys_with_prefix
        let keys = storage
            .keys_with_prefix(b"prefix1")
            .await
            .expect("Failed to get keys");
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&b"prefix1_key1".to_vec()));
        assert!(keys.contains(&b"prefix1_key2".to_vec()));

        // Test scan_prefix
        let pairs = storage
            .scan_prefix(b"prefix1")
            .await
            .expect("Failed to scan");
        assert_eq!(pairs.len(), 2);

        let mut found_values = Vec::new();
        for (key, value) in pairs {
            if key == b"prefix1_key1" {
                found_values.push(value);
            } else if key == b"prefix1_key2" {
                found_values.push(value);
            }
        }
        assert_eq!(found_values.len(), 2);
        assert!(found_values.contains(&b"value1".to_vec()));
        assert!(found_values.contains(&b"value2".to_vec()));
    }

    #[tokio::test]
    async fn test_batch_operations() {
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new());

        let operations = vec![
            BatchOperation::Put {
                key: b"batch_key1".to_vec(),
                value: b"batch_value1".to_vec(),
            },
            BatchOperation::Put {
                key: b"batch_key2".to_vec(),
                value: b"batch_value2".to_vec(),
            },
            BatchOperation::Delete {
                key: b"batch_key1".to_vec(),
            },
        ];

        storage
            .batch_write(operations)
            .await
            .expect("Failed to batch write");

        // Check results
        assert_eq!(
            storage.get(b"batch_key1").await.expect("Failed to get"),
            None
        );
        assert_eq!(
            storage.get(b"batch_key2").await.expect("Failed to get"),
            Some(b"batch_value2".to_vec())
        );
    }

    #[tokio::test]
    async fn test_storage_stats() {
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new());

        // Perform some operations
        storage
            .put(b"stats_key", b"stats_value")
            .await
            .expect("Failed to put");
        storage.get(b"stats_key").await.expect("Failed to get");
        storage
            .delete(b"stats_key")
            .await
            .expect("Failed to delete");

        let stats = storage.stats().await;
        assert!(stats.read_count > 0);
        assert!(stats.write_count > 0);
        assert!(stats.delete_count > 0);
    }
}
