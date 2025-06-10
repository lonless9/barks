//! Non-volatile storage abstraction using trait-based design.
//!
//! This module provides a generic storage interface that abstracts over
//! the underlying storage implementation (RocksDB).

use async_trait::async_trait;
use std::fmt::Debug;
use std::path::Path;
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

/// Storage statistics information.
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

impl StorageStats {
    /// Calculate read/write ratio.
    pub fn read_write_ratio(&self) -> f64 {
        if self.write_count == 0 {
            if self.read_count == 0 {
                0.0
            } else {
                f64::INFINITY
            }
        } else {
            self.read_count as f64 / self.write_count as f64
        }
    }

    /// Calculate error rate.
    pub fn error_rate(&self) -> f64 {
        let total_ops = self.read_count + self.write_count + self.delete_count + self.batch_count;
        if total_ops == 0 {
            0.0
        } else {
            self.error_count as f64 / total_ops as f64
        }
    }
}

/// Configuration for storage creation.
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// Path to the storage directory.
    pub path: String,
    /// Create the database if it doesn't exist.
    pub create_if_missing: bool,
    /// Maximum number of open files.
    pub max_open_files: Option<i32>,
    /// Write buffer size in bytes.
    pub write_buffer_size: Option<usize>,
    /// Block cache size in bytes.
    pub block_cache_size: Option<usize>,
    /// Enable compression.
    pub compression: bool,
    /// Sync writes to disk.
    pub sync_writes: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            path: "./storage".to_string(),
            create_if_missing: true,
            max_open_files: Some(1000),
            write_buffer_size: Some(64 * 1024 * 1024), // 64MB
            block_cache_size: Some(256 * 1024 * 1024), // 256MB
            compression: true,
            sync_writes: false,
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

    /// Set the storage path.
    pub fn path<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.config.path = path.as_ref().to_string_lossy().to_string();
        self
    }

    /// Set whether to create the database if it doesn't exist.
    pub fn create_if_missing(mut self, create: bool) -> Self {
        self.config.create_if_missing = create;
        self
    }

    /// Set the maximum number of open files.
    pub fn max_open_files(mut self, max_files: i32) -> Self {
        self.config.max_open_files = Some(max_files);
        self
    }

    /// Set the write buffer size.
    pub fn write_buffer_size(mut self, size: usize) -> Self {
        self.config.write_buffer_size = Some(size);
        self
    }

    /// Set the block cache size.
    pub fn block_cache_size(mut self, size: usize) -> Self {
        self.config.block_cache_size = Some(size);
        self
    }

    /// Enable or disable compression.
    pub fn compression(mut self, enable: bool) -> Self {
        self.config.compression = enable;
        self
    }

    /// Enable or disable sync writes.
    pub fn sync_writes(mut self, sync: bool) -> Self {
        self.config.sync_writes = sync;
        self
    }

    /// Build a storage instance with the specified configuration.
    pub async fn build(self) -> Result<Arc<dyn Storage>> {
        let storage = RocksDbStorage::new(self.config).await?;
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
        self.reads.fetch_add(1, Ordering::AcqRel);
    }

    fn record_write(&self) {
        self.writes.fetch_add(1, Ordering::AcqRel);
    }

    fn record_delete(&self) {
        self.deletes.fetch_add(1, Ordering::AcqRel);
    }

    fn record_batch(&self) {
        self.batches.fetch_add(1, Ordering::AcqRel);
    }

    fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::AcqRel);
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

/// RocksDB-based storage implementation.
#[derive(Debug)]
struct RocksDbStorage {
    db: Arc<rocksdb::DB>,
    stats: Arc<InternalStorageStats>,
}

impl RocksDbStorage {
    async fn new(config: StorageConfig) -> Result<Self> {
        let stats = Arc::new(InternalStorageStats::default());

        // Configure RocksDB options
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(config.create_if_missing);

        if let Some(max_files) = config.max_open_files {
            opts.set_max_open_files(max_files);
        }

        if let Some(buffer_size) = config.write_buffer_size {
            opts.set_write_buffer_size(buffer_size);
        }

        if config.compression {
            opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        } else {
            opts.set_compression_type(rocksdb::DBCompressionType::None);
        }

        // Set block-based table options for cache
        if let Some(cache_size) = config.block_cache_size {
            let mut block_opts = rocksdb::BlockBasedOptions::default();
            let cache = rocksdb::Cache::new_lru_cache(cache_size);
            block_opts.set_block_cache(&cache);
            opts.set_block_based_table_factory(&block_opts);
        }

        // Open the database
        let db = tokio::task::spawn_blocking(move || rocksdb::DB::open(&opts, &config.path))
            .await
            .map_err(|e| {
                CommonError::storage_error(format!("Failed to spawn blocking task: {}", e))
            })?
            .map_err(|e| CommonError::storage_error(format!("Failed to open RocksDB: {}", e)))?;

        Ok(Self {
            db: Arc::new(db),
            stats,
        })
    }

    async fn execute_blocking<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&rocksdb::DB) -> std::result::Result<R, rocksdb::Error> + Send + 'static,
        R: Send + 'static,
    {
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || f(&db))
            .await
            .map_err(|e| CommonError::storage_error(format!("Blocking task failed: {}", e)))?
            .map_err(|e| CommonError::storage_error(format!("RocksDB operation failed: {}", e)))
    }
}

#[async_trait]
impl Storage for RocksDbStorage {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let result = self
            .execute_blocking({
                let key = key.to_vec();
                move |db| db.get(&key)
            })
            .await;

        match result {
            Ok(value) => {
                self.stats.record_read();
                Ok(value)
            }
            Err(e) => {
                self.stats.record_error();
                Err(e)
            }
        }
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let result = self
            .execute_blocking({
                let key = key.to_vec();
                let value = value.to_vec();
                move |db| db.put(&key, &value)
            })
            .await;

        match result {
            Ok(_) => {
                self.stats.record_write();
                Ok(())
            }
            Err(e) => {
                self.stats.record_error();
                Err(e)
            }
        }
    }

    async fn delete(&self, key: &[u8]) -> Result<()> {
        let result = self
            .execute_blocking({
                let key = key.to_vec();
                move |db| db.delete(&key)
            })
            .await;

        match result {
            Ok(_) => {
                self.stats.record_delete();
                Ok(())
            }
            Err(e) => {
                self.stats.record_error();
                Err(e)
            }
        }
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool> {
        let result = self
            .execute_blocking({
                let key = key.to_vec();
                move |db| db.get(&key).map(|opt| opt.is_some())
            })
            .await;

        match result {
            Ok(exists) => {
                self.stats.record_read();
                Ok(exists)
            }
            Err(e) => {
                self.stats.record_error();
                Err(e)
            }
        }
    }

    async fn keys_with_prefix(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>> {
        let result = self
            .execute_blocking({
                let prefix = prefix.to_vec();
                move |db| {
                    let iter = db.prefix_iterator(&prefix);
                    let mut keys = Vec::new();
                    for item in iter {
                        match item {
                            Ok((key, _)) => {
                                if key.starts_with(&prefix) {
                                    keys.push(key.to_vec());
                                } else {
                                    break;
                                }
                            }
                            Err(e) => return Err(e),
                        }
                    }
                    Ok(keys)
                }
            })
            .await;

        match result {
            Ok(keys) => {
                self.stats.record_read();
                Ok(keys)
            }
            Err(e) => {
                self.stats.record_error();
                Err(e)
            }
        }
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let result = self
            .execute_blocking({
                let prefix = prefix.to_vec();
                move |db| {
                    let iter = db.prefix_iterator(&prefix);
                    let mut pairs = Vec::new();
                    for item in iter {
                        match item {
                            Ok((key, value)) => {
                                if key.starts_with(&prefix) {
                                    pairs.push((key.to_vec(), value.to_vec()));
                                } else {
                                    break;
                                }
                            }
                            Err(e) => return Err(e),
                        }
                    }
                    Ok(pairs)
                }
            })
            .await;

        match result {
            Ok(pairs) => {
                self.stats.record_read();
                Ok(pairs)
            }
            Err(e) => {
                self.stats.record_error();
                Err(e)
            }
        }
    }

    async fn batch_write(&self, operations: Vec<BatchOperation>) -> Result<()> {
        let result = self
            .execute_blocking(move |db| {
                let mut batch = rocksdb::WriteBatch::default();
                for op in operations {
                    match op {
                        BatchOperation::Put { key, value } => {
                            batch.put(&key, &value);
                        }
                        BatchOperation::Delete { key } => {
                            batch.delete(&key);
                        }
                    }
                }
                db.write(batch)
            })
            .await;

        match result {
            Ok(_) => {
                self.stats.record_batch();
                Ok(())
            }
            Err(e) => {
                self.stats.record_error();
                Err(e)
            }
        }
    }

    async fn flush(&self) -> Result<()> {
        let result = self
            .execute_blocking(move |db| {
                let mut flush_opts = rocksdb::FlushOptions::default();
                flush_opts.set_wait(true);
                db.flush_opt(&flush_opts)
            })
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(e) => {
                self.stats.record_error();
                Err(e)
            }
        }
    }

    async fn compact(&self) -> Result<()> {
        let result = self
            .execute_blocking(move |db| {
                db.compact_range::<&[u8], &[u8]>(None, None);
                Ok(())
            })
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(e) => {
                self.stats.record_error();
                Err(e)
            }
        }
    }

    async fn stats(&self) -> StorageStats {
        // Get approximate key count and size from RocksDB properties
        let (total_keys, total_size) = self
            .execute_blocking(move |db| {
                let keys = db
                    .property_value(rocksdb::properties::ESTIMATE_NUM_KEYS)
                    .unwrap_or_default()
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(0);

                let size = db
                    .property_value(rocksdb::properties::ESTIMATE_LIVE_DATA_SIZE)
                    .unwrap_or_default()
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(0);

                Ok((keys, size))
            })
            .await
            .unwrap_or((0, 0));

        self.stats.get_stats(total_keys, total_size)
    }

    async fn close(&self) -> Result<()> {
        // RocksDB will be closed when the Arc is dropped
        // This is a no-op for RocksDB as it handles cleanup automatically
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn create_test_storage() -> (Arc<dyn Storage>, TempDir) {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let storage = StorageBuilder::new()
            .path(temp_dir.path())
            .create_if_missing(true)
            .build()
            .await
            .expect("Failed to create storage");
        (storage, temp_dir)
    }

    #[tokio::test]
    async fn test_storage_basic_operations() {
        let (storage, _temp_dir) = create_test_storage().await;

        // Test put and get
        let key = b"test_key";
        let value = b"test_value";

        storage.put(key, value).await.expect("Failed to put value");
        let retrieved = storage.get(key).await.expect("Failed to get value");
        assert_eq!(retrieved, Some(value.to_vec()));

        // Test contains_key
        assert!(
            storage
                .contains_key(key)
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
        storage.delete(key).await.expect("Failed to delete key");
        let retrieved = storage
            .get(key)
            .await
            .expect("Failed to get value after delete");
        assert_eq!(retrieved, None);
        assert!(
            !storage
                .contains_key(key)
                .await
                .expect("Failed to check key after delete")
        );
    }

    #[tokio::test]
    async fn test_storage_prefix_operations() {
        let (storage, _temp_dir) = create_test_storage().await;

        // Insert test data with common prefix
        let prefix = b"prefix:";
        let test_data = vec![
            (b"prefix:key1".to_vec(), b"value1".to_vec()),
            (b"prefix:key2".to_vec(), b"value2".to_vec()),
            (b"prefix:key3".to_vec(), b"value3".to_vec()),
            (b"other:key".to_vec(), b"other_value".to_vec()),
        ];

        for (key, value) in &test_data {
            storage.put(key, value).await.expect("Failed to put value");
        }

        // Test keys_with_prefix
        let keys = storage
            .keys_with_prefix(prefix)
            .await
            .expect("Failed to get keys with prefix");
        assert_eq!(keys.len(), 3);
        assert!(keys.contains(&b"prefix:key1".to_vec()));
        assert!(keys.contains(&b"prefix:key2".to_vec()));
        assert!(keys.contains(&b"prefix:key3".to_vec()));

        // Test scan_prefix
        let pairs = storage
            .scan_prefix(prefix)
            .await
            .expect("Failed to scan prefix");
        assert_eq!(pairs.len(), 3);

        let mut found_pairs = std::collections::HashSet::new();
        for (key, value) in pairs {
            found_pairs.insert((key, value));
        }

        assert!(found_pairs.contains(&(b"prefix:key1".to_vec(), b"value1".to_vec())));
        assert!(found_pairs.contains(&(b"prefix:key2".to_vec(), b"value2".to_vec())));
        assert!(found_pairs.contains(&(b"prefix:key3".to_vec(), b"value3".to_vec())));
    }

    #[tokio::test]
    async fn test_storage_batch_operations() {
        let (storage, _temp_dir) = create_test_storage().await;

        // Prepare batch operations
        let operations = vec![
            BatchOperation::Put {
                key: b"batch_key1".to_vec(),
                value: b"batch_value1".to_vec(),
            },
            BatchOperation::Put {
                key: b"batch_key2".to_vec(),
                value: b"batch_value2".to_vec(),
            },
            BatchOperation::Put {
                key: b"batch_key3".to_vec(),
                value: b"batch_value3".to_vec(),
            },
        ];

        // Execute batch write
        storage
            .batch_write(operations)
            .await
            .expect("Failed to execute batch write");

        // Verify all values were written
        assert_eq!(
            storage
                .get(b"batch_key1")
                .await
                .expect("Failed to get value"),
            Some(b"batch_value1".to_vec())
        );
        assert_eq!(
            storage
                .get(b"batch_key2")
                .await
                .expect("Failed to get value"),
            Some(b"batch_value2".to_vec())
        );
        assert_eq!(
            storage
                .get(b"batch_key3")
                .await
                .expect("Failed to get value"),
            Some(b"batch_value3".to_vec())
        );

        // Test batch with mixed operations
        let mixed_operations = vec![
            BatchOperation::Put {
                key: b"new_key".to_vec(),
                value: b"new_value".to_vec(),
            },
            BatchOperation::Delete {
                key: b"batch_key1".to_vec(),
            },
        ];

        storage
            .batch_write(mixed_operations)
            .await
            .expect("Failed to execute mixed batch");

        // Verify results
        assert_eq!(
            storage.get(b"new_key").await.expect("Failed to get value"),
            Some(b"new_value".to_vec())
        );
        assert_eq!(
            storage
                .get(b"batch_key1")
                .await
                .expect("Failed to get value"),
            None
        );
    }

    #[tokio::test]
    async fn test_storage_stats() {
        let (storage, _temp_dir) = create_test_storage().await;

        let initial_stats = storage.stats().await;
        assert_eq!(initial_stats.read_count, 0);
        assert_eq!(initial_stats.write_count, 0);
        assert_eq!(initial_stats.delete_count, 0);

        // Perform some operations
        storage
            .put(b"key1", b"value1")
            .await
            .expect("Failed to put");
        storage
            .put(b"key2", b"value2")
            .await
            .expect("Failed to put");
        storage.get(b"key1").await.expect("Failed to get");
        storage.get(b"nonexistent").await.expect("Failed to get");
        storage.delete(b"key1").await.expect("Failed to delete");

        let stats = storage.stats().await;
        assert_eq!(stats.write_count, 2); // 2 puts
        assert_eq!(stats.read_count, 2); // 2 gets
        assert_eq!(stats.delete_count, 1); // 1 delete
        assert_eq!(stats.error_count, 0); // No errors

        // Test statistics calculations
        assert_eq!(stats.read_write_ratio(), 1.0); // 2 reads / 2 writes
        assert_eq!(stats.error_rate(), 0.0); // 0 errors / 5 total ops
    }

    #[tokio::test]
    async fn test_storage_flush_and_compact() {
        let (storage, _temp_dir) = create_test_storage().await;

        // Add some data
        for i in 0..10 {
            let key = format!("key_{}", i);
            let value = format!("value_{}", i);
            storage
                .put(key.as_bytes(), value.as_bytes())
                .await
                .expect("Failed to put");
        }

        // Test flush
        storage.flush().await.expect("Failed to flush");

        // Test compact
        storage.compact().await.expect("Failed to compact");

        // Verify data is still accessible after flush and compact
        for i in 0..10 {
            let key = format!("key_{}", i);
            let expected_value = format!("value_{}", i);
            let actual_value = storage.get(key.as_bytes()).await.expect("Failed to get");
            assert_eq!(actual_value, Some(expected_value.into_bytes()));
        }
    }

    #[tokio::test]
    async fn test_storage_builder_configuration() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");

        let storage = StorageBuilder::new()
            .path(temp_dir.path())
            .create_if_missing(true)
            .max_open_files(500)
            .write_buffer_size(32 * 1024 * 1024) // 32MB
            .block_cache_size(128 * 1024 * 1024) // 128MB
            .compression(false)
            .sync_writes(true)
            .build()
            .await
            .expect("Failed to create configured storage");

        // Test that the storage works with custom configuration
        storage.put(b"test", b"value").await.expect("Failed to put");
        let value = storage.get(b"test").await.expect("Failed to get");
        assert_eq!(value, Some(b"value".to_vec()));
    }

    #[tokio::test]
    async fn test_storage_close() {
        let (storage, _temp_dir) = create_test_storage().await;

        // Add some data
        storage.put(b"key", b"value").await.expect("Failed to put");

        // Close storage
        storage.close().await.expect("Failed to close storage");

        // Storage should still be accessible (RocksDB handles cleanup automatically)
        let value = storage
            .get(b"key")
            .await
            .expect("Failed to get after close");
        assert_eq!(value, Some(b"value".to_vec()));
    }

    #[tokio::test]
    async fn test_storage_builder_default() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");

        // Test Default implementation for StorageBuilder
        let storage1 = StorageBuilder::default()
            .path(temp_dir.path())
            .create_if_missing(true)
            .build()
            .await
            .expect("Failed to create storage from default");

        let storage2 = StorageBuilder::new()
            .path(temp_dir.path().join("storage2"))
            .create_if_missing(true)
            .build()
            .await
            .expect("Failed to create storage from new");

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
    async fn test_storage_stats_read_write_ratio() {
        let (storage, _temp_dir) = create_test_storage().await;

        // Test initial stats
        let initial_stats = storage.stats().await;

        // Test read_write_ratio when both counts are 0
        assert_eq!(initial_stats.read_write_ratio(), 0.0);

        // Add some data to trigger writes
        for i in 0..5 {
            let key = format!("key_{}", i);
            let value = format!("value_{}", i);
            storage
                .put(key.as_bytes(), value.as_bytes())
                .await
                .expect("Failed to put");
        }

        // Read some data
        for i in 0..3 {
            let key = format!("key_{}", i);
            let _value = storage.get(key.as_bytes()).await.expect("Failed to get");
        }

        let stats = storage.stats().await;

        // Should have some reads and writes now
        assert!(stats.read_count > 0);
        assert!(stats.write_count > 0);

        // Test ratio calculation
        let ratio = stats.read_write_ratio();
        assert!(ratio > 0.0);
        assert!(ratio.is_finite());
    }

    #[tokio::test]
    async fn test_storage_stats_infinity_ratio() {
        let (_storage, _temp_dir) = create_test_storage().await;

        // Test the specific case where read_count > 0 and write_count == 0
        // This is tricky to achieve in practice, so we'll test the logic directly
        let stats = StorageStats {
            read_count: 10,
            write_count: 0,
            delete_count: 0,
            batch_count: 0,
            error_count: 0,
            total_keys: 0,
            total_size_bytes: 0,
        };

        // Should return infinity when write_count is 0 but read_count > 0
        assert_eq!(stats.read_write_ratio(), f64::INFINITY);

        // Test the case where both are 0
        let zero_stats = StorageStats {
            read_count: 0,
            write_count: 0,
            delete_count: 0,
            batch_count: 0,
            error_count: 0,
            total_keys: 0,
            total_size_bytes: 0,
        };

        assert_eq!(zero_stats.read_write_ratio(), 0.0);

        // Test normal ratio calculation
        let normal_stats = StorageStats {
            read_count: 6,
            write_count: 3,
            delete_count: 0,
            batch_count: 0,
            error_count: 0,
            total_keys: 0,
            total_size_bytes: 0,
        };

        assert_eq!(normal_stats.read_write_ratio(), 2.0);
    }
}
