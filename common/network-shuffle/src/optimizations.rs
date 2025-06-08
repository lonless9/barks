//! Shuffle optimizations including compression, spill management, and sort-based shuffle.

use crate::traits::*;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;

/// Compression algorithms supported for shuffle data
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionCodec {
    None,
    // Future: Add compression support when dependencies are available
    // Lz4,
    // Snappy,
    // Gzip,
}

impl CompressionCodec {
    /// Compress data using the specified codec
    pub fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        match self {
            CompressionCodec::None => Ok(data.to_vec()),
            // Future compression implementations would go here
        }
    }

    /// Decompress data using the specified codec
    pub fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        match self {
            CompressionCodec::None => Ok(data.to_vec()),
            // Future decompression implementations would go here
        }
    }
}

/// Configuration for shuffle optimizations
#[derive(Debug, Clone)]
pub struct ShuffleConfig {
    /// Compression codec to use
    pub compression: CompressionCodec,
    /// Maximum memory to use before spilling to disk (in bytes)
    pub spill_threshold: usize,
    /// Whether to use sort-based shuffle
    pub sort_based_shuffle: bool,
    /// Buffer size for I/O operations
    pub io_buffer_size: usize,
}

impl Default for ShuffleConfig {
    fn default() -> Self {
        Self {
            compression: CompressionCodec::None,
            spill_threshold: 64 * 1024 * 1024, // 64MB
            sort_based_shuffle: true,
            io_buffer_size: 64 * 1024, // 64KB
        }
    }
}

/// Enhanced shuffle block manager with compression and spill support
pub struct OptimizedShuffleBlockManager {
    root_dir: PathBuf,
    config: ShuffleConfig,
    memory_usage: Arc<RwLock<usize>>,
    spill_files: Arc<RwLock<HashMap<ShuffleBlockId, PathBuf>>>,
}

impl OptimizedShuffleBlockManager {
    pub fn new<P: AsRef<Path>>(root_dir: P, config: ShuffleConfig) -> Result<Self> {
        let path = root_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&path)?;
        Ok(Self {
            root_dir: path,
            config,
            memory_usage: Arc::new(RwLock::new(0)),
            spill_files: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    fn get_block_path(&self, block_id: &ShuffleBlockId) -> PathBuf {
        self.root_dir
            .join(block_id.shuffle_id.to_string())
            .join(format!("{}_{}", block_id.map_id, block_id.reduce_id))
    }

    async fn should_spill(&self, additional_size: usize) -> bool {
        let current_usage = *self.memory_usage.read().await;
        current_usage + additional_size > self.config.spill_threshold
    }

    async fn spill_to_disk(&self, block_id: ShuffleBlockId, data: Vec<u8>) -> Result<()> {
        let path = self.get_block_path(&block_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }

        // Compress data if configured
        let compressed_data = self.config.compression.compress(&data)?;

        let mut file = File::create(&path).await?;
        file.write_all(&compressed_data).await?;

        // Record spill file location
        self.spill_files.write().await.insert(block_id, path);

        Ok(())
    }
}

#[async_trait]
impl ShuffleBlockManager for OptimizedShuffleBlockManager {
    async fn get_block(&self, block_id: &ShuffleBlockId) -> Result<Vec<u8>> {
        // Check if block is spilled to disk
        if let Some(path) = self.spill_files.read().await.get(block_id) {
            let compressed_data = fs::read(path).await?;
            return self.config.compression.decompress(&compressed_data);
        }

        // If not spilled, read from regular storage
        let path = self.get_block_path(block_id);
        let compressed_data = fs::read(&path)
            .await
            .map_err(|e| anyhow!("Failed to read block {:?} from {:?}: {}", block_id, path, e))?;

        self.config.compression.decompress(&compressed_data)
    }

    async fn put_block(&self, block_id: ShuffleBlockId, data: Vec<u8>) -> Result<()> {
        // Check if we should spill to disk
        if self.should_spill(data.len()).await {
            self.spill_to_disk(block_id, data).await
        } else {
            // Store in regular location with compression
            let path = self.get_block_path(&block_id);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).await?;
            }

            let compressed_data = self.config.compression.compress(&data)?;
            let mut file = File::create(&path).await?;
            file.write_all(&compressed_data).await?;

            // Update memory usage
            *self.memory_usage.write().await += data.len();

            Ok(())
        }
    }

    async fn remove_block(&self, block_id: &ShuffleBlockId) -> Result<()> {
        // Remove from spill files if present
        if let Some(path) = self.spill_files.write().await.remove(block_id) {
            if fs::try_exists(&path).await? {
                fs::remove_file(&path).await?;
            }
        }

        // Remove from regular storage
        let path = self.get_block_path(block_id);
        if fs::try_exists(&path).await? {
            fs::remove_file(&path).await?;
        }

        Ok(())
    }

    async fn contains_block(&self, block_id: &ShuffleBlockId) -> Result<bool> {
        // Check spill files first
        if self.spill_files.read().await.contains_key(block_id) {
            return Ok(true);
        }

        // Check regular storage
        let path = self.get_block_path(block_id);
        Ok(fs::try_exists(&path).await?)
    }

    async fn get_block_size(&self, block_id: &ShuffleBlockId) -> Result<u64> {
        // Check spill files first
        if let Some(path) = self.spill_files.read().await.get(block_id) {
            return Ok(fs::metadata(path).await?.len());
        }

        // Check regular storage
        let path = self.get_block_path(block_id);
        Ok(fs::metadata(&path).await?.len())
    }
}

/// Hash-based shuffle writer that sorts data before writing to improve read performance
pub struct HashShuffleWriter<K, V> {
    shuffle_id: u32,
    map_id: u32,
    partitioner: Arc<dyn crate::traits::Partitioner<K>>,
    block_manager: Arc<dyn ShuffleBlockManager>,
    config: ShuffleConfig,
    // Buffer for each reduce partition: reduce_id -> Vec<(K, V)>
    buffers: HashMap<u32, Vec<(K, V)>>,
    memory_usage: usize,
}

impl<K, V> HashShuffleWriter<K, V>
where
    K: serde::Serialize + Send + Sync + Clone + bincode::Encode + std::hash::Hash + Ord,
    V: serde::Serialize + Send + Sync + Clone + bincode::Encode,
{
    pub fn new(
        shuffle_id: u32,
        map_id: u32,
        partitioner: Arc<dyn crate::traits::Partitioner<K>>,
        block_manager: Arc<dyn ShuffleBlockManager>,
        config: ShuffleConfig,
    ) -> Self {
        Self {
            shuffle_id,
            map_id,
            partitioner,
            block_manager,
            config,
            buffers: HashMap::new(),
            memory_usage: 0,
        }
    }

    /// Write a key-value pair with automatic sorting and spilling
    pub async fn write(&mut self, record: (K, V)) -> Result<()> {
        let (key, value) = record;

        let reduce_id = self.partitioner.get_partition(&key);

        // Add to buffer
        let buffer = self.buffers.entry(reduce_id).or_default();
        buffer.push((key, value));

        // Estimate memory usage (rough approximation)
        self.memory_usage += std::mem::size_of::<(K, V)>();

        // Check if we need to spill
        if self.memory_usage > self.config.spill_threshold {
            self.spill_buffers().await?;
        }

        Ok(())
    }

    async fn spill_buffers(&mut self) -> Result<()> {
        for (reduce_id, buffer) in self.buffers.iter_mut() {
            if !buffer.is_empty() {
                // Sort the buffer by key for better read performance
                if self.config.sort_based_shuffle {
                    buffer.sort_by(|a, b| a.0.cmp(&b.0));
                }

                // Serialize and write to block manager
                let serialized = bincode::encode_to_vec(&*buffer, bincode::config::standard())
                    .map_err(|e| anyhow!("Failed to serialize shuffle buffer: {}", e))?;

                let block_id = ShuffleBlockId {
                    shuffle_id: self.shuffle_id,
                    map_id: self.map_id,
                    reduce_id: *reduce_id,
                };

                self.block_manager.put_block(block_id, serialized).await?;
                buffer.clear();
            }
        }

        self.memory_usage = 0;
        Ok(())
    }

    pub async fn close(&mut self) -> Result<crate::traits::MapStatus> {
        // Flush any remaining buffers
        self.spill_buffers().await?;

        // Return block sizes for MapStatus
        let mut block_sizes = HashMap::new();
        for reduce_id in 0..self.partitioner.num_partitions() {
            let block_id = ShuffleBlockId {
                shuffle_id: self.shuffle_id,
                map_id: self.map_id,
                reduce_id,
            };

            if self.block_manager.contains_block(&block_id).await? {
                let size = self.block_manager.get_block_size(&block_id).await?;
                block_sizes.insert(reduce_id, size);
            }
        }

        Ok(crate::traits::MapStatus::new(block_sizes))
    }
}
