//! Shuffle optimizations including compression, spill management, and sort-based shuffle.

use crate::traits::*;
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt;

/// Compression algorithms supported for shuffle data
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionCodec {
    None,
    #[cfg(feature = "compression")]
    Lz4,
    // Future: Add more compression support when dependencies are available
    // Snappy,
    // Gzip,
}

impl CompressionCodec {
    /// Compress data using the specified codec
    pub fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        match self {
            CompressionCodec::None => Ok(data.to_vec()),
            #[cfg(feature = "compression")]
            CompressionCodec::Lz4 => {
                let compressed = lz4_flex::compress_prepend_size(data);
                Ok(compressed)
            }
        }
    }

    /// Decompress data using the specified codec
    pub fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        match self {
            CompressionCodec::None => Ok(data.to_vec()),
            #[cfg(feature = "compression")]
            CompressionCodec::Lz4 => {
                let decompressed = lz4_flex::decompress_size_prepended(data)
                    .map_err(|e| anyhow!("LZ4 decompression failed: {}", e))?;
                Ok(decompressed)
            }
        }
    }

    /// Get the compression ratio estimate for this codec
    pub fn estimated_compression_ratio(&self) -> f32 {
        match self {
            CompressionCodec::None => 1.0,
            #[cfg(feature = "compression")]
            CompressionCodec::Lz4 => 0.6, // LZ4 typically achieves ~40% compression
        }
    }

    /// Check if this codec is available (dependencies compiled in)
    pub fn is_available(&self) -> bool {
        match self {
            CompressionCodec::None => true,
            #[cfg(feature = "compression")]
            CompressionCodec::Lz4 => true,
        }
    }
}

/// Configuration for shuffle optimizations
#[derive(Debug, Clone, Serialize, Deserialize)]
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
            #[cfg(feature = "compression")]
            compression: CompressionCodec::Lz4,
            #[cfg(not(feature = "compression"))]
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
}

impl OptimizedShuffleBlockManager {
    pub fn new<P: AsRef<Path>>(root_dir: P, config: ShuffleConfig) -> Result<Self> {
        let path = root_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&path)?;
        Ok(Self {
            root_dir: path,
            config,
        })
    }

    fn get_block_path(&self, block_id: &ShuffleBlockId) -> PathBuf {
        self.root_dir
            .join(block_id.shuffle_id.to_string())
            .join(format!("{}_{}", block_id.map_id, block_id.reduce_id))
    }
}

#[async_trait]
impl ShuffleBlockManager for OptimizedShuffleBlockManager {
    async fn get_block(&self, block_id: &ShuffleBlockId) -> Result<Vec<u8>> {
        let path = self.get_block_path(block_id);
        let compressed_data = fs::read(&path)
            .await
            .map_err(|e| anyhow!("Failed to read block {:?} from {:?}: {}", block_id, path, e))?;

        self.config.compression.decompress(&compressed_data)
    }

    async fn put_block(&self, block_id: ShuffleBlockId, data: Vec<u8>) -> Result<()> {
        let path = self.get_block_path(&block_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let compressed_data = self.config.compression.compress(&data)?;
        let mut file = File::create(&path).await?;
        file.write_all(&compressed_data).await?;

        Ok(())
    }

    async fn remove_block(&self, block_id: &ShuffleBlockId) -> Result<()> {
        // Remove from regular storage
        let path = self.get_block_path(block_id);
        if fs::try_exists(&path).await? {
            fs::remove_file(&path).await?;
        }

        Ok(())
    }

    async fn contains_block(&self, block_id: &ShuffleBlockId) -> Result<bool> {
        // Check regular storage
        let path = self.get_block_path(block_id);
        Ok(fs::try_exists(&path).await?)
    }

    async fn get_block_size(&self, block_id: &ShuffleBlockId) -> Result<u64> {
        // Check regular storage
        let path = self.get_block_path(block_id);
        Ok(fs::metadata(&path).await?.len())
    }

    /// Removes all files and directories associated with a given shuffle_id.
    /// This is useful for cleaning up after a job is finished.
    async fn remove_shuffle(&self, shuffle_id: u32) -> Result<()> {
        let shuffle_dir = self.root_dir.join(shuffle_id.to_string());
        if fs::try_exists(&shuffle_dir).await? {
            fs::remove_dir_all(&shuffle_dir).await?;
        }
        Ok(())
    }
}

impl OptimizedShuffleBlockManager {
    /// Get statistics about shuffle storage usage
    pub async fn get_shuffle_stats(&self, shuffle_id: u32) -> Result<ShuffleStats> {
        let shuffle_dir = self.root_dir.join(shuffle_id.to_string());
        let mut total_size = 0u64;
        let mut block_count = 0u32;

        if fs::try_exists(&shuffle_dir).await? {
            let mut entries = fs::read_dir(&shuffle_dir).await?;
            while let Some(entry) = entries.next_entry().await? {
                if entry.file_type().await?.is_file() {
                    total_size += entry.metadata().await?.len();
                    block_count += 1;
                }
            }
        }

        Ok(ShuffleStats {
            shuffle_id,
            total_size_bytes: total_size,
            block_count,
            compression_ratio: self.config.compression.estimated_compression_ratio(),
        })
    }

    /// Clean up old shuffle data based on age
    pub async fn cleanup_old_shuffles(&self, max_age_secs: u64) -> Result<Vec<u32>> {
        let mut cleaned_shuffles = Vec::new();
        let cutoff_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs()
            .saturating_sub(max_age_secs);

        let mut entries = fs::read_dir(&self.root_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            if entry.file_type().await?.is_dir() {
                if let Ok(shuffle_id) = entry.file_name().to_string_lossy().parse::<u32>() {
                    let metadata = entry.metadata().await?;
                    if let Ok(modified) = metadata.modified() {
                        if let Ok(modified_secs) = modified.duration_since(std::time::UNIX_EPOCH) {
                            if modified_secs.as_secs() < cutoff_time {
                                self.remove_shuffle(shuffle_id).await?;
                                cleaned_shuffles.push(shuffle_id);
                            }
                        }
                    }
                }
            }
        }

        Ok(cleaned_shuffles)
    }
}

/// Statistics about shuffle storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShuffleStats {
    pub shuffle_id: u32,
    pub total_size_bytes: u64,
    pub block_count: u32,
    pub compression_ratio: f32,
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
    // Track spill files for merging later
    spill_files: HashMap<u32, Vec<PathBuf>>,
    spill_dir: PathBuf,
}

impl<K, V> HashShuffleWriter<K, V>
where
    K: serde::Serialize
        + Send
        + Sync
        + Clone
        + bincode::Encode
        + bincode::Decode<()>
        + std::hash::Hash
        + Ord,
    V: serde::Serialize + Send + Sync + Clone + bincode::Encode + bincode::Decode<()>,
{
    pub fn new(
        shuffle_id: u32,
        map_id: u32,
        partitioner: Arc<dyn crate::traits::Partitioner<K>>,
        block_manager: Arc<dyn ShuffleBlockManager>,
        config: ShuffleConfig,
    ) -> Self {
        // Each writer gets its own temporary spill directory with a unique identifier
        // to avoid conflicts when multiple tests run in parallel
        let unique_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let spill_dir = std::env::temp_dir().join(format!(
            "barks_shuffle_{}_{}_{}",
            shuffle_id, map_id, unique_id
        ));
        Self {
            shuffle_id,
            map_id,
            partitioner,
            block_manager,
            config,
            buffers: HashMap::new(),
            memory_usage: 0,
            spill_files: HashMap::new(),
            spill_dir,
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
        // Create spill directory if it doesn't exist
        fs::create_dir_all(&self.spill_dir).await?;

        for (reduce_id, buffer) in self.buffers.iter_mut().filter(|(_, b)| !b.is_empty()) {
            if self.config.sort_based_shuffle {
                buffer.sort_by(|a, b| a.0.cmp(&b.0));
            }

            let spill_path = self.spill_dir.join(format!(
                "shuffle_{}_{}_spill_{}",
                self.shuffle_id,
                reduce_id,
                self.spill_files.get(reduce_id).map_or(0, |v| v.len())
            ));

            let serialized = bincode::encode_to_vec(&*buffer, bincode::config::standard())
                .map_err(|e| anyhow!("Failed to serialize shuffle spill buffer: {}", e))?;

            fs::write(&spill_path, &serialized).await?;

            self.spill_files
                .entry(*reduce_id)
                .or_default()
                .push(spill_path);
            buffer.clear();
        }

        self.memory_usage = 0;
        Ok(())
    }

    pub async fn close(&mut self) -> Result<crate::traits::MapStatus> {
        // Return block sizes for MapStatus
        let mut block_sizes = HashMap::new();
        for reduce_id in 0..self.partitioner.num_partitions() {
            let block_id = ShuffleBlockId {
                shuffle_id: self.shuffle_id,
                map_id: self.map_id,
                reduce_id,
            };

            let spills = self.spill_files.remove(&reduce_id).unwrap_or_default();
            let mut all_records: Vec<(K, V)> = Vec::new();

            // First, read all existing spill files
            for spill_file in spills {
                // Check if spill file exists before trying to read it
                if fs::try_exists(&spill_file).await.unwrap_or(false) {
                    let data = fs::read(&spill_file).await?;
                    let (decoded, _): (Vec<(K, V)>, _) =
                        bincode::decode_from_slice(&data, bincode::config::standard())?;
                    all_records.extend(decoded);

                    // Clean up spill file
                    let _ = fs::remove_file(&spill_file).await;
                }
            }

            // Then, add any remaining data in the buffer for this partition
            if let Some(buffer) = self.buffers.get(&reduce_id) {
                all_records.extend(buffer.clone());
            }

            if !all_records.is_empty() {
                // Sort the buffer by key for better read performance
                if self.config.sort_based_shuffle {
                    all_records.sort_by(|a, b| a.0.cmp(&b.0));
                }

                let serialized = bincode::encode_to_vec(&all_records, bincode::config::standard())
                    .map_err(|e| anyhow!("Failed to serialize merged shuffle data: {}", e))?;

                self.block_manager
                    .put_block(block_id.clone(), serialized)
                    .await?;
                let size = self.block_manager.get_block_size(&block_id).await?;
                block_sizes.insert(reduce_id, size);
            } else {
                // Create an empty block for partitions with no data
                let empty_data: Vec<(K, V)> = Vec::new();
                let serialized =
                    bincode::encode_to_vec(&empty_data, bincode::config::standard())
                        .map_err(|e| anyhow!("Failed to serialize empty shuffle buffer: {}", e))?;

                self.block_manager
                    .put_block(block_id.clone(), serialized)
                    .await?;
                let size = self.block_manager.get_block_size(&block_id).await?;
                block_sizes.insert(reduce_id, size);
            }
        }

        // Clean up spill directory
        let _ = fs::remove_dir_all(&self.spill_dir).await;

        Ok(crate::traits::MapStatus::new(block_sizes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shuffle::MemoryShuffleManager;
    use crate::traits::{Partitioner, ShuffleBlockId};
    use std::hash::{Hash, Hasher};
    use std::sync::Arc;
    use tempfile::tempdir;

    #[derive(Debug, Clone, Hash, Eq, PartialEq)]
    struct SimplePartitioner {
        num_partitions: u32,
    }

    impl Partitioner<String> for SimplePartitioner {
        fn num_partitions(&self) -> u32 {
            self.num_partitions
        }
        fn get_partition(&self, key: &String) -> u32 {
            // Simple hash partitioner for testing
            let mut s = std::collections::hash_map::DefaultHasher::new();
            key.hash(&mut s);
            (s.finish() % self.num_partitions as u64) as u32
        }
    }

    #[test]
    fn test_compression_codec() {
        let test_data = b"Hello, World! This is a test string that should compress well with LZ4.";

        // Test None compression
        let none_codec = CompressionCodec::None;
        let compressed = none_codec.compress(test_data).unwrap();
        let decompressed = none_codec.decompress(&compressed).unwrap();
        assert_eq!(test_data, decompressed.as_slice());
        assert_eq!(compressed.len(), test_data.len());

        #[cfg(feature = "compression")]
        {
            // Test LZ4 compression
            let lz4_codec = CompressionCodec::Lz4;
            assert!(lz4_codec.is_available());

            let compressed = lz4_codec.compress(test_data).unwrap();
            let decompressed = lz4_codec.decompress(&compressed).unwrap();
            assert_eq!(test_data, decompressed.as_slice());

            // LZ4 should compress this test data
            println!(
                "Original size: {}, Compressed size: {}",
                test_data.len(),
                compressed.len()
            );
            // Note: For small data, LZ4 might not compress much due to overhead
        }
    }

    #[test]
    fn test_compression_codec_properties() {
        let none_codec = CompressionCodec::None;
        assert!(none_codec.is_available());
        assert_eq!(none_codec.estimated_compression_ratio(), 1.0);

        #[cfg(feature = "compression")]
        {
            let lz4_codec = CompressionCodec::Lz4;
            assert!(lz4_codec.is_available());
            // LZ4 typically achieves ~40% compression, so ratio is ~0.6
            assert!((lz4_codec.estimated_compression_ratio() - 0.6).abs() < f32::EPSILON);
        }
    }

    #[tokio::test]
    async fn test_optimized_shuffle_block_manager_spill() {
        let dir = tempdir().unwrap();
        let config = ShuffleConfig {
            spill_threshold: 100, // Small threshold to force spilling
            ..Default::default()
        };
        let manager = OptimizedShuffleBlockManager::new(dir.path(), config).unwrap();

        let block_id1 = ShuffleBlockId {
            shuffle_id: 1,
            map_id: 0,
            reduce_id: 0,
        };
        let data1 = vec![0; 50]; // Under threshold
        manager
            .put_block(block_id1.clone(), data1.clone())
            .await
            .unwrap();

        let block_id2 = ShuffleBlockId {
            shuffle_id: 1,
            map_id: 0,
            reduce_id: 1,
        };
        let data2 = vec![1; 60];
        manager
            .put_block(block_id2.clone(), data2.clone())
            .await
            .unwrap();

        // With the fix, the manager simply writes files. There are no "spilled" files.
        // We just verify that we can get the data back.
        assert!(manager.contains_block(&block_id1).await.unwrap());
        assert!(manager.contains_block(&block_id2).await.unwrap());

        // Test getting both blocks
        let retrieved1 = manager.get_block(&block_id1).await.unwrap();
        assert_eq!(retrieved1, data1);

        let retrieved2 = manager.get_block(&block_id2).await.unwrap();
        assert_eq!(retrieved2, data2);

        // Test removing a block
        manager.remove_block(&block_id2).await.unwrap();
        assert!(!manager.contains_block(&block_id2).await.unwrap());
    }

    #[tokio::test]
    async fn test_optimized_shuffle_manager_stats_and_cleanup() {
        let dir = tempdir().unwrap();
        let config = ShuffleConfig::default();
        let manager = OptimizedShuffleBlockManager::new(dir.path(), config).unwrap();

        // --- Test Cleanup Setup ---
        // Create an old shuffle directory first
        let old_shuffle_id = 2;
        let old_shuffle_dir = dir.path().join(old_shuffle_id.to_string());
        tokio::fs::create_dir(&old_shuffle_dir).await.unwrap();
        tokio::fs::write(old_shuffle_dir.join("0_0"), b"old_data")
            .await
            .unwrap();

        // Wait for a second so the old directory is "old"
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        // --- Test Stats ---
        let shuffle_id = 1;
        let block_id1 = ShuffleBlockId {
            shuffle_id,
            map_id: 0,
            reduce_id: 0,
        };
        let block_id2 = ShuffleBlockId {
            shuffle_id,
            map_id: 0,
            reduce_id: 1,
        };
        let data1 = vec![0; 50];
        let data2 = vec![1; 60];

        manager.put_block(block_id1.clone(), data1).await.unwrap();
        manager.put_block(block_id2.clone(), data2).await.unwrap();

        let stats = manager.get_shuffle_stats(shuffle_id).await.unwrap();
        assert_eq!(stats.shuffle_id, shuffle_id);
        assert_eq!(stats.block_count, 2);
        let expected_size = manager.get_block_size(&block_id1).await.unwrap()
            + manager.get_block_size(&block_id2).await.unwrap();
        assert_eq!(stats.total_size_bytes, expected_size);

        // --- Test Cleanup ---

        // The current shuffle directory should exist because we put blocks in it
        let current_shuffle_dir = dir.path().join(shuffle_id.to_string());

        // Cleanup shuffles older than 1 second (old shuffle should be cleaned, current should remain)
        let cleaned = manager.cleanup_old_shuffles(1).await.unwrap();

        assert!(cleaned.contains(&old_shuffle_id));
        assert!(!tokio::fs::try_exists(&old_shuffle_dir).await.unwrap());
        assert!(tokio::fs::try_exists(&current_shuffle_dir).await.unwrap());

        manager.remove_shuffle(shuffle_id).await.unwrap();
        assert!(!tokio::fs::try_exists(&current_shuffle_dir).await.unwrap());
    }

    #[tokio::test]
    async fn test_hash_shuffle_writer() {
        let block_manager = Arc::new(MemoryShuffleManager::new());
        let partitioner = Arc::new(SimplePartitioner { num_partitions: 2 });

        let mut writer = HashShuffleWriter::<String, i32>::new(
            1, // shuffle_id
            0, // map_id
            partitioner,
            block_manager.clone(),
            ShuffleConfig::default(),
        );

        // Write some records
        writer
            .write(("key_for_part_0".to_string(), 100))
            .await
            .unwrap(); // partition 0
        writer
            .write(("key_for_part_1".to_string(), 200))
            .await
            .unwrap(); // partition 1
        writer
            .write(("another_for_part_0".to_string(), 101))
            .await
            .unwrap(); // partition 0

        // Close the writer to flush buffers and get MapStatus
        let map_status = writer.close().await.unwrap();

        // Verify MapStatus
        let block_sizes = map_status.get_block_sizes();
        assert_eq!(block_sizes.len(), 2);
        assert!(block_sizes.get(&0).unwrap() > &0);
        assert!(block_sizes.get(&1).unwrap() > &0);

        // Verify the contents of the blocks
        let block_id0 = ShuffleBlockId {
            shuffle_id: 1,
            map_id: 0,
            reduce_id: 0,
        };
        let block0_data = block_manager.get_block(&block_id0).await.unwrap();
        let (records0, _): (Vec<(String, i32)>, _) =
            bincode::decode_from_slice(&block0_data, bincode::config::standard()).unwrap();
        assert_eq!(records0.len(), 2);
        assert!(records0.contains(&("key_for_part_0".to_string(), 100)));
        assert!(records0.contains(&("another_for_part_0".to_string(), 101)));

        let block_id1 = ShuffleBlockId {
            shuffle_id: 1,
            map_id: 0,
            reduce_id: 1,
        };
        let block1_data = block_manager.get_block(&block_id1).await.unwrap();
        let (records1, _): (Vec<(String, i32)>, _) =
            bincode::decode_from_slice(&block1_data, bincode::config::standard()).unwrap();
        assert_eq!(records1, vec![("key_for_part_1".to_string(), 200)]);
    }

    #[tokio::test]
    async fn test_hash_shuffle_writer_spilling_append() {
        let block_manager = Arc::new(MemoryShuffleManager::new());
        let partitioner = Arc::new(SimplePartitioner { num_partitions: 1 });

        // Set a very low threshold to force a spill on each write.
        let config = ShuffleConfig {
            spill_threshold: 1,        // Spill after ~1 byte (very aggressive)
            sort_based_shuffle: false, // Disable sort for predictable order
            ..Default::default()
        };

        let mut writer =
            HashShuffleWriter::<String, i32>::new(1, 0, partitioner, block_manager.clone(), config);

        // Write records one by one, each should trigger a spill
        writer.write(("a".to_string(), 1)).await.unwrap();
        writer.write(("b".to_string(), 2)).await.unwrap();
        writer.write(("c".to_string(), 3)).await.unwrap();

        let block_id = ShuffleBlockId {
            shuffle_id: 1,
            map_id: 0,
            reduce_id: 0,
        };

        // With the new implementation, spills go to temporary files, not the block manager
        // So the block won't exist until close() is called
        assert!(!block_manager.contains_block(&block_id).await.unwrap());

        // Close the writer to merge all spill files.
        writer.close().await.unwrap();

        // Now the block should exist and contain all records
        let data = block_manager.get_block(&block_id).await.unwrap();
        let (mut records, _): (Vec<(String, i32)>, _) =
            bincode::decode_from_slice(&data, bincode::config::standard()).unwrap();

        // Sort the records for consistent comparison since spill order might vary
        records.sort_by(|a, b| a.0.cmp(&b.0));

        assert_eq!(
            records,
            vec![
                ("a".to_string(), 1),
                ("b".to_string(), 2),
                ("c".to_string(), 3)
            ]
        );
    }
}
