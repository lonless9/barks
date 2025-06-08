//! Network Shuffle traits

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Trait for shuffle data
pub trait ShuffleData: Send + Sync + Clone {
    type Key: Send + Sync + Clone;
    type Value: Send + Sync + Clone;

    /// Get shuffle key
    fn key(&self) -> &Self::Key;

    /// Get shuffle value
    fn value(&self) -> &Self::Value;

    /// Serialize to bytes
    fn to_bytes(&self) -> Result<Vec<u8>>;

    /// Deserialize from bytes
    fn from_bytes(data: &[u8]) -> Result<Self>;
}

/// Trait for shuffle writer
#[async_trait]
pub trait ShuffleWriter: Send + Sync {
    type Data: ShuffleData;

    /// Write shuffle data
    async fn write(&mut self, partition_id: u32, data: Self::Data) -> Result<()>;

    /// Flush all pending writes
    async fn flush(&mut self) -> Result<()>;

    /// Close writer
    async fn close(&mut self) -> Result<()>;

    /// Get written data size
    fn bytes_written(&self) -> u64;
}

/// Trait for shuffle reader
#[async_trait]
pub trait ShuffleReader: Send + Sync {
    type Data: ShuffleData;

    /// Read shuffle data for partition
    async fn read(&mut self, partition_id: u32) -> Result<Vec<Self::Data>>;

    /// Read all partitions
    async fn read_all(&mut self) -> Result<HashMap<u32, Vec<Self::Data>>>;

    /// Close reader
    async fn close(&mut self) -> Result<()>;

    /// Get read data size
    fn bytes_read(&self) -> u64;
}

/// Trait for shuffle manager
#[async_trait]
pub trait ShuffleManager: Send + Sync {
    type Writer: ShuffleWriter;
    type Reader: ShuffleReader;

    /// Create shuffle writer
    async fn create_writer(&self, shuffle_id: u32, map_id: u32) -> Result<Self::Writer>;

    /// Create shuffle reader
    async fn create_reader(&self, shuffle_id: u32, reduce_id: u32) -> Result<Self::Reader>;

    /// Register shuffle
    async fn register_shuffle(&self, shuffle_id: u32, num_partitions: u32) -> Result<()>;

    /// Unregister shuffle
    async fn unregister_shuffle(&self, shuffle_id: u32) -> Result<()>;

    /// Get shuffle info
    async fn get_shuffle_info(&self, shuffle_id: u32) -> Result<ShuffleInfo>;
}

/// Shuffle information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShuffleInfo {
    pub shuffle_id: u32,
    pub num_partitions: u32,
    pub num_maps: u32,
    pub total_size: u64,
}

/// Trait for shuffle block manager
#[async_trait]
pub trait ShuffleBlockManager: Send + Sync {
    /// Get shuffle block
    async fn get_block(&self, block_id: &ShuffleBlockId) -> Result<Vec<u8>>;

    /// Put shuffle block
    async fn put_block(&self, block_id: ShuffleBlockId, data: Vec<u8>) -> Result<()>;

    /// Remove shuffle block
    async fn remove_block(&self, block_id: &ShuffleBlockId) -> Result<()>;

    /// Check if block exists
    async fn contains_block(&self, block_id: &ShuffleBlockId) -> Result<bool>;

    /// Get block size
    async fn get_block_size(&self, block_id: &ShuffleBlockId) -> Result<u64>;
}

/// Shuffle block identifier
#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct ShuffleBlockId {
    pub shuffle_id: u32,
    pub map_id: u32,
    pub reduce_id: u32,
}

/// Trait for a partitioner, which defines how keys are mapped to reduce partitions.
/// Note: We'll implement specific partitioners for concrete types rather than using generics
/// to work around typetag limitations with generic traits.
pub trait Partitioner<K>: Send + Sync + std::fmt::Debug {
    /// Get the number of partitions.
    fn num_partitions(&self) -> u32;

    /// Get the partition for a given key.
    fn get_partition(&self, key: &K) -> u32;
}

/// The status of a completed map task, sent back to the driver.
/// It contains the locations and sizes of the shuffle blocks written by the task.
#[derive(Debug, Clone, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
pub struct MapStatus {
    // For now, location is implicit (the executor that ran the task).
    // A full implementation would have a `BlockManagerId`.
    // The map contains reduce_id -> block_size.
    block_sizes: HashMap<u32, u64>,
}

impl MapStatus {
    pub fn new(block_sizes: HashMap<u32, u64>) -> Self {
        Self { block_sizes }
    }

    pub fn get_block_sizes(&self) -> &HashMap<u32, u64> {
        &self.block_sizes
    }
}
