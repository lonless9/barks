//! Shuffle implementations

use crate::traits::*;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use anyhow::Result;
use serde::{Deserialize, Serialize};

/// Basic shuffle data implementation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BasicShuffleData<K, V> {
    key: K,
    value: V,
}

impl<K, V> BasicShuffleData<K, V>
where
    K: Send + Sync + Clone,
    V: Send + Sync + Clone,
{
    pub fn new(key: K, value: V) -> Self {
        Self { key, value }
    }
}

impl<K, V> ShuffleData for BasicShuffleData<K, V>
where
    K: Send + Sync + Clone + Serialize + for<'de> Deserialize<'de>,
    V: Send + Sync + Clone + Serialize + for<'de> Deserialize<'de>,
{
    type Key = K;
    type Value = V;
    
    fn key(&self) -> &Self::Key {
        &self.key
    }
    
    fn value(&self) -> &Self::Value {
        &self.value
    }
    
    fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }
    
    fn from_bytes(data: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(data)?)
    }
}

/// In-memory shuffle manager
pub struct MemoryShuffleManager {
    shuffles: Arc<RwLock<HashMap<u32, ShuffleInfo>>>,
    blocks: Arc<RwLock<HashMap<ShuffleBlockId, Vec<u8>>>>,
}

impl MemoryShuffleManager {
    pub fn new() -> Self {
        Self {
            shuffles: Arc::new(RwLock::new(HashMap::new())),
            blocks: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Default for MemoryShuffleManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Memory shuffle writer
pub struct MemoryShuffleWriter<D: ShuffleData> {
    shuffle_id: u32,
    map_id: u32,
    blocks: Arc<RwLock<HashMap<ShuffleBlockId, Vec<u8>>>>,
    bytes_written: u64,
    _phantom: std::marker::PhantomData<D>,
}

impl<D: ShuffleData> MemoryShuffleWriter<D> {
    pub fn new(shuffle_id: u32, map_id: u32, blocks: Arc<RwLock<HashMap<ShuffleBlockId, Vec<u8>>>>) -> Self {
        Self {
            shuffle_id,
            map_id,
            blocks,
            bytes_written: 0,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<D: ShuffleData> ShuffleWriter for MemoryShuffleWriter<D> {
    type Data = D;
    
    async fn write(&mut self, partition_id: u32, data: Self::Data) -> Result<()> {
        let block_id = ShuffleBlockId {
            shuffle_id: self.shuffle_id,
            map_id: self.map_id,
            reduce_id: partition_id,
        };
        
        let bytes = data.to_bytes()?;
        self.bytes_written += bytes.len() as u64;
        
        let mut blocks = self.blocks.write().await;
        blocks.insert(block_id, bytes);
        
        Ok(())
    }
    
    async fn flush(&mut self) -> Result<()> {
        // Memory implementation doesn't need flushing
        Ok(())
    }
    
    async fn close(&mut self) -> Result<()> {
        // Nothing to close for memory implementation
        Ok(())
    }
    
    fn bytes_written(&self) -> u64 {
        self.bytes_written
    }
}
