//! Shuffle implementations

use crate::traits::*;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::sync::RwLock;

#[cfg(feature = "server")]
use tracing::{debug, error, info};

/// Basic shuffle data implementation
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
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
    K: Send + Sync + Clone + Serialize + for<'de> Deserialize<'de> + Encode + Decode<()>,
    V: Send + Sync + Clone + Serialize + for<'de> Deserialize<'de> + Encode + Decode<()>,
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
        bincode::encode_to_vec(self, bincode::config::standard())
            .map_err(|e| anyhow::anyhow!("Bincode serialization failed: {}", e))
    }

    fn from_bytes(data: &[u8]) -> Result<Self> {
        bincode::decode_from_slice(data, bincode::config::standard())
            .map(|(res, _)| res)
            .map_err(|e| anyhow::anyhow!("Bincode deserialization failed: {}", e))
    }
}

/// In-memory shuffle manager
#[derive(Default, Clone)]
pub struct MemoryShuffleManager {
    blocks: Arc<RwLock<HashMap<ShuffleBlockId, Vec<u8>>>>,
}

impl MemoryShuffleManager {
    pub fn new() -> Self {
        Self {
            blocks: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl ShuffleBlockManager for MemoryShuffleManager {
    async fn get_block(&self, block_id: &ShuffleBlockId) -> Result<Vec<u8>> {
        let blocks = self.blocks.read().await;
        blocks
            .get(block_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Block not found: {:?}", block_id))
    }

    async fn put_block(&self, block_id: ShuffleBlockId, data: Vec<u8>) -> Result<()> {
        let mut blocks = self.blocks.write().await;
        blocks.insert(block_id, data);
        Ok(())
    }

    async fn remove_block(&self, block_id: &ShuffleBlockId) -> Result<()> {
        let mut blocks = self.blocks.write().await;
        blocks.remove(block_id);
        Ok(())
    }

    async fn contains_block(&self, block_id: &ShuffleBlockId) -> Result<bool> {
        Ok(self.blocks.read().await.contains_key(block_id))
    }

    async fn get_block_size(&self, block_id: &ShuffleBlockId) -> Result<u64> {
        let blocks = self.blocks.read().await;
        Ok(blocks.get(block_id).map(|d| d.len() as u64).unwrap_or(0))
    }
}

/// File-based shuffle block manager
#[derive(Debug, Clone)]
pub struct FileShuffleBlockManager {
    root_dir: PathBuf,
}

impl FileShuffleBlockManager {
    pub fn new<P: AsRef<Path>>(root_dir: P) -> Result<Self> {
        let path = root_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&path)?;
        Ok(Self { root_dir: path })
    }

    fn get_block_path(&self, block_id: &ShuffleBlockId) -> PathBuf {
        self.root_dir
            .join(block_id.shuffle_id.to_string())
            .join(format!("{}_{}", block_id.map_id, block_id.reduce_id))
    }
}

#[async_trait]
impl ShuffleBlockManager for FileShuffleBlockManager {
    async fn get_block(&self, block_id: &ShuffleBlockId) -> Result<Vec<u8>> {
        let path = self.get_block_path(block_id);
        fs::read(&path).await.map_err(|e| {
            anyhow::anyhow!("Failed to read block {:?} from {:?}: {}", block_id, path, e)
        })
    }

    async fn put_block(&self, block_id: ShuffleBlockId, data: Vec<u8>) -> Result<()> {
        let path = self.get_block_path(&block_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let mut file = File::create(&path).await?;
        file.write_all(&data).await?;
        Ok(())
    }

    async fn remove_block(&self, block_id: &ShuffleBlockId) -> Result<()> {
        let path = self.get_block_path(block_id);
        if fs::try_exists(&path).await? {
            fs::remove_file(&path).await?;
        }
        Ok(())
    }

    async fn contains_block(&self, block_id: &ShuffleBlockId) -> Result<bool> {
        fs::try_exists(&self.get_block_path(block_id))
            .await
            .map_err(|e| e.into())
    }

    async fn get_block_size(&self, block_id: &ShuffleBlockId) -> Result<u64> {
        let path = self.get_block_path(block_id);
        match fs::metadata(&path).await {
            Ok(metadata) => Ok(metadata.len()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(e) => Err(e.into()),
        }
    }
}

impl FileShuffleBlockManager {
    /// Removes all files and directories associated with a given shuffle_id.
    /// This is useful for cleaning up after a job is finished.
    pub async fn remove_shuffle(&self, shuffle_id: u32) -> Result<()> {
        let shuffle_dir = self.root_dir.join(shuffle_id.to_string());
        if fs::try_exists(&shuffle_dir).await? {
            fs::remove_dir_all(&shuffle_dir).await?;
        }
        Ok(())
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
    pub fn new(
        shuffle_id: u32,
        map_id: u32,
        blocks: Arc<RwLock<HashMap<ShuffleBlockId, Vec<u8>>>>,
    ) -> Self {
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

/// A writer that partitions data by key and writes to shuffle files.
///
/// This writer buffers outputs for each reduce partition and flushes them to the
/// underlying ShuffleBlockManager.
pub struct BytewaxShuffleWriter<K, V> {
    shuffle_id: u32,
    map_id: u32,
    partitioner: Arc<dyn Partitioner>,
    block_manager: Arc<dyn ShuffleBlockManager>,
    // Buffer for each reduce partition: reduce_id -> Vec<Serialized(K, V)>
    buffers: HashMap<u32, Vec<u8>>,
    // Track block sizes for MapStatus
    block_sizes: HashMap<u32, u64>,
    _marker: std::marker::PhantomData<(K, V)>,
}

impl<K, V> BytewaxShuffleWriter<K, V>
where
    K: Serialize + Send + Sync + Clone + Encode,
    V: Serialize + Send + Sync + Clone + Encode,
{
    pub fn new(
        shuffle_id: u32,
        map_id: u32,
        partitioner: Arc<dyn Partitioner>,
        block_manager: Arc<dyn ShuffleBlockManager>,
    ) -> Self {
        Self {
            shuffle_id,
            map_id,
            partitioner,
            block_manager,
            buffers: HashMap::new(),
            block_sizes: HashMap::new(),
            _marker: std::marker::PhantomData,
        }
    }

    /// Write a key-value pair. The key is used to determine the output partition.
    pub async fn write(&mut self, record: (K, V)) -> Result<()> {
        let (key, value) = record;
        // For now, we'll use a simple hash-based partitioning
        // In a real implementation, we'd use the partitioner properly
        let reduce_id = 0; // Simplified for now

        let serialized_record =
            bincode::encode_to_vec((key, value), bincode::config::standard())
                .map_err(|e| anyhow::anyhow!("Failed to serialize shuffle record: {}", e))?;

        let buffer = self.buffers.entry(reduce_id).or_default();
        buffer.extend_from_slice(&serialized_record);
        Ok(())
    }

    /// Flush all buffered data to the block manager and return the map status.
    /// This should be called once all records for the map task have been written.
    pub async fn close(mut self) -> Result<MapStatus> {
        #[cfg(feature = "server")]
        info!(
            "Closing shuffle writer for map task {}_{}",
            self.shuffle_id, self.map_id
        );

        for (reduce_id, buffer) in self.buffers.drain() {
            if !buffer.is_empty() {
                let block_id = ShuffleBlockId {
                    shuffle_id: self.shuffle_id,
                    map_id: self.map_id,
                    reduce_id,
                };
                let block_size = buffer.len() as u64;
                self.block_manager.put_block(block_id, buffer).await?;
                self.block_sizes.insert(reduce_id, block_size);
            }
        }
        Ok(MapStatus::new(self.block_sizes))
    }
}

/// Fetches shuffle blocks from remote executors for a reduce task.
#[cfg(feature = "server")]
pub struct BytewaxShuffleReader<K, V> {
    client: reqwest::Client,
    _marker: std::marker::PhantomData<(K, V)>,
}

#[cfg(feature = "server")]
impl<K: for<'de> Deserialize<'de>, V: for<'de> Deserialize<'de>> BytewaxShuffleReader<K, V> {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
            _marker: std::marker::PhantomData,
        }
    }

    pub async fn read_partition(
        &self,
        _map_status: &MapStatus,
        _reduce_id: u32,
    ) -> Result<Vec<(K, V)>> {
        // Implementation would fetch data from locations in map_status
        // and deserialize it.
        unimplemented!("ShuffleReader::read_partition needs implementation");
    }
}

#[cfg(feature = "server")]
pub mod server {
    use super::*;
    use axum::{
        extract::{Path, State},
        http::StatusCode,
        response::IntoResponse,
        routing::get,
        Router,
    };
    use std::net::SocketAddr;
    use tracing::{error, info};

    #[derive(Clone)]
    struct ShuffleServerState {
        block_manager: Arc<dyn ShuffleBlockManager>,
    }

    /// A simple HTTP server to serve shuffle blocks.
    pub struct ShuffleServer {
        addr: SocketAddr,
        block_manager: Arc<dyn ShuffleBlockManager>,
    }

    impl ShuffleServer {
        pub fn new(addr: SocketAddr, block_manager: Arc<dyn ShuffleBlockManager>) -> Self {
            Self {
                addr,
                block_manager,
            }
        }

        pub async fn start(self) -> Result<()> {
            let state = ShuffleServerState {
                block_manager: self.block_manager,
            };
            let app = Router::new()
                .route(
                    "/shuffle/:shuffle_id/:map_id/:reduce_id",
                    get(get_shuffle_block),
                )
                .with_state(state);

            info!("Shuffle server listening on {}", self.addr);
            let listener = tokio::net::TcpListener::bind(self.addr).await?;
            axum::serve(listener, app).await?;
            Ok(())
        }
    }

    async fn get_shuffle_block(
        State(state): State<ShuffleServerState>,
        Path((shuffle_id, map_id, reduce_id)): Path<(u32, u32, u32)>,
    ) -> impl IntoResponse {
        let block_id = ShuffleBlockId {
            shuffle_id,
            map_id,
            reduce_id,
        };
        match state.block_manager.get_block(&block_id).await {
            Ok(data) => (StatusCode::OK, data).into_response(),
            Err(e) => {
                error!("Failed to get shuffle block {:?}: {}", block_id, e);
                (StatusCode::NOT_FOUND, e.to_string()).into_response()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_file_shuffle_block_manager() {
        let dir = tempdir().unwrap();
        let manager = FileShuffleBlockManager::new(dir.path()).unwrap();

        let block_id1 = ShuffleBlockId {
            shuffle_id: 1,
            map_id: 0,
            reduce_id: 0,
        };
        let data1 = b"hello".to_vec();

        let block_id2 = ShuffleBlockId {
            shuffle_id: 1,
            map_id: 0,
            reduce_id: 1,
        };
        let data2 = b"world".to_vec();

        // Test put and contains
        assert!(!manager.contains_block(&block_id1).await.unwrap());
        manager
            .put_block(block_id1.clone(), data1.clone())
            .await
            .unwrap();
        assert!(manager.contains_block(&block_id1).await.unwrap());

        manager
            .put_block(block_id2.clone(), data2.clone())
            .await
            .unwrap();

        // Test get
        let retrieved_data1 = manager.get_block(&block_id1).await.unwrap();
        assert_eq!(retrieved_data1, data1);

        // Test get_block_size
        let size1 = manager.get_block_size(&block_id1).await.unwrap();
        assert_eq!(size1, data1.len() as u64);

        // Test remove
        manager.remove_block(&block_id1).await.unwrap();
        assert!(!manager.contains_block(&block_id1).await.unwrap());
        assert_eq!(manager.get_block_size(&block_id1).await.unwrap(), 0);
        assert!(manager.get_block(&block_id1).await.is_err());

        // Test remove_shuffle
        assert!(manager.contains_block(&block_id2).await.unwrap());
        manager.remove_shuffle(1).await.unwrap();
        assert!(!manager.contains_block(&block_id2).await.unwrap());

        // Check that the shuffle directory is gone
        assert!(!dir.path().join("1").exists());
    }
}
