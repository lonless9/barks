//! Shuffle implementations

use crate::traits::*;
use anyhow::Result;
use async_trait::async_trait;
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

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
