//! LevelDB-based shuffle block manager for improved performance and persistence

use crate::traits::{ShuffleBlockId, ShuffleBlockManager};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use rusty_leveldb::{DB, LdbIterator, Options};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

/// LevelDB-based shuffle block manager
pub struct LevelDBShuffleBlockManager {
    /// The LevelDB database instance
    db: Arc<Mutex<DB>>,
    /// Path to the database
    path: PathBuf,
}

impl LevelDBShuffleBlockManager {
    /// Create a new LevelDB shuffle block manager
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        // Create directory if it doesn't exist
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| anyhow!("Failed to create directory: {}", e))?;
        }

        let mut options = Options::default();
        options.create_if_missing = true;

        // Configure for better performance with shuffle data
        options.write_buffer_size = 64 * 1024 * 1024; // 64MB write buffer
        options.max_open_files = 1000;

        let db = DB::open(&path, options).map_err(|e| anyhow!("Failed to open LevelDB: {}", e))?;

        Ok(Self {
            db: Arc::new(Mutex::new(db)),
            path,
        })
    }

    /// Get the database path
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Generate a key for the given block ID
    fn block_key(&self, block_id: &ShuffleBlockId) -> Vec<u8> {
        format!(
            "shuffle_{}_{}_{}",
            block_id.shuffle_id, block_id.map_id, block_id.reduce_id
        )
        .into_bytes()
    }

    /// Get statistics about the database
    pub async fn stats(&self) -> Result<LevelDBShuffleStats> {
        let mut db = self.db.lock().await;
        let mut total_blocks = 0;
        let mut total_size = 0;

        let mut iter = db
            .new_iter()
            .map_err(|e| anyhow!("Failed to create iterator: {}", e))?;
        iter.seek_to_first();

        while iter.valid() {
            total_blocks += 1;
            let mut key_bytes = Vec::new();
            let mut value_bytes = Vec::new();
            if iter.current(&mut key_bytes, &mut value_bytes) {
                total_size += value_bytes.len();
            }
            iter.advance();
        }

        Ok(LevelDBShuffleStats {
            total_blocks,
            total_size_bytes: total_size,
            database_path: self.path.to_string_lossy().to_string(),
        })
    }

    /// Compact the database to reclaim space
    pub async fn compact(&self) -> Result<()> {
        let _db = self.db.lock().await;

        // LevelDB doesn't expose a direct compact method in this crate
        // In a production system, we might use a different LevelDB binding
        // that provides more control over compaction

        // For now, this is a no-op
        Ok(())
    }

    /// Clear all shuffle data for a specific shuffle ID
    pub async fn clear_shuffle(&self, shuffle_id: u32) -> Result<()> {
        let mut db = self.db.lock().await;
        let prefix = format!("shuffle_{}_", shuffle_id).into_bytes();
        let mut keys_to_delete = Vec::new();

        let mut iter = db
            .new_iter()
            .map_err(|e| anyhow!("Failed to create iterator: {}", e))?;
        iter.seek(&prefix);

        while iter.valid() {
            let mut key_bytes = Vec::new();
            let mut value_bytes = Vec::new();
            if iter.current(&mut key_bytes, &mut value_bytes) {
                if key_bytes.starts_with(&prefix) {
                    keys_to_delete.push(key_bytes);
                } else {
                    break; // Keys are sorted, so we can stop here
                }
            }
            iter.advance();
        }

        // Delete all keys for this shuffle
        for key in keys_to_delete {
            db.delete(&key)
                .map_err(|e| anyhow!("Failed to delete shuffle block: {}", e))?;
        }

        Ok(())
    }
}

#[async_trait]
impl ShuffleBlockManager for LevelDBShuffleBlockManager {
    async fn get_block(&self, block_id: &ShuffleBlockId) -> Result<Vec<u8>> {
        let key = self.block_key(block_id);
        let mut db = self.db.lock().await;

        match db.get(&key) {
            Some(data) => Ok(data),
            None => Err(anyhow!("Shuffle block not found: {:?}", block_id)),
        }
    }

    async fn put_block(&self, block_id: ShuffleBlockId, data: Vec<u8>) -> Result<()> {
        let key = self.block_key(&block_id);
        let mut db = self.db.lock().await;

        db.put(&key, &data)
            .map_err(|e| anyhow!("Failed to write shuffle block {:?}: {}", block_id, e))?;

        Ok(())
    }

    async fn remove_block(&self, block_id: &ShuffleBlockId) -> Result<()> {
        let key = self.block_key(block_id);
        let mut db = self.db.lock().await;

        db.delete(&key)
            .map_err(|e| anyhow!("Failed to delete shuffle block {:?}: {}", block_id, e))?;

        Ok(())
    }

    async fn contains_block(&self, block_id: &ShuffleBlockId) -> Result<bool> {
        let key = self.block_key(block_id);
        let mut db = self.db.lock().await;

        Ok(db.get(&key).is_some())
    }

    async fn get_block_size(&self, block_id: &ShuffleBlockId) -> Result<u64> {
        let key = self.block_key(block_id);
        let mut db = self.db.lock().await;

        match db.get(&key) {
            Some(data) => Ok(data.len() as u64),
            None => Err(anyhow!("Shuffle block not found: {:?}", block_id)),
        }
    }

    async fn remove_shuffle(&self, shuffle_id: u32) -> Result<()> {
        self.clear_shuffle(shuffle_id).await
    }
}

impl LevelDBShuffleBlockManager {
    /// Parse a block key back into a ShuffleBlockId
    fn parse_block_key(&self, key: &str) -> Option<ShuffleBlockId> {
        let parts: Vec<&str> = key.split('_').collect();
        if parts.len() >= 4 && parts[0] == "shuffle" {
            let shuffle_id = parts[1].parse().ok()?;
            let map_id = parts[2].parse().ok()?;
            let reduce_id = parts[3].parse().ok()?;

            Some(ShuffleBlockId {
                shuffle_id,
                map_id,
                reduce_id,
            })
        } else {
            None
        }
    }

    /// List all blocks in the database
    pub async fn list_blocks(&self) -> Result<Vec<ShuffleBlockId>> {
        let mut db = self.db.lock().await;
        let mut blocks = Vec::new();

        let mut iter = db
            .new_iter()
            .map_err(|e| anyhow!("Failed to create iterator: {}", e))?;
        iter.seek_to_first();

        while iter.valid() {
            let mut key_bytes = Vec::new();
            let mut value_bytes = Vec::new();
            if iter.current(&mut key_bytes, &mut value_bytes) {
                if let Ok(key_str) = String::from_utf8(key_bytes) {
                    if let Some(block_id) = self.parse_block_key(&key_str) {
                        blocks.push(block_id);
                    }
                }
            }
            iter.advance();
        }

        Ok(blocks)
    }

    /// Clear all data from the database
    pub async fn clear(&self) -> Result<()> {
        // Clear all data by dropping and recreating the database
        let path = self.path.clone();

        // Close the current database
        drop(self.db.lock().await);

        // Remove the database directory
        if path.exists() {
            std::fs::remove_dir_all(&path)
                .map_err(|e| anyhow!("Failed to remove database directory: {}", e))?;
        }

        // Recreate the database
        let mut options = Options::default();
        options.create_if_missing = true;
        options.write_buffer_size = 64 * 1024 * 1024;
        options.max_open_files = 1000;

        let new_db =
            DB::open(&path, options).map_err(|e| anyhow!("Failed to recreate LevelDB: {}", e))?;

        // Replace the database instance
        *self.db.lock().await = new_db;

        Ok(())
    }
}

/// Statistics for LevelDB shuffle block manager
#[derive(Debug, Clone)]
pub struct LevelDBShuffleStats {
    /// Total number of blocks stored
    pub total_blocks: usize,
    /// Total size of all blocks in bytes
    pub total_size_bytes: usize,
    /// Path to the database
    pub database_path: String,
}

impl Clone for LevelDBShuffleBlockManager {
    fn clone(&self) -> Self {
        Self {
            db: Arc::clone(&self.db),
            path: self.path.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_leveldb_shuffle_basic_operations() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("shuffle_db");

        let manager = LevelDBShuffleBlockManager::new(&db_path).unwrap();

        let block_id = ShuffleBlockId {
            shuffle_id: 1,
            map_id: 0,
            reduce_id: 0,
        };
        let data = vec![1, 2, 3, 4, 5];

        // Test put and get
        manager
            .put_block(block_id.clone(), data.clone())
            .await
            .unwrap();
        let retrieved = manager.get_block(&block_id).await.unwrap();
        assert_eq!(retrieved, data);

        // Test list blocks
        let blocks = manager.list_blocks().await.unwrap();
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0], block_id);

        // Test remove
        manager.remove_block(&block_id).await.unwrap();
        assert!(manager.get_block(&block_id).await.is_err());
    }

    #[tokio::test]
    async fn test_leveldb_shuffle_stats() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("shuffle_stats_db");

        let manager = LevelDBShuffleBlockManager::new(&db_path).unwrap();

        // Add some blocks
        for i in 0..5 {
            let block_id = ShuffleBlockId {
                shuffle_id: 1,
                map_id: i,
                reduce_id: 0,
            };
            let data = vec![i as u8; 100]; // 100 bytes each
            manager.put_block(block_id, data).await.unwrap();
        }

        let stats = manager.stats().await.unwrap();
        assert_eq!(stats.total_blocks, 5);
        assert_eq!(stats.total_size_bytes, 500); // 5 * 100 bytes
    }

    #[tokio::test]
    async fn test_leveldb_shuffle_clear_shuffle() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("shuffle_clear_db");

        let manager = LevelDBShuffleBlockManager::new(&db_path).unwrap();

        // Add blocks for different shuffles
        for shuffle_id in 1..=3 {
            for map_id in 0..2 {
                let block_id = ShuffleBlockId {
                    shuffle_id,
                    map_id,
                    reduce_id: 0,
                };
                let data = vec![shuffle_id as u8; 10];
                manager.put_block(block_id, data).await.unwrap();
            }
        }

        // Should have 6 blocks total
        let stats = manager.stats().await.unwrap();
        assert_eq!(stats.total_blocks, 6);

        // Clear shuffle 2
        manager.clear_shuffle(2).await.unwrap();

        // Should have 4 blocks remaining
        let stats = manager.stats().await.unwrap();
        assert_eq!(stats.total_blocks, 4);

        // Verify the right blocks were removed
        let blocks = manager.list_blocks().await.unwrap();
        for block in blocks {
            assert_ne!(block.shuffle_id, 2);
        }
    }
}
