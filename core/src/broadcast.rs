//! Broadcast variables implementation
//!
//! Broadcast variables allow the programmer to keep a read-only variable cached on each machine
//! rather than shipping a copy of it with tasks. They can be used to give every node a copy of a
//! large input dataset in an efficient manner.

use crate::traits::Data;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

/// Unique identifier for a broadcast variable
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct BroadcastId(pub String);

impl BroadcastId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }
}

impl Default for BroadcastId {
    fn default() -> Self {
        Self::new()
    }
}

/// Broadcast variable that holds a read-only value
#[derive(Debug, Clone)]
pub struct BroadcastVariable<T: Data> {
    /// Unique identifier for this broadcast variable
    pub id: BroadcastId,
    /// The broadcast value
    value: Arc<T>,
}

impl<T: Data> BroadcastVariable<T> {
    /// Create a new broadcast variable with the given value
    pub fn new(value: T) -> Self {
        Self {
            id: BroadcastId::new(),
            value: Arc::new(value),
        }
    }

    /// Get the value of this broadcast variable
    pub fn value(&self) -> &T {
        &self.value
    }

    /// Get the ID of this broadcast variable
    pub fn id(&self) -> &BroadcastId {
        &self.id
    }
}

/// Serializable broadcast variable for network transmission
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableBroadcast {
    /// Unique identifier
    pub id: BroadcastId,
    /// Serialized value
    pub data: Vec<u8>,
}

impl SerializableBroadcast {
    /// Create a serializable broadcast from a broadcast variable
    pub fn from_broadcast<T: Data>(broadcast: &BroadcastVariable<T>) -> Result<Self, String> {
        let data = bincode::encode_to_vec(&*broadcast.value, bincode::config::standard())
            .map_err(|e| format!("Failed to serialize broadcast variable: {}", e))?;

        Ok(Self {
            id: broadcast.id.clone(),
            data,
        })
    }

    /// Deserialize into a broadcast variable
    pub fn to_broadcast<T: Data>(&self) -> Result<BroadcastVariable<T>, String> {
        let (value, _): (T, _) =
            bincode::decode_from_slice(&self.data, bincode::config::standard())
                .map_err(|e| format!("Failed to deserialize broadcast variable: {}", e))?;

        Ok(BroadcastVariable {
            id: self.id.clone(),
            value: Arc::new(value),
        })
    }
}

/// Manager for broadcast variables on the driver
#[derive(Debug, Default)]
pub struct BroadcastManager {
    /// Map of broadcast ID to serialized data
    broadcasts: Arc<RwLock<HashMap<BroadcastId, SerializableBroadcast>>>,
}

impl BroadcastManager {
    /// Create a new broadcast manager
    pub fn new() -> Self {
        Self {
            broadcasts: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a new broadcast variable
    pub async fn register_broadcast<T: Data>(
        &self,
        broadcast: &BroadcastVariable<T>,
    ) -> Result<(), String> {
        let serializable = SerializableBroadcast::from_broadcast(broadcast)?;
        let mut broadcasts = self.broadcasts.write().await;
        broadcasts.insert(broadcast.id.clone(), serializable);
        Ok(())
    }

    /// Get a broadcast variable by ID
    pub async fn get_broadcast(&self, id: &BroadcastId) -> Option<SerializableBroadcast> {
        let broadcasts = self.broadcasts.read().await;
        broadcasts.get(id).cloned()
    }

    /// Remove a broadcast variable
    pub async fn remove_broadcast(&self, id: &BroadcastId) -> bool {
        let mut broadcasts = self.broadcasts.write().await;
        broadcasts.remove(id).is_some()
    }

    /// Get all broadcast IDs
    pub async fn list_broadcasts(&self) -> Vec<BroadcastId> {
        let broadcasts = self.broadcasts.read().await;
        broadcasts.keys().cloned().collect()
    }

    /// Clear all broadcast variables
    pub async fn clear(&self) {
        let mut broadcasts = self.broadcasts.write().await;
        broadcasts.clear();
    }
}

/// Cache for broadcast variables on executors
#[derive(Debug, Default)]
pub struct BroadcastCache {
    /// Map of broadcast ID to serialized data
    cache: Arc<RwLock<HashMap<BroadcastId, SerializableBroadcast>>>,
}

impl BroadcastCache {
    /// Create a new broadcast cache
    pub fn new() -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Cache a broadcast variable
    pub async fn cache_broadcast(&self, broadcast: SerializableBroadcast) {
        let mut cache = self.cache.write().await;
        cache.insert(broadcast.id.clone(), broadcast);
    }

    /// Get a cached broadcast variable
    pub async fn get_broadcast(&self, id: &BroadcastId) -> Option<SerializableBroadcast> {
        let cache = self.cache.read().await;
        cache.get(id).cloned()
    }

    /// Check if a broadcast variable is cached
    pub async fn contains(&self, id: &BroadcastId) -> bool {
        let cache = self.cache.read().await;
        cache.contains_key(id)
    }

    /// Remove a broadcast variable from cache
    pub async fn remove_broadcast(&self, id: &BroadcastId) -> bool {
        let mut cache = self.cache.write().await;
        cache.remove(id).is_some()
    }

    /// Clear all cached broadcast variables
    pub async fn clear(&self) {
        let mut cache = self.cache.write().await;
        cache.clear();
    }

    /// Get cache statistics
    pub async fn stats(&self) -> BroadcastCacheStats {
        let cache = self.cache.read().await;
        BroadcastCacheStats {
            cached_broadcasts: cache.len(),
            total_size_bytes: cache.values().map(|b| b.data.len()).sum(),
        }
    }
}

/// Statistics for broadcast cache
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BroadcastCacheStats {
    /// Number of cached broadcast variables
    pub cached_broadcasts: usize,
    /// Total size of cached data in bytes
    pub total_size_bytes: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_broadcast_variable() {
        let value = vec![1, 2, 3, 4, 5];
        let broadcast = BroadcastVariable::new(value.clone());

        assert_eq!(broadcast.value(), &value);
        assert!(!broadcast.id().0.is_empty());
    }

    #[tokio::test]
    async fn test_serializable_broadcast() {
        let value = vec![1, 2, 3, 4, 5];
        let broadcast = BroadcastVariable::new(value.clone());

        let serializable = SerializableBroadcast::from_broadcast(&broadcast).unwrap();
        let deserialized: BroadcastVariable<Vec<i32>> = serializable.to_broadcast().unwrap();

        assert_eq!(deserialized.value(), &value);
        assert_eq!(deserialized.id(), broadcast.id());
    }

    #[tokio::test]
    async fn test_broadcast_manager() {
        let manager = BroadcastManager::new();
        let value = vec![1, 2, 3, 4, 5];
        let broadcast = BroadcastVariable::new(value.clone());

        // Register broadcast
        manager.register_broadcast(&broadcast).await.unwrap();

        // Get broadcast
        let retrieved = manager.get_broadcast(broadcast.id()).await.unwrap();
        let deserialized: BroadcastVariable<Vec<i32>> = retrieved.to_broadcast().unwrap();
        assert_eq!(deserialized.value(), &value);

        // List broadcasts
        let broadcasts = manager.list_broadcasts().await;
        assert_eq!(broadcasts.len(), 1);
        assert_eq!(broadcasts[0], *broadcast.id());

        // Remove broadcast
        assert!(manager.remove_broadcast(broadcast.id()).await);
        assert!(manager.get_broadcast(broadcast.id()).await.is_none());
    }

    #[tokio::test]
    async fn test_broadcast_cache() {
        let cache = BroadcastCache::new();
        let value = vec![1, 2, 3, 4, 5];
        let broadcast = BroadcastVariable::new(value.clone());
        let serializable = SerializableBroadcast::from_broadcast(&broadcast).unwrap();

        // Cache broadcast
        cache.cache_broadcast(serializable.clone()).await;

        // Check if cached
        assert!(cache.contains(broadcast.id()).await);

        // Get cached broadcast
        let retrieved = cache.get_broadcast(broadcast.id()).await.unwrap();
        let deserialized: BroadcastVariable<Vec<i32>> = retrieved.to_broadcast().unwrap();
        assert_eq!(deserialized.value(), &value);

        // Get stats
        let stats = cache.stats().await;
        assert_eq!(stats.cached_broadcasts, 1);
        assert!(stats.total_size_bytes > 0);

        // Remove from cache
        assert!(cache.remove_broadcast(broadcast.id()).await);
        assert!(!cache.contains(broadcast.id()).await);
    }
}
