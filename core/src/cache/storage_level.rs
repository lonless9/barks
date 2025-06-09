//! Storage levels for RDD caching

use serde::{Deserialize, Serialize};

/// Defines how an RDD should be cached
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum StorageLevel {
    /// No caching
    #[default]
    None,
    /// Cache in memory only
    MemoryOnly,
    /// Cache on disk only
    DiskOnly,
    /// Cache in memory, spill to disk if memory is full
    MemoryAndDisk,
    /// Cache in memory only, with serialization
    MemoryOnlySer,
    /// Cache on disk only, with serialization
    DiskOnlySer,
    /// Cache in memory and disk, with serialization
    MemoryAndDiskSer,
    /// Cache in memory with 2x replication
    MemoryOnly2,
    /// Cache in memory and disk with 2x replication
    MemoryAndDisk2,
}

impl StorageLevel {
    /// Check if this storage level uses memory
    pub fn use_memory(&self) -> bool {
        matches!(
            self,
            StorageLevel::MemoryOnly
                | StorageLevel::MemoryAndDisk
                | StorageLevel::MemoryOnlySer
                | StorageLevel::MemoryAndDiskSer
                | StorageLevel::MemoryOnly2
                | StorageLevel::MemoryAndDisk2
        )
    }

    /// Check if this storage level uses disk
    pub fn use_disk(&self) -> bool {
        matches!(
            self,
            StorageLevel::DiskOnly
                | StorageLevel::MemoryAndDisk
                | StorageLevel::DiskOnlySer
                | StorageLevel::MemoryAndDiskSer
                | StorageLevel::MemoryAndDisk2
        )
    }

    /// Check if this storage level uses serialization
    pub fn use_serialization(&self) -> bool {
        matches!(
            self,
            StorageLevel::MemoryOnlySer
                | StorageLevel::DiskOnlySer
                | StorageLevel::MemoryAndDiskSer
        )
    }

    /// Get the replication factor
    pub fn replication(&self) -> u32 {
        match self {
            StorageLevel::MemoryOnly2 | StorageLevel::MemoryAndDisk2 => 2,
            StorageLevel::None => 0,
            _ => 1,
        }
    }

    /// Check if caching is enabled
    pub fn is_cached(&self) -> bool {
        !matches!(self, StorageLevel::None)
    }
}

/// Cache statistics for monitoring
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CacheStats {
    /// Number of cache hits
    pub hits: u64,
    /// Number of cache misses
    pub misses: u64,
    /// Total memory used by cache (in bytes)
    pub memory_used: u64,
    /// Total disk used by cache (in bytes)
    pub disk_used: u64,
    /// Number of cached partitions
    pub cached_partitions: u64,
    /// Number of evicted partitions
    pub evicted_partitions: u64,
}

impl CacheStats {
    /// Calculate cache hit ratio
    pub fn hit_ratio(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }

    /// Record a cache hit
    pub fn record_hit(&mut self) {
        self.hits += 1;
    }

    /// Record a cache miss
    pub fn record_miss(&mut self) {
        self.misses += 1;
    }

    /// Update memory usage
    pub fn update_memory_usage(&mut self, bytes: u64) {
        self.memory_used = bytes;
    }

    /// Update disk usage
    pub fn update_disk_usage(&mut self, bytes: u64) {
        self.disk_used = bytes;
    }

    /// Record a partition being cached
    pub fn record_cached_partition(&mut self) {
        self.cached_partitions += 1;
    }

    /// Record a partition being evicted
    pub fn record_evicted_partition(&mut self) {
        self.evicted_partitions += 1;
        if self.cached_partitions > 0 {
            self.cached_partitions -= 1;
        }
    }
}
