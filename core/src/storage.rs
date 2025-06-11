//! Defines storage levels for caching distributed datasets.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Defines the storage level for a `DistributedDataset`.
///
/// This specifies how a dataset's partitions should be cached. It mirrors the
/// storage levels available in Apache Spark.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageLevel {
    pub use_disk: bool,
    pub use_memory: bool,
    /// In Spark, this means storing objects as deserialized Java objects.
    /// Here, it means storing as `RecordBatch` in memory. If false, it would
    /// imply a serialized format.
    pub deserialized: bool,
    pub replication: u16,
}

impl StorageLevel {
    /// No caching.
    pub const NONE: StorageLevel = StorageLevel {
        use_disk: false,
        use_memory: false,
        deserialized: false,
        replication: 1,
    };
    /// Cache in memory as `RecordBatch`.
    pub const MEMORY_ONLY: StorageLevel = StorageLevel {
        use_disk: false,
        use_memory: true,
        deserialized: true,
        replication: 1,
    };
    /// Cache on disk only.
    pub const DISK_ONLY: StorageLevel = StorageLevel {
        use_disk: true,
        use_memory: false,
        deserialized: false,
        replication: 1,
    };
    /// Cache in memory and on disk.
    pub const MEMORY_AND_DISK: StorageLevel = StorageLevel {
        use_disk: true,
        use_memory: true,
        deserialized: true,
        replication: 1,
    };
    // Other levels can be added here, e.g., _SER for serialized, _2 for replication > 1.
}

impl Default for StorageLevel {
    /// The default storage level is no caching.
    fn default() -> Self {
        Self::NONE
    }
}

impl fmt::Display for StorageLevel {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "StorageLevel(disk={}, memory={}, deserialized={}, replication={})",
            self.use_disk, self.use_memory, self.deserialized, self.replication
        )
    }
}
