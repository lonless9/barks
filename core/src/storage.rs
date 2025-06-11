//! Defines storage levels for caching distributed datasets.

use barks_common::error::{CommonError, Result};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// Defines the storage level for a `DistributedDataset`.
///
/// This specifies how a dataset's partitions should be cached. It mirrors the
/// storage levels available in Apache Spark.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageLevel {
    pub use_disk: bool,
    pub use_memory: bool,
    pub use_off_heap: bool,
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
        use_off_heap: false,
        deserialized: false,
        replication: 1,
    };
    /// Cache in memory as `RecordBatch`.
    pub const MEMORY_ONLY: StorageLevel = StorageLevel {
        use_disk: false,
        use_memory: true,
        use_off_heap: false,
        deserialized: true,
        replication: 1,
    };
    /// Cache on disk only.
    pub const DISK_ONLY: StorageLevel = StorageLevel {
        use_disk: true,
        use_memory: false,
        use_off_heap: false,
        deserialized: false,
        replication: 1,
    };
    /// Cache in memory and on disk.
    pub const MEMORY_AND_DISK: StorageLevel = StorageLevel {
        use_disk: true,
        use_memory: true,
        use_off_heap: false,
        deserialized: true,
        replication: 1,
    };
    // Other levels can be added here, e.g., _SER for serialized, _2 for replication > 1.
}

impl Default for StorageLevel {
    /// The default storage level is in-memory as deserialized objects.
    fn default() -> Self {
        Self::MEMORY_ONLY
    }
}

impl fmt::Display for StorageLevel {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "StorageLevel(disk={}, memory={}, off_heap={}, deserialized={}, replication={})",
            self.use_disk, self.use_memory, self.use_off_heap, self.deserialized, self.replication
        )
    }
}

impl FromStr for StorageLevel {
    type Err = CommonError;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_ascii_uppercase().as_str() {
            "NONE" => Ok(Self {
                use_disk: false,
                use_memory: false,
                use_off_heap: false,
                deserialized: false,
                replication: 1,
            }),
            "DISK_ONLY" => Ok(Self {
                use_disk: true,
                use_memory: false,
                use_off_heap: false,
                deserialized: false,
                replication: 1,
            }),
            "DISK_ONLY_2" => Ok(Self {
                use_disk: true,
                use_memory: false,
                use_off_heap: false,
                deserialized: false,
                replication: 2,
            }),
            "DISK_ONLY_3" => Ok(Self {
                use_disk: true,
                use_memory: false,
                use_off_heap: false,
                deserialized: false,
                replication: 3,
            }),
            "MEMORY_ONLY" => Ok(Self {
                use_disk: false,
                use_memory: true,
                use_off_heap: false,
                deserialized: true,
                replication: 1,
            }),
            "MEMORY_ONLY_2" => Ok(Self {
                use_disk: false,
                use_memory: true,
                use_off_heap: false,
                deserialized: true,
                replication: 2,
            }),
            "MEMORY_ONLY_SER" => Ok(Self {
                use_disk: false,
                use_memory: true,
                use_off_heap: false,
                deserialized: false,
                replication: 1,
            }),
            "MEMORY_ONLY_SER_2" => Ok(Self {
                use_disk: false,
                use_memory: true,
                use_off_heap: false,
                deserialized: false,
                replication: 2,
            }),
            "MEMORY_AND_DISK" => Ok(Self {
                use_disk: true,
                use_memory: true,
                use_off_heap: false,
                deserialized: true,
                replication: 1,
            }),
            "MEMORY_AND_DISK_2" => Ok(Self {
                use_disk: true,
                use_memory: true,
                use_off_heap: false,
                deserialized: true,
                replication: 2,
            }),
            "MEMORY_AND_DISK_SER" => Ok(Self {
                use_disk: true,
                use_memory: true,
                use_off_heap: false,
                deserialized: false,
                replication: 1,
            }),
            "MEMORY_AND_DISK_SER_2" => Ok(Self {
                use_disk: true,
                use_memory: true,
                use_off_heap: false,
                deserialized: false,
                replication: 2,
            }),
            "OFF_HEAP" => Ok(Self {
                use_disk: false,
                use_memory: true,
                use_off_heap: true,
                deserialized: false,
                replication: 1,
            }),
            _ => Err(CommonError::invalid(format!(
                "invalid storage level: {}",
                s
            ))),
        }
    }
}
