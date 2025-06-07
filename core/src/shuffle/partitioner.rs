//! Defines partitioners for distributing data in a shuffle.

use std::hash::{Hash, Hasher};

// Re-export the Partitioner trait from the network-shuffle crate
// to make it available within the core logic.
pub use barks_network_shuffle::traits::Partitioner;

/// A partitioner that uses the hash of the key to distribute data.
#[derive(Clone, Debug)]
pub struct HashPartitioner {
    num_partitions: u32,
}

impl HashPartitioner {
    pub fn new(num_partitions: u32) -> Self {
        assert!(num_partitions > 0, "Number of partitions must be positive.");
        Self { num_partitions }
    }
}

impl Partitioner for HashPartitioner {
    fn num_partitions(&self) -> u32 {
        self.num_partitions
    }
}

impl HashPartitioner {
    /// Get the partition for a hashable key
    pub fn get_partition_for<K: HashPartitionable>(&self, key: &K) -> u32 {
        key.get_partition(self.num_partitions)
    }
}

/// A specific partitioner for types that implement Hash
pub trait HashPartitionable: Hash + Send + Sync {
    fn get_partition(&self, num_partitions: u32) -> u32 {
        let mut s = std::collections::hash_map::DefaultHasher::new();
        self.hash(&mut s);
        (s.finish() % num_partitions as u64) as u32
    }
}

// Implement for common types
impl HashPartitionable for i32 {}
impl HashPartitionable for i64 {}
impl HashPartitionable for String {}
impl HashPartitionable for &str {}
