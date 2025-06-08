//! Defines partitioners for distributing data in a shuffle.

use std::hash::{Hash, Hasher};
use std::marker::PhantomData;

// Re-export the Partitioner trait from the network-shuffle crate
// to make it available within the core logic.
pub use barks_network_shuffle::traits::Partitioner;

/// A partitioner that uses the hash of the key to distribute data.
#[derive(Clone, Debug)]
pub struct HashPartitioner {
    num_partitions: u32,
    seed: u64,
}

impl HashPartitioner {
    pub fn new(num_partitions: u32) -> Self {
        assert!(num_partitions > 0, "Number of partitions must be positive.");
        Self {
            num_partitions,
            seed: 0, // Default seed
        }
    }

    pub fn with_seed(num_partitions: u32, seed: u64) -> Self {
        assert!(num_partitions > 0, "Number of partitions must be positive.");
        Self {
            num_partitions,
            seed,
        }
    }
}

impl<K: Hash + Send + Sync + HashPartitionable> Partitioner<K> for HashPartitioner {
    fn num_partitions(&self) -> u32 {
        self.num_partitions
    }

    fn get_partition(&self, key: &K) -> u32 {
        key.get_partition_with_seed(self.num_partitions, self.seed)
    }
}

/// A specific partitioner for types that implement Hash
pub trait HashPartitionable: Hash + Send + Sync {
    fn get_partition(&self, num_partitions: u32) -> u32 {
        self.get_partition_with_seed(num_partitions, 0)
    }

    fn get_partition_with_seed(&self, num_partitions: u32, seed: u64) -> u32 {
        let mut s = std::collections::hash_map::DefaultHasher::new();
        seed.hash(&mut s);
        self.hash(&mut s);
        (s.finish() % num_partitions as u64) as u32
    }
}

// Implement for common types
impl HashPartitionable for i32 {}
impl HashPartitionable for i64 {}
impl HashPartitionable for u32 {}
impl HashPartitionable for u64 {}
impl HashPartitionable for String {}
impl HashPartitionable for &str {}
impl<T: Hash + Send + Sync> HashPartitionable for Vec<T> {}

/// A range partitioner that distributes keys based on sorted ranges.
/// This is essential for sortByKey operations and provides better data locality.
#[derive(Clone, Debug)]
pub struct RangePartitioner<K> {
    num_partitions: u32,
    range_bounds: Vec<K>,
    _phantom: PhantomData<K>,
}

impl<K> RangePartitioner<K>
where
    K: Ord + Clone + Send + Sync + std::fmt::Debug + 'static,
{
    /// Create a new range partitioner with the given bounds.
    /// The bounds should be sorted and have length = num_partitions - 1.
    pub fn new(num_partitions: u32, mut range_bounds: Vec<K>) -> Self {
        assert!(num_partitions > 0, "Number of partitions must be positive.");
        assert_eq!(
            range_bounds.len(),
            (num_partitions - 1) as usize,
            "Range bounds length must be num_partitions - 1"
        );

        range_bounds.sort();
        Self {
            num_partitions,
            range_bounds,
            _phantom: PhantomData,
        }
    }

    /// Create a range partitioner by sampling the given data.
    /// This is the typical way to create a range partitioner in practice.
    pub fn from_sample(num_partitions: u32, mut sample_data: Vec<K>) -> Self {
        assert!(num_partitions > 0, "Number of partitions must be positive.");

        if sample_data.is_empty() || num_partitions == 1 {
            return Self {
                num_partitions,
                range_bounds: Vec::new(),
                _phantom: PhantomData,
            };
        }

        sample_data.sort();
        sample_data.dedup();

        let mut range_bounds = Vec::new();
        let step = sample_data.len() / num_partitions as usize;

        for i in 1..num_partitions {
            let index = (i as usize * step).min(sample_data.len() - 1);
            range_bounds.push(sample_data[index].clone());
        }

        Self {
            num_partitions,
            range_bounds,
            _phantom: PhantomData,
        }
    }

    fn get_partition(&self, key: &K) -> u32 {
        // Binary search to find the appropriate partition
        match self.range_bounds.binary_search(key) {
            Ok(index) => index as u32,
            Err(index) => index as u32,
        }
    }
}

impl<K> Partitioner<K> for RangePartitioner<K>
where
    K: Ord + Clone + Send + Sync + std::fmt::Debug + 'static,
{
    fn num_partitions(&self) -> u32 {
        self.num_partitions
    }

    fn get_partition(&self, key: &K) -> u32 {
        // Binary search to find the appropriate partition
        match self.range_bounds.binary_search(key) {
            Ok(index) => index as u32,
            Err(index) => index as u32,
        }
    }
}

/// A custom partitioner that allows users to define their own partitioning logic.
#[derive(Clone)]
pub struct CustomPartitioner<K> {
    num_partitions: u32,
    partition_func: fn(&K) -> u32,
    _phantom: PhantomData<K>,
}

impl<K> CustomPartitioner<K>
where
    K: Send + Sync + 'static,
{
    pub fn new(num_partitions: u32, partition_func: fn(&K) -> u32) -> Self {
        assert!(num_partitions > 0, "Number of partitions must be positive.");
        Self {
            num_partitions,
            partition_func,
            _phantom: PhantomData,
        }
    }

    pub fn get_partition_for(&self, key: &K) -> u32 {
        let partition = (self.partition_func)(key);
        partition % self.num_partitions
    }

    fn get_partition(&self, key: &K) -> u32 {
        self.get_partition_for(key)
    }
}

impl<K> Partitioner<K> for CustomPartitioner<K>
where
    K: Send + Sync + 'static,
{
    fn num_partitions(&self) -> u32 {
        self.num_partitions
    }

    fn get_partition(&self, key: &K) -> u32 {
        self.get_partition_for(key)
    }
}

impl<K> std::fmt::Debug for CustomPartitioner<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CustomPartitioner")
            .field("num_partitions", &self.num_partitions)
            .field("partition_func", &"<function>")
            .finish()
    }
}
