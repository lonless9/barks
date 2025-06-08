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

impl<K: Hash + Send + Sync> Partitioner<K> for HashPartitioner {
    fn num_partitions(&self) -> u32 {
        self.num_partitions
    }

    fn get_partition(&self, key: &K) -> u32 {
        let mut s = std::collections::hash_map::DefaultHasher::new();
        self.seed.hash(&mut s);
        key.hash(&mut s);
        (s.finish() % self.num_partitions as u64) as u32
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_partitioner() {
        let partitioner = HashPartitioner::new(10);
        assert_eq!(
            <HashPartitioner as Partitioner<String>>::num_partitions(&partitioner),
            10
        );

        let key1 = "hello".to_string();
        let key2 = "world".to_string();

        let p1 = <HashPartitioner as Partitioner<String>>::get_partition(&partitioner, &key1);
        let p2 = <HashPartitioner as Partitioner<String>>::get_partition(&partitioner, &key2);
        let p1_again = <HashPartitioner as Partitioner<String>>::get_partition(&partitioner, &key1);

        assert_eq!(p1, p1_again);
        assert_ne!(p1, p2);
        assert!(p1 < 10);
        assert!(p2 < 10);
    }

    #[test]
    fn test_hash_partitioner_with_seed() {
        let partitioner1 = HashPartitioner::new(10);
        let partitioner2 = HashPartitioner::with_seed(10, 12345);

        let key = "test_key";
        let p1 = <HashPartitioner as Partitioner<&str>>::get_partition(&partitioner1, &key);
        let p2 = <HashPartitioner as Partitioner<&str>>::get_partition(&partitioner2, &key);

        // With very high probability, different seeds should produce different partitions
        assert_ne!(p1, p2);
    }

    #[test]
    #[should_panic]
    fn test_hash_partitioner_zero_partitions() {
        HashPartitioner::new(0);
    }

    #[test]
    fn test_range_partitioner_from_sample() {
        let samples = vec![10, 20, 30, 40, 50, 60, 70, 80, 90, 100];
        let partitioner = RangePartitioner::from_sample(5, samples);

        assert_eq!(
            <RangePartitioner<i32> as Partitioner<i32>>::num_partitions(&partitioner),
            5
        );

        // Bounds should be [30, 50, 70, 90] (step=2, indices 2,4,6,8)
        assert_eq!(
            <RangePartitioner<i32> as Partitioner<i32>>::get_partition(&partitioner, &5),
            0
        );
        assert_eq!(
            <RangePartitioner<i32> as Partitioner<i32>>::get_partition(&partitioner, &30),
            0
        ); // a key equal to a bound goes to the lower partition
        assert_eq!(
            <RangePartitioner<i32> as Partitioner<i32>>::get_partition(&partitioner, &31),
            1
        );
        assert_eq!(
            <RangePartitioner<i32> as Partitioner<i32>>::get_partition(&partitioner, &50),
            1
        );
        assert_eq!(
            <RangePartitioner<i32> as Partitioner<i32>>::get_partition(&partitioner, &51),
            2
        );
        assert_eq!(
            <RangePartitioner<i32> as Partitioner<i32>>::get_partition(&partitioner, &101),
            4
        );
    }

    #[test]
    fn test_range_partitioner_empty_sample() {
        let partitioner = RangePartitioner::<i32>::from_sample(5, vec![]);
        assert_eq!(
            <RangePartitioner<i32> as Partitioner<i32>>::num_partitions(&partitioner),
            5
        );
        assert_eq!(
            <RangePartitioner<i32> as Partitioner<i32>>::get_partition(&partitioner, &100),
            0
        ); // everything goes to partition 0
    }

    #[test]
    fn test_custom_partitioner() {
        let partitioner = CustomPartitioner::new(4, |key: &String| key.len() as u32);

        assert_eq!(
            <CustomPartitioner<String> as Partitioner<String>>::num_partitions(&partitioner),
            4
        );
        assert_eq!(
            <CustomPartitioner<String> as Partitioner<String>>::get_partition(
                &partitioner,
                &"a".to_string()
            ),
            1 % 4
        ); // len 1
        assert_eq!(
            <CustomPartitioner<String> as Partitioner<String>>::get_partition(
                &partitioner,
                &"abcd".to_string()
            ),
            4 % 4
        ); // len 4 -> partition 0
    }
}
