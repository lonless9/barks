//! Defines shuffle dependencies between RDDs.

use crate::shuffle::{Aggregator, Partitioner};
use crate::traits::{Data, RddBase};
use std::sync::Arc;

/// Represents a dependency on the output of a shuffle stage.
pub struct ShuffleDependency<K: Data, V: Data, C: Data> {
    pub shuffle_id: usize,
    pub rdd: Arc<dyn RddBase<Item = (K, V)>>,
    pub aggregator: Arc<dyn Aggregator<K, V, C>>,
    pub partitioner: Arc<dyn Partitioner<K>>,
}

impl<K: Data, V: Data, C: Data> ShuffleDependency<K, V, C> {
    pub fn new(
        shuffle_id: usize,
        rdd: Arc<dyn RddBase<Item = (K, V)>>,
        aggregator: Arc<dyn Aggregator<K, V, C>>,
        partitioner: Arc<dyn Partitioner<K>>,
    ) -> Self {
        Self {
            shuffle_id,
            rdd,
            aggregator,
            partitioner,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rdd::DistributedRdd;
    use crate::shuffle::{HashPartitioner, ReduceAggregator};
    use crate::traits::IsRdd;

    #[test]
    fn test_shuffle_dependency_creation() {
        let rdd = Arc::new(DistributedRdd::from_vec(vec![("a".to_string(), 1)]));
        let aggregator = Arc::new(ReduceAggregator::new(|a, b| a + b));
        let partitioner = Arc::new(HashPartitioner::new(2));

        let dep = ShuffleDependency::new(1, rdd.clone(), aggregator.clone(), partitioner.clone());

        assert_eq!(dep.shuffle_id, 1);
        assert_eq!(dep.rdd.id(), rdd.id());
        assert_eq!(dep.partitioner.num_partitions(), 2);
    }
}
