//! RDD transformation implementations.

use crate::rdd::{CogroupedRdd, JoinedRdd, ShuffledRdd, SortedRdd};
use crate::shuffle::Partitioner;
use crate::traits::Data;
use std::sync::Arc;

/// An extension trait for RDDs of key-value pairs.
pub trait PairRdd<K: Data, V: Data>
where
    Self: Sized,
{
    /// Groups values by key and applies a reduction function.
    /// This is a wide transformation that triggers a shuffle.
    fn reduce_by_key(
        self,
        reduce_func: fn(V, V) -> V,
        partitioner: Arc<dyn Partitioner>,
    ) -> ShuffledRdd<K, V, V>;

    /// Groups all values for a key into a single sequence.
    /// This is a wide transformation that triggers a shuffle.
    fn group_by_key(self, partitioner: Arc<dyn Partitioner + 'static>)
        -> ShuffledRdd<K, V, Vec<V>>;

    /// Return an RDD containing all pairs of elements with matching keys in `self` and `other`.
    fn join<W: Data>(
        self,
        other: Arc<dyn crate::traits::RddBase<Item = (K, W)>>,
        partitioner: Arc<dyn Partitioner + 'static>,
    ) -> JoinedRdd<K, V, W>;

    /// Return an RDD with the elements sorted by key.
    fn sort_by_key(self, ascending: bool) -> SortedRdd<K, V>
    where
        K: Ord + std::fmt::Debug;

    /// Return an RDD that groups data from both RDDs by key.
    /// This is the foundation for join operations.
    fn cogroup<W: Data>(
        self,
        other: Arc<dyn crate::traits::RddBase<Item = (K, W)>>,
        partitioner: Arc<dyn Partitioner + 'static>,
    ) -> CogroupedRdd<K, V, W>;

    /// Combine values with the same key using a custom aggregator.
    fn combine_by_key<C: Data>(
        self,
        aggregator: Arc<dyn crate::shuffle::Aggregator<K, V, C>>,
        partitioner: Arc<dyn Partitioner>,
    ) -> ShuffledRdd<K, V, C>;
}

// Implementation of PairRdd for SimpleRdd with key-value pairs
impl<K: Data, V: Data> PairRdd<K, V> for crate::rdd::SimpleRdd<(K, V)> {
    fn reduce_by_key(
        self,
        reduce_func: fn(V, V) -> V,
        partitioner: Arc<dyn Partitioner>,
    ) -> ShuffledRdd<K, V, V> {
        let aggregator = Arc::new(crate::shuffle::ReduceAggregator::new(reduce_func));
        ShuffledRdd::new(0, Arc::new(self), aggregator, partitioner)
    }

    fn group_by_key(
        self,
        partitioner: Arc<dyn Partitioner + 'static>,
    ) -> ShuffledRdd<K, V, Vec<V>> {
        #[derive(Clone, Debug)]
        struct GroupByAggregator<V: Data>(std::marker::PhantomData<V>);

        impl<K: Data, V: Data> crate::shuffle::Aggregator<K, V, Vec<V>> for GroupByAggregator<V> {
            fn create_combiner(&self, v: V) -> Vec<V> {
                vec![v]
            }
            fn merge_value(&self, mut c: Vec<V>, v: V) -> Vec<V> {
                c.push(v);
                c
            }
            fn merge_combiners(&self, mut c1: Vec<V>, mut c2: Vec<V>) -> Vec<V> {
                c1.append(&mut c2);
                c1
            }
        }

        ShuffledRdd::new(
            0,
            Arc::new(self),
            Arc::new(GroupByAggregator(std::marker::PhantomData)),
            partitioner,
        )
    }

    fn join<W: Data>(
        self,
        other: Arc<dyn crate::traits::RddBase<Item = (K, W)>>,
        partitioner: Arc<dyn Partitioner + 'static>,
    ) -> JoinedRdd<K, V, W> {
        JoinedRdd::new(0, Arc::new(self), other, partitioner)
    }

    fn sort_by_key(self, ascending: bool) -> SortedRdd<K, V>
    where
        K: Ord + std::fmt::Debug,
    {
        // For now, create a sorted RDD with a simple range partitioner
        // In a real implementation, we would sample the data first
        let num_partitions = self.num_partitions() as u32;
        crate::rdd::sorted_rdd::create_sorted_rdd(
            0,
            Arc::new(self),
            num_partitions,
            ascending,
            1000,
        )
    }

    fn cogroup<W: Data>(
        self,
        other: Arc<dyn crate::traits::RddBase<Item = (K, W)>>,
        partitioner: Arc<dyn Partitioner + 'static>,
    ) -> CogroupedRdd<K, V, W> {
        CogroupedRdd::new(0, Arc::new(self), other, partitioner)
    }

    fn combine_by_key<C: Data>(
        self,
        aggregator: Arc<dyn crate::shuffle::Aggregator<K, V, C>>,
        partitioner: Arc<dyn Partitioner>,
    ) -> ShuffledRdd<K, V, C> {
        ShuffledRdd::new(0, Arc::new(self), aggregator, partitioner)
    }
}
