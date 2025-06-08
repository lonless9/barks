//! RDD transformation implementations.

use crate::rdd::ShuffledRdd;
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
        _other: Arc<dyn crate::traits::RddBase<Item = (K, W)>>,
        _partitioner: Arc<dyn Partitioner + 'static>,
    ) -> Arc<dyn crate::traits::RddBase<Item = (K, (V, W))>> {
        // The implementation of join is based on cogroup.
        // For now, this is a placeholder as cogroup itself is a complex shuffle operation.
        unimplemented!(
            "join is not yet implemented. It requires a cogroup shuffle implementation."
        );
    }

    /// Return an RDD with the elements sorted by key.
    fn sort_by_key(self, _ascending: bool) -> Arc<dyn crate::traits::RddBase<Item = (K, V)>>
    where
        K: Ord,
    {
        // A full implementation requires a custom RangePartitioner and sampling the RDD.
        // This is a placeholder for the API.
        unimplemented!("sort_by_key is not yet implemented. It requires a RangePartitioner.");
    }
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
}
