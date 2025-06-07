//! Defines the Aggregator trait for combining values in shuffle operations.

use std::fmt::Debug;

/// Aggregator trait for combining values for a key.
/// Used in operations like `reduceByKey` and `combineByKey`.
///
/// K: Key type
/// V: Input value type
/// C: Combiner (intermediate/output) type
///
/// Note: We use a trait object approach without typetag for generics
/// since typetag doesn't support generic trait deserialization
pub trait Aggregator<K, V, C>: Send + Sync + Debug {
    /// Create a combiner from the first value for a key.
    fn create_combiner(&self, v: V) -> C;

    /// Merge a new value into an existing combiner.
    fn merge_value(&self, c: C, v: V) -> C;

    /// Merge two combiners.
    fn merge_combiners(&self, c1: C, c2: C) -> C;
}

/// A simple aggregator for reduceByKey operations where the combiner type is the same as the value type
#[derive(Clone, Debug)]
pub struct ReduceAggregator<V> {
    reduce_func: fn(V, V) -> V,
}

impl<V> ReduceAggregator<V> {
    pub fn new(reduce_func: fn(V, V) -> V) -> Self {
        Self { reduce_func }
    }
}

impl<K, V> Aggregator<K, V, V> for ReduceAggregator<V>
where
    K: Send + Sync + Clone + Debug + 'static,
    V: Send + Sync + Clone + Debug + 'static,
{
    fn create_combiner(&self, v: V) -> V {
        v
    }

    fn merge_value(&self, c: V, v: V) -> V {
        (self.reduce_func)(c, v)
    }

    fn merge_combiners(&self, c1: V, c2: V) -> V {
        (self.reduce_func)(c1, c2)
    }
}
