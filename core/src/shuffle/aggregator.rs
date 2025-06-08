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

/// A generic aggregator for combine_by_key operations
#[derive(Clone, Debug)]
pub struct CombineAggregator<V, C> {
    create_combiner: fn(V) -> C,
    merge_value: fn(C, V) -> C,
    merge_combiners: fn(C, C) -> C,
}

impl<V, C> CombineAggregator<V, C> {
    pub fn new(
        create_combiner: fn(V) -> C,
        merge_value: fn(C, V) -> C,
        merge_combiners: fn(C, C) -> C,
    ) -> Self {
        Self {
            create_combiner,
            merge_value,
            merge_combiners,
        }
    }
}

impl<K, V, C> Aggregator<K, V, C> for CombineAggregator<V, C>
where
    K: Send + Sync + Clone + Debug + 'static,
    V: Send + Sync + Clone + Debug + 'static,
    C: Send + Sync + Clone + Debug + 'static,
{
    fn create_combiner(&self, v: V) -> C {
        (self.create_combiner)(v)
    }

    fn merge_value(&self, c: C, v: V) -> C {
        (self.merge_value)(c, v)
    }

    fn merge_combiners(&self, c1: C, c2: C) -> C {
        (self.merge_combiners)(c1, c2)
    }
}

/// Statistical aggregators for common operations
#[derive(Clone, Debug)]
pub struct SumAggregator<V> {
    _phantom: std::marker::PhantomData<V>,
}

impl<V> SumAggregator<V> {
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<V> Default for SumAggregator<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> Aggregator<K, V, V> for SumAggregator<V>
where
    K: Send + Sync + Clone + Debug + 'static,
    V: Send + Sync + Clone + Debug + std::ops::Add<Output = V> + 'static,
{
    fn create_combiner(&self, v: V) -> V {
        v
    }

    fn merge_value(&self, c: V, v: V) -> V {
        c + v
    }

    fn merge_combiners(&self, c1: V, c2: V) -> V {
        c1 + c2
    }
}

/// Count aggregator that counts the number of values per key
#[derive(Clone, Debug)]
pub struct CountAggregator<V> {
    _phantom: std::marker::PhantomData<V>,
}

impl<V> CountAggregator<V> {
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<V> Default for CountAggregator<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> Aggregator<K, V, u64> for CountAggregator<V>
where
    K: Send + Sync + Clone + Debug + 'static,
    V: Send + Sync + Clone + Debug + 'static,
{
    fn create_combiner(&self, _v: V) -> u64 {
        1
    }

    fn merge_value(&self, c: u64, _v: V) -> u64 {
        c + 1
    }

    fn merge_combiners(&self, c1: u64, c2: u64) -> u64 {
        c1 + c2
    }
}

/// Average aggregator that computes the average of values per key
#[derive(Clone, Debug)]
pub struct AverageAggregator<V> {
    _phantom: std::marker::PhantomData<V>,
}

impl<V> AverageAggregator<V> {
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<V> Default for AverageAggregator<V> {
    fn default() -> Self {
        Self::new()
    }
}

/// Combiner for average calculation: (sum, count)
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct AverageCombiner<V> {
    pub sum: V,
    pub count: u64,
}

impl<K, V> Aggregator<K, V, AverageCombiner<V>> for AverageAggregator<V>
where
    K: Send + Sync + Clone + Debug + 'static,
    V: Send + Sync + Clone + Debug + std::ops::Add<Output = V> + 'static,
{
    fn create_combiner(&self, v: V) -> AverageCombiner<V> {
        AverageCombiner { sum: v, count: 1 }
    }

    fn merge_value(&self, c: AverageCombiner<V>, v: V) -> AverageCombiner<V> {
        AverageCombiner {
            sum: c.sum + v,
            count: c.count + 1,
        }
    }

    fn merge_combiners(
        &self,
        c1: AverageCombiner<V>,
        c2: AverageCombiner<V>,
    ) -> AverageCombiner<V> {
        AverageCombiner {
            sum: c1.sum + c2.sum,
            count: c1.count + c2.count,
        }
    }
}

/// Group-by-key aggregator that collects all values for a key into a vector
#[derive(Clone, Debug)]
pub struct GroupByKeyAggregator<V> {
    _phantom: std::marker::PhantomData<V>,
}

impl<V> GroupByKeyAggregator<V> {
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<V> Default for GroupByKeyAggregator<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> Aggregator<K, V, Vec<V>> for GroupByKeyAggregator<V>
where
    K: Send + Sync + Clone + Debug + 'static,
    V: Send + Sync + Clone + Debug + 'static,
{
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
