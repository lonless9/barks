//! Defines shuffle dependencies between RDDs.

use crate::shuffle::{Aggregator, Partitioner};
use crate::traits::{Data, RddBase};
use std::sync::Arc;

/// Represents a dependency on the output of a shuffle stage.
pub struct ShuffleDependency<K: Data, V: Data, C: Data> {
    pub shuffle_id: usize,
    pub rdd: Arc<dyn RddBase<Item = (K, V)>>,
    pub aggregator: Arc<dyn Aggregator<K, V, C>>,
    pub partitioner: Arc<dyn Partitioner>,
}

impl<K: Data, V: Data, C: Data> ShuffleDependency<K, V, C> {
    pub fn new(
        shuffle_id: usize,
        rdd: Arc<dyn RddBase<Item = (K, V)>>,
        aggregator: Arc<dyn Aggregator<K, V, C>>,
        partitioner: Arc<dyn Partitioner>,
    ) -> Self {
        Self {
            shuffle_id,
            rdd,
            aggregator,
            partitioner,
        }
    }
}
