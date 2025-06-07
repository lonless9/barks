//! RDD transformation implementations.

use crate::traits::{Data, Dependency, Partition, Rdd, RddBase, RddResult};
use std::sync::Arc;

/// RDD that applies a map transformation
#[derive(Clone)]
pub struct MapRdd<T: Data, U: Data> {
    id: usize,
    parent_rdd: Arc<dyn Rdd<T>>,
    func: Arc<dyn Fn(T) -> U + Send + Sync>,
}

impl<T: Data, U: Data> std::fmt::Debug for MapRdd<T, U> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MapRdd")
            .field("id", &self.id)
            .field("func", &"<map_function>")
            .finish()
    }
}

impl<T: Data, U: Data> MapRdd<T, U> {
    pub fn new(
        id: usize,
        parent: Arc<dyn Rdd<T>>,
        func: Arc<dyn Fn(T) -> U + Send + Sync>,
    ) -> Self {
        Self {
            id,
            parent_rdd: parent,
            func,
        }
    }
}

impl<T: Data, U: Data> RddBase for MapRdd<T, U> {
    fn id(&self) -> usize {
        self.id
    }

    fn num_partitions(&self) -> usize {
        self.parent_rdd.num_partitions()
    }

    fn dependencies(&self) -> Vec<Dependency> {
        // For now, we'll create a placeholder dependency
        // In a real implementation, we'd need to handle the trait upcasting properly
        vec![]
    }
}

impl<T: Data, U: Data> Rdd<U> for MapRdd<T, U> {
    fn compute(&self, partition: &dyn Partition) -> RddResult<Box<dyn Iterator<Item = U>>> {
        let parent_iter = self.parent_rdd.compute(partition)?;
        let func = self.func.clone();
        Ok(Box::new(parent_iter.map(move |item| (func)(item))))
    }
}

/// RDD that applies a filter transformation
#[derive(Clone)]
pub struct FilterRdd<T: Data> {
    id: usize,
    parent_rdd: Arc<dyn Rdd<T>>,
    predicate: Arc<dyn Fn(&T) -> bool + Send + Sync>,
}

impl<T: Data> std::fmt::Debug for FilterRdd<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilterRdd")
            .field("id", &self.id)
            .field("predicate", &"<filter_predicate>")
            .finish()
    }
}

impl<T: Data> FilterRdd<T> {
    pub fn new(
        id: usize,
        parent: Arc<dyn Rdd<T>>,
        predicate: Arc<dyn Fn(&T) -> bool + Send + Sync>,
    ) -> Self {
        Self {
            id,
            parent_rdd: parent,
            predicate,
        }
    }
}

impl<T: Data> RddBase for FilterRdd<T> {
    fn id(&self) -> usize {
        self.id
    }

    fn num_partitions(&self) -> usize {
        self.parent_rdd.num_partitions()
    }

    fn dependencies(&self) -> Vec<Dependency> {
        // For now, we'll create a placeholder dependency
        // In a real implementation, we'd need to handle the trait upcasting properly
        vec![]
    }
}

impl<T: Data> Rdd<T> for FilterRdd<T> {
    fn compute(&self, partition: &dyn Partition) -> RddResult<Box<dyn Iterator<Item = T>>> {
        let parent_iter = self.parent_rdd.compute(partition)?;
        let predicate = self.predicate.clone();
        Ok(Box::new(parent_iter.filter(move |item| (predicate)(item))))
    }
}
