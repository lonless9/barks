//! Local Task Scheduler
//!
//! This module provides a local task scheduler that uses Rayon's thread pool
//! to execute RDD operations in parallel on a single machine.

use crate::traits::{Partition, RddError, RddResult};
use rayon::prelude::*;
use std::fmt::Debug;
use std::sync::Arc;

/// Task represents a unit of work to be executed
pub struct Task<T> {
    pub partition: Box<dyn Partition>,
    pub compute_fn: Arc<dyn Fn(&dyn Partition) -> RddResult<Vec<T>> + Send + Sync>,
}

impl<T> Debug for Task<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Task")
            .field("partition", &self.partition)
            .field("compute_fn", &"<function>")
            .finish()
    }
}

impl<T> Task<T> {
    pub fn new(
        partition: Box<dyn Partition>,
        compute_fn: Arc<dyn Fn(&dyn Partition) -> RddResult<Vec<T>> + Send + Sync>,
    ) -> Self {
        Self {
            partition,
            compute_fn,
        }
    }

    pub fn execute(&self) -> RddResult<Vec<T>> {
        (self.compute_fn)(self.partition.as_ref())
    }
}

/// LocalScheduler manages parallel execution of tasks using Rayon
#[derive(Debug)]
pub struct LocalScheduler {
    /// Number of threads in the thread pool
    num_threads: usize,
}

impl LocalScheduler {
    /// Create a new LocalScheduler with the specified number of threads
    pub fn new(num_threads: usize) -> Self {
        Self { num_threads }
    }

    /// Create a new LocalScheduler with the default number of threads (CPU cores)
    pub fn default() -> Self {
        Self {
            num_threads: rayon::current_num_threads(),
        }
    }

    /// Get the number of threads
    pub fn num_threads(&self) -> usize {
        self.num_threads
    }

    /// Execute a collection of tasks in parallel
    pub fn execute_tasks<T>(&self, tasks: Vec<Task<T>>) -> RddResult<Vec<Vec<T>>>
    where
        T: Send + Sync + Debug,
    {
        // Use rayon to execute tasks in parallel
        let results: Result<Vec<Vec<T>>, RddError> =
            tasks.into_par_iter().map(|task| task.execute()).collect();

        results
    }

    /// Execute a collection of tasks and collect all results into a single vector
    pub fn execute_and_collect<T>(&self, tasks: Vec<Task<T>>) -> RddResult<Vec<T>>
    where
        T: Send + Sync + Debug,
    {
        let partition_results = self.execute_tasks(tasks)?;
        let mut result = Vec::new();
        for partition_data in partition_results {
            result.extend(partition_data);
        }
        Ok(result)
    }

    /// Execute a collection of tasks and reduce the results using a fold operation
    pub fn execute_and_reduce<T, R, F, G>(
        &self,
        tasks: Vec<Task<T>>,
        identity: R,
        fold_fn: F,
        reduce_fn: G,
    ) -> RddResult<R>
    where
        T: Send + Sync + Debug,
        R: Send + Sync + Clone + Debug,
        F: Fn(R, T) -> R + Send + Sync,
        G: Fn(R, R) -> R + Send + Sync,
    {
        let results: Result<R, RddError> = tasks
            .into_par_iter()
            .map(|task| {
                let partition_data = task.execute()?;
                Ok(partition_data.into_iter().fold(identity.clone(), &fold_fn))
            })
            .reduce(
                || Ok(identity.clone()),
                |acc, item| match (acc, item) {
                    (Ok(a), Ok(b)) => Ok(reduce_fn(a, b)),
                    (Err(e), _) | (_, Err(e)) => Err(e),
                },
            );

        results
    }

    /// Execute a collection of tasks and count the total number of elements
    pub fn execute_and_count<T>(&self, tasks: Vec<Task<T>>) -> RddResult<usize>
    where
        T: Send + Sync + Debug,
    {
        let results: Result<usize, RddError> = tasks
            .into_par_iter()
            .map(|task| {
                let partition_data = task.execute()?;
                Ok(partition_data.len())
            })
            .sum();

        results
    }

    /// Execute a collection of tasks with a side effect function (foreach)
    pub fn execute_foreach<T, F>(&self, tasks: Vec<Task<T>>, foreach_fn: F) -> RddResult<()>
    where
        T: Send + Sync + Debug,
        F: Fn(&T) + Send + Sync,
    {
        let results: Result<(), RddError> = tasks
            .into_par_iter()
            .map(|task| {
                let partition_data = task.execute()?;
                partition_data.iter().for_each(&foreach_fn);
                Ok(())
            })
            .collect();

        results
    }
}

impl Default for LocalScheduler {
    fn default() -> Self {
        Self::default()
    }
}
