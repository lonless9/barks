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
    pub compute_fn: ComputeFn<T>,
}

/// Type alias for complex compute function type
pub type ComputeFn<T> = Arc<dyn Fn(&dyn Partition) -> RddResult<Vec<T>> + Send + Sync>;

impl<T> Debug for Task<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Task")
            .field("partition", &self.partition)
            .field("compute_fn", &"<function>")
            .finish()
    }
}

impl<T> Task<T> {
    pub fn new(partition: Box<dyn Partition>, compute_fn: ComputeFn<T>) -> Self {
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
    pub fn with_default_threads() -> Self {
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
        Self::with_default_threads()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::BasicPartition;

    fn create_test_tasks(num_tasks: usize) -> Vec<Task<i32>> {
        (0..num_tasks)
            .map(|i| {
                let compute_fn = Arc::new(move |p: &dyn Partition| {
                    Ok(vec![(p.index() * 10) as i32, (p.index() * 10 + 1) as i32])
                });
                Task::new(Box::new(BasicPartition::new(i)), compute_fn)
            })
            .collect()
    }

    #[test]
    fn test_local_scheduler_new() {
        let scheduler = LocalScheduler::new(4);
        assert_eq!(scheduler.num_threads(), 4);
    }

    #[test]
    fn test_local_scheduler_with_default_threads() {
        let scheduler = LocalScheduler::with_default_threads();
        assert!(scheduler.num_threads() > 0);
    }

    #[test]
    fn test_execute_tasks() {
        let scheduler = LocalScheduler::default();
        let tasks = create_test_tasks(3);
        let results = scheduler.execute_tasks(tasks).unwrap();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0], vec![0, 1]);
        assert_eq!(results[1], vec![10, 11]);
        assert_eq!(results[2], vec![20, 21]);
    }

    #[test]
    fn test_execute_and_collect() {
        let scheduler = LocalScheduler::default();
        let tasks = create_test_tasks(3);
        let result = scheduler.execute_and_collect(tasks).unwrap();

        assert_eq!(result, vec![0, 1, 10, 11, 20, 21]);
    }

    #[test]
    fn test_execute_and_count() {
        let scheduler = LocalScheduler::default();
        let tasks = create_test_tasks(5);
        let count = scheduler.execute_and_count(tasks).unwrap();

        assert_eq!(count, 10); // 5 tasks * 2 items/task
    }

    #[test]
    fn test_execute_and_reduce() {
        let scheduler = LocalScheduler::default();
        let tasks = create_test_tasks(4); // [0,1], [10,11], [20,21], [30,31]
        let sum = scheduler
            .execute_and_reduce(tasks, 0, |acc, item| acc + item, |a, b| a + b)
            .unwrap();

        // Sum = (0+1) + (10+11) + (20+21) + (30+31) = 1 + 21 + 41 + 61 = 124
        assert_eq!(sum, 124);
    }

    #[test]
    fn test_execute_foreach() {
        let scheduler = LocalScheduler::default();
        let tasks = create_test_tasks(3);
        let sum = Arc::new(std::sync::atomic::AtomicI32::new(0));

        let sum_clone = sum.clone();
        scheduler
            .execute_foreach(tasks, move |item| {
                sum_clone.fetch_add(*item, std::sync::atomic::Ordering::SeqCst);
            })
            .unwrap();

        assert_eq!(sum.load(std::sync::atomic::Ordering::SeqCst), 63); // (0+1) + (10+11) + (20+21) = 1 + 21 + 41 = 63
    }
}
