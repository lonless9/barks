//! RDD Actions
//!
//! This module contains action implementations for RDDs.
//! Actions trigger computation and return results to the driver program.

use crate::rdd::SimpleRdd;
use crate::traits::RddResult;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

impl<T> SimpleRdd<T>
where
    T: Send + Sync + Clone + Serialize + for<'de> Deserialize<'de> + Debug + 'static,
{
    /// Collect all elements of the RDD into a vector
    /// Automatically uses parallel execution when multiple partitions exist
    pub fn collect(&self) -> RddResult<Vec<T>> {
        let partitions = self.partitions();

        if partitions.len() <= 1 {
            // Single partition: use sequential execution
            let mut result = Vec::new();
            for partition in partitions {
                let partition_data = self.compute(partition.as_ref())?;
                result.extend(partition_data);
            }
            Ok(result)
        } else {
            // Multiple partitions: use parallel execution with rayon
            let results: Result<Vec<Vec<T>>, _> = partitions
                .into_par_iter()
                .map(|partition| self.compute(partition.as_ref()))
                .collect();

            let partition_results = results?;
            let mut result = Vec::new();
            for partition_data in partition_results {
                result.extend(partition_data);
            }
            Ok(result)
        }
    }

    /// Count the number of elements in the RDD
    /// Automatically uses parallel execution when multiple partitions exist
    pub fn count(&self) -> RddResult<usize> {
        let partitions = self.partitions();

        if partitions.len() <= 1 {
            // Single partition: use sequential execution
            let mut total = 0;
            for partition in partitions {
                let partition_data = self.compute(partition.as_ref())?;
                total += partition_data.len();
            }
            Ok(total)
        } else {
            // Multiple partitions: use parallel execution with rayon
            let total: Result<usize, _> = partitions
                .into_par_iter()
                .map(|partition| {
                    let partition_data = self.compute(partition.as_ref())?;
                    Ok(partition_data.len())
                })
                .sum();
            total
        }
    }

    /// Take the first n elements of the RDD
    pub fn take(&self, n: usize) -> RddResult<Vec<T>> {
        let mut result = Vec::new();
        let partitions = self.partitions();

        for partition in partitions {
            if result.len() >= n {
                break;
            }

            let partition_data = self.compute(partition.as_ref())?;
            let remaining = n - result.len();

            if partition_data.len() <= remaining {
                result.extend(partition_data);
            } else {
                result.extend(partition_data.into_iter().take(remaining));
            }
        }

        Ok(result)
    }

    /// Get the first element of the RDD
    pub fn first(&self) -> RddResult<Option<T>> {
        let partitions = self.partitions();

        for partition in partitions {
            let partition_data = self.compute(partition.as_ref())?;
            if let Some(first_element) = partition_data.into_iter().next() {
                return Ok(Some(first_element));
            }
        }

        Ok(None)
    }

    /// Reduce the elements of the RDD using a binary operator
    /// Automatically uses parallel execution when multiple partitions exist
    pub fn reduce<F>(&self, f: F) -> RddResult<Option<T>>
    where
        F: Fn(T, T) -> T + Send + Sync,
    {
        let partitions = self.partitions();

        if partitions.len() <= 1 {
            // Single partition: use sequential execution
            let collected = self.collect()?;
            if collected.is_empty() {
                return Ok(None);
            }

            let mut iter = collected.into_iter();
            let first = iter.next().unwrap();
            let result = iter.fold(first, &f);
            Ok(Some(result))
        } else {
            // Multiple partitions: use parallel execution with rayon
            // Compute each partition in parallel and reduce within each partition
            let partition_results: Result<Vec<Option<T>>, _> = partitions
                .into_par_iter()
                .map(|partition| {
                    let partition_data = self.compute(partition.as_ref())?;
                    if partition_data.is_empty() {
                        return Ok(None);
                    }

                    let mut iter = partition_data.into_iter();
                    let first = iter.next().unwrap();
                    let result = iter.fold(first, &f);
                    Ok(Some(result))
                })
                .collect();

            let results = partition_results?;

            // Reduce the partition results
            let non_empty_results: Vec<T> = results.into_iter().flatten().collect();
            if non_empty_results.is_empty() {
                return Ok(None);
            }

            let mut iter = non_empty_results.into_iter();
            let first = iter.next().unwrap();
            let final_result = iter.fold(first, &f);
            Ok(Some(final_result))
        }
    }

    /// Apply a function to each element of the RDD (for side effects)
    /// Automatically uses parallel execution when multiple partitions exist
    pub fn foreach<F>(&self, f: F) -> RddResult<()>
    where
        F: Fn(&T) + Send + Sync,
    {
        let partitions = self.partitions();

        if partitions.len() <= 1 {
            // Single partition: use sequential execution
            for partition in partitions {
                let partition_data = self.compute(partition.as_ref())?;
                partition_data.iter().for_each(&f);
            }
            Ok(())
        } else {
            // Multiple partitions: use parallel execution with rayon
            let results: Result<Vec<()>, _> = partitions
                .into_par_iter()
                .map(|partition| {
                    let partition_data = self.compute(partition.as_ref())?;
                    partition_data.iter().for_each(&f);
                    Ok(())
                })
                .collect();

            results?;
            Ok(())
        }
    }
}
