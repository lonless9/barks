//! RDD Actions
//!
//! This module contains action implementations for RDDs.
//! Actions trigger computation and return results to the driver program.

use crate::rdd::SimpleRdd;
use crate::traits::RddResult;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

impl<T> SimpleRdd<T>
where
    T: Send + Sync + Clone + Serialize + for<'de> Deserialize<'de> + Debug + 'static,
{
    /// Collect all elements of the RDD into a vector
    pub fn collect(&self) -> RddResult<Vec<T>> {
        let mut result = Vec::new();
        let partitions = self.partitions();

        for partition in partitions {
            let partition_data = self.compute(partition.as_ref())?;
            result.extend(partition_data);
        }

        Ok(result)
    }

    /// Count the number of elements in the RDD
    pub fn count(&self) -> RddResult<usize> {
        let mut total = 0;
        let partitions = self.partitions();

        for partition in partitions {
            let partition_data = self.compute(partition.as_ref())?;
            total += partition_data.len();
        }

        Ok(total)
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
}
