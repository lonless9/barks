//! Collection utilities for Barks
//!
//! This module provides utility functions and types for working with collections.

use std::collections::HashMap;
use std::hash::Hash;

/// Extension trait for HashMap with additional utility methods
pub trait HashMapExt<K, V> {
    /// Get a value or insert a default value if the key doesn't exist
    fn get_or_insert_default(&mut self, key: K, default: V) -> &mut V
    where
        K: Clone;

    /// Merge another HashMap into this one, using a function to resolve conflicts
    fn merge_with<F>(&mut self, other: HashMap<K, V>, f: F)
    where
        F: Fn(&V, V) -> V,
        K: Clone;
}

impl<K, V> HashMapExt<K, V> for HashMap<K, V>
where
    K: Eq + Hash + Clone,
{
    fn get_or_insert_default(&mut self, key: K, default: V) -> &mut V {
        self.entry(key).or_insert(default)
    }

    fn merge_with<F>(&mut self, other: HashMap<K, V>, f: F)
    where
        F: Fn(&V, V) -> V,
        K: Clone,
    {
        for (key, value) in other {
            match self.get(&key) {
                Some(existing) => {
                    let new_value = f(existing, value);
                    self.insert(key, new_value);
                }
                None => {
                    self.insert(key, value);
                }
            }
        }
    }
}

/// Utility functions for working with vectors
pub mod vec_utils {
    /// Partition a vector into chunks of approximately equal size
    pub fn partition_evenly<T>(mut vec: Vec<T>, num_partitions: usize) -> Vec<Vec<T>> {
        if num_partitions == 0 || vec.is_empty() {
            return vec![vec];
        }

        let chunk_size = (vec.len() + num_partitions - 1) / num_partitions;
        let mut result = Vec::with_capacity(num_partitions);

        while !vec.is_empty() {
            let take = std::cmp::min(chunk_size, vec.len());
            let chunk = vec.drain(..take).collect();
            result.push(chunk);
        }

        result
    }

    /// Flatten a vector of vectors into a single vector
    pub fn flatten<T>(nested: Vec<Vec<T>>) -> Vec<T> {
        nested.into_iter().flatten().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hashmap_get_or_insert_default() {
        let mut map = HashMap::new();
        map.insert("key1", 10);

        let value = map.get_or_insert_default("key1", 20);
        assert_eq!(*value, 10);

        let value = map.get_or_insert_default("key2", 30);
        assert_eq!(*value, 30);
    }

    #[test]
    fn test_hashmap_merge_with() {
        let mut map1 = HashMap::new();
        map1.insert("a", 1);
        map1.insert("b", 2);

        let mut map2 = HashMap::new();
        map2.insert("b", 3);
        map2.insert("c", 4);

        map1.merge_with(map2, |&existing, new| existing + new);

        assert_eq!(map1.get("a"), Some(&1));
        assert_eq!(map1.get("b"), Some(&5)); // 2 + 3
        assert_eq!(map1.get("c"), Some(&4));
    }

    #[test]
    fn test_partition_evenly() {
        let vec = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let partitions = vec_utils::partition_evenly(vec, 3);

        assert_eq!(partitions.len(), 3);
        assert_eq!(partitions[0], vec![1, 2, 3, 4]);
        assert_eq!(partitions[1], vec![5, 6, 7, 8]);
        assert_eq!(partitions[2], vec![9, 10]);
    }

    #[test]
    fn test_flatten() {
        let nested = vec![vec![1, 2], vec![3, 4, 5], vec![6]];
        let flattened = vec_utils::flatten(nested);
        assert_eq!(flattened, vec![1, 2, 3, 4, 5, 6]);
    }
}
