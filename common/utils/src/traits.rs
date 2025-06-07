//! Common traits for Barks
//!
//! This module provides common traits used across different Barks modules.

use std::fmt::Debug;
use std::hash::Hash;

/// Trait for types that can be cloned efficiently
pub trait EfficientClone {
    /// Clone the value efficiently, potentially using reference counting
    fn efficient_clone(&self) -> Self;
}

/// Implement EfficientClone for types that implement Clone
impl<T: Clone> EfficientClone for T {
    fn efficient_clone(&self) -> Self {
        self.clone()
    }
}

/// Trait for types that can be merged
pub trait Mergeable {
    /// Merge with another instance of the same type
    fn merge(self, other: Self) -> Self;
}

/// Implement Mergeable for Vec
impl<T> Mergeable for Vec<T> {
    fn merge(mut self, mut other: Self) -> Self {
        self.append(&mut other);
        self
    }
}

/// Trait for types that can be reduced
pub trait Reducible {
    type Item;

    /// Reduce the collection to a single value using the provided function
    fn reduce<F>(self, f: F) -> Option<Self::Item>
    where
        F: Fn(Self::Item, Self::Item) -> Self::Item;
}

/// Implement Reducible for Vec
impl<T> Reducible for Vec<T> {
    type Item = T;

    fn reduce<F>(mut self, f: F) -> Option<T>
    where
        F: Fn(T, T) -> T,
    {
        if self.is_empty() {
            return None;
        }

        let mut result = self.remove(0);
        for item in self {
            result = f(result, item);
        }
        Some(result)
    }
}

/// Trait for types that can be mapped over
pub trait Mappable {
    type Item;
    type Output<U>;

    /// Map a function over the collection
    fn map<U, F>(self, f: F) -> Self::Output<U>
    where
        F: Fn(Self::Item) -> U;
}

/// Implement Mappable for Vec
impl<T> Mappable for Vec<T> {
    type Item = T;
    type Output<U> = Vec<U>;

    fn map<U, F>(self, f: F) -> Vec<U>
    where
        F: Fn(T) -> U,
    {
        self.into_iter().map(f).collect()
    }
}

/// Trait for types that can be filtered
pub trait Filterable {
    type Item;

    /// Filter the collection using the provided predicate
    fn filter<F>(self, predicate: F) -> Self
    where
        F: Fn(&Self::Item) -> bool;
}

/// Implement Filterable for Vec
impl<T> Filterable for Vec<T> {
    type Item = T;

    fn filter<F>(self, predicate: F) -> Self
    where
        F: Fn(&T) -> bool,
    {
        self.into_iter().filter(|item| predicate(item)).collect()
    }
}

/// Trait for types that have a size
pub trait Sizeable {
    /// Get the size/length of the collection
    fn size(&self) -> usize;

    /// Check if the collection is empty
    fn is_empty(&self) -> bool {
        self.size() == 0
    }
}

/// Implement Sizeable for Vec
impl<T> Sizeable for Vec<T> {
    fn size(&self) -> usize {
        self.len()
    }
}

/// Trait for types that can be converted to and from bytes
pub trait ByteConvertible: Sized {
    type Error: Debug;

    /// Convert to bytes
    fn to_bytes(&self) -> Result<Vec<u8>, Self::Error>;

    /// Convert from bytes
    fn from_bytes(bytes: &[u8]) -> Result<Self, Self::Error>;
}

/// Trait for types that can be hashed consistently
pub trait ConsistentHash {
    /// Get a consistent hash value
    fn consistent_hash(&self) -> u64;
}

/// Implement ConsistentHash for types that implement Hash
impl<T: Hash> ConsistentHash for T {
    fn consistent_hash(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hasher;

        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mergeable() {
        let vec1 = vec![1, 2, 3];
        let vec2 = vec![4, 5, 6];
        let merged = vec1.merge(vec2);

        assert_eq!(merged, vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn test_reducible() {
        let vec = vec![1, 2, 3, 4, 5];
        let sum = vec.reduce(|a, b| a + b);

        assert_eq!(sum, Some(15));
    }

    #[test]
    fn test_mappable() {
        let vec = vec![1, 2, 3, 4, 5];
        let doubled = vec.map(|x| x * 2);

        assert_eq!(doubled, vec![2, 4, 6, 8, 10]);
    }

    #[test]
    fn test_filterable() {
        let vec = vec![1, 2, 3, 4, 5, 6];
        let evens = vec.filter(|&x| x % 2 == 0);

        assert_eq!(evens, vec![2, 4, 6]);
    }

    #[test]
    fn test_sizeable() {
        let vec = vec![1, 2, 3];
        assert_eq!(vec.size(), 3);
        assert!(!vec.is_empty());

        let empty_vec: Vec<i32> = vec![];
        assert_eq!(empty_vec.size(), 0);
        assert!(empty_vec.is_empty());
    }

    #[test]
    fn test_consistent_hash() {
        let value = "test_string";
        let hash1 = value.consistent_hash();
        let hash2 = value.consistent_hash();

        assert_eq!(hash1, hash2);
    }
}
