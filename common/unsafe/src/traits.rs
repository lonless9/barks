//! Unsafe operations traits
//!
//! Note: The previous manual memory management has been replaced by `bumpalo`.
//! This module now contains only the remaining unsafe traits that are still needed.

use anyhow::Result;

/// Trait for unsafe pointer operations
pub trait UnsafePointer<T>: Send + Sync {
    /// Create from raw pointer
    ///
    /// # Safety
    ///
    /// The caller must ensure that:
    /// - `ptr` is either null or points to a valid instance of `T`
    /// - If `ptr` is non-null, the caller has ownership of the pointed-to value
    /// - The lifetime of the pointed-to value extends beyond the lifetime of this pointer wrapper
    unsafe fn from_raw(ptr: *mut T) -> Self;

    /// Convert to raw pointer
    fn into_raw(self) -> *mut T;

    /// Get reference (unsafe)
    ///
    /// # Safety
    ///
    /// The caller must ensure that:
    /// - The pointer is non-null and points to a valid instance of `T`
    /// - The returned reference does not outlive the pointed-to value
    /// - No mutable references to the same data exist during the lifetime of the returned reference
    unsafe fn as_ref(&self) -> Option<&T>;

    /// Get mutable reference (unsafe)
    ///
    /// # Safety
    ///
    /// The caller must ensure that:
    /// - The pointer is non-null and points to a valid instance of `T`
    /// - The returned reference does not outlive the pointed-to value
    /// - No other references (mutable or immutable) to the same data exist during the lifetime of the returned reference
    unsafe fn as_mut(&mut self) -> Option<&mut T>;

    /// Check if pointer is null
    fn is_null(&self) -> bool;
}

/// Trait for unsafe atomic operations
pub trait UnsafeAtomic<T>: Send + Sync {
    /// Load value atomically
    fn load(&self, ordering: std::sync::atomic::Ordering) -> T;

    /// Store value atomically
    fn store(&self, value: T, ordering: std::sync::atomic::Ordering);

    /// Compare and swap
    fn compare_and_swap(&self, current: T, new: T, ordering: std::sync::atomic::Ordering) -> T;

    /// Fetch and add
    fn fetch_add(&self, value: T, ordering: std::sync::atomic::Ordering) -> T
    where
        T: std::ops::Add<Output = T> + Copy;

    /// Fetch and sub
    fn fetch_sub(&self, value: T, ordering: std::sync::atomic::Ordering) -> T
    where
        T: std::ops::Sub<Output = T> + Copy;
}

/// Trait for unsafe serialization
pub trait UnsafeSerialization: Send + Sync {
    /// Serialize to raw bytes (unsafe)
    ///
    /// # Safety
    ///
    /// The caller must ensure that:
    /// - `value` points to a valid instance of `T`
    /// - `T` has a stable memory layout suitable for raw serialization
    /// - The serialized data can be safely deserialized back to `T`
    unsafe fn serialize_raw<T>(&self, value: &T) -> Result<Vec<u8>>;

    /// Deserialize from raw bytes (unsafe)
    ///
    /// # Safety
    ///
    /// The caller must ensure that:
    /// - `data` contains valid serialized representation of `T`
    /// - `data` was created by a compatible serialization method
    /// - `T` has a stable memory layout suitable for raw deserialization
    /// - The deserialized value will be properly initialized
    unsafe fn deserialize_raw<T>(&self, data: &[u8]) -> Result<T>;

    /// Get serialized size
    fn serialized_size<T>(&self, value: &T) -> usize;
}
