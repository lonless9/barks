//! Unsafe operations traits

use anyhow::Result;
use std::ptr::NonNull;

/// Trait for unsafe memory operations
pub trait UnsafeMemory: Send + Sync {
    /// Allocate raw memory
    ///
    /// # Safety
    ///
    /// The caller must ensure that:
    /// - `size` is non-zero
    /// - `align` is a power of two and non-zero
    /// - The returned pointer, if successful, must be properly deallocated using `deallocate`
    unsafe fn allocate(&self, size: usize, align: usize) -> Result<NonNull<u8>>;

    /// Deallocate raw memory
    ///
    /// # Safety
    ///
    /// The caller must ensure that:
    /// - `ptr` was previously allocated by this allocator with the same `size` and `align`
    /// - `ptr` has not been deallocated before
    /// - `size` and `align` match the original allocation parameters
    unsafe fn deallocate(&self, ptr: NonNull<u8>, size: usize, align: usize) -> Result<()>;

    /// Reallocate memory
    ///
    /// # Safety
    ///
    /// The caller must ensure that:
    /// - `ptr` was previously allocated by this allocator with `old_size` and `align`
    /// - `ptr` has not been deallocated before
    /// - `old_size` and `align` match the original allocation parameters
    /// - `new_size` is non-zero
    /// - The returned pointer, if successful, must be properly deallocated using `deallocate`
    unsafe fn reallocate(
        &self,
        ptr: NonNull<u8>,
        old_size: usize,
        new_size: usize,
        align: usize,
    ) -> Result<NonNull<u8>>;

    /// Copy memory
    ///
    /// # Safety
    ///
    /// The caller must ensure that:
    /// - `src` and `dst` are valid pointers for reads and writes respectively
    /// - `src` and `dst` point to allocated memory of at least `size` bytes
    /// - The memory regions do not overlap (use memmove semantics if overlap is possible)
    unsafe fn copy(&self, src: NonNull<u8>, dst: NonNull<u8>, size: usize) -> Result<()>;

    /// Set memory
    ///
    /// # Safety
    ///
    /// The caller must ensure that:
    /// - `ptr` is a valid pointer for writes
    /// - `ptr` points to allocated memory of at least `size` bytes
    unsafe fn set(&self, ptr: NonNull<u8>, value: u8, size: usize) -> Result<()>;
}

/// Trait for unsafe buffer operations
pub trait UnsafeBuffer {
    /// Get buffer pointer
    fn as_ptr(&self) -> *const u8;

    /// Get mutable buffer pointer
    fn as_mut_ptr(&mut self) -> *mut u8;

    /// Get buffer size
    fn size(&self) -> usize;

    /// Get buffer capacity
    fn capacity(&self) -> usize;

    /// Resize buffer (unsafe)
    ///
    /// # Safety
    ///
    /// The caller must ensure that:
    /// - The buffer has sufficient capacity or can be reallocated
    /// - Any existing data beyond `new_size` will be considered invalid
    /// - The buffer remains in a valid state after resizing
    unsafe fn resize(&mut self, new_size: usize) -> Result<()>;

    /// Reserve capacity (unsafe)
    ///
    /// # Safety
    ///
    /// The caller must ensure that:
    /// - The buffer can be reallocated to accommodate the additional capacity
    /// - Existing data remains valid after the operation
    /// - The buffer remains in a valid state after reserving
    unsafe fn reserve(&mut self, additional: usize) -> Result<()>;
}

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
