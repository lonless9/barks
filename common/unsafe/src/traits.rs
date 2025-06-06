//! Unsafe operations traits

use std::ptr::NonNull;
use anyhow::Result;

/// Trait for unsafe memory operations
pub trait UnsafeMemory: Send + Sync {
    /// Allocate raw memory
    unsafe fn allocate(&self, size: usize, align: usize) -> Result<NonNull<u8>>;
    
    /// Deallocate raw memory
    unsafe fn deallocate(&self, ptr: NonNull<u8>, size: usize, align: usize) -> Result<()>;
    
    /// Reallocate memory
    unsafe fn reallocate(&self, ptr: NonNull<u8>, old_size: usize, new_size: usize, align: usize) -> Result<NonNull<u8>>;
    
    /// Copy memory
    unsafe fn copy(&self, src: NonNull<u8>, dst: NonNull<u8>, size: usize) -> Result<()>;
    
    /// Set memory
    unsafe fn set(&self, ptr: NonNull<u8>, value: u8, size: usize) -> Result<()>;
}

/// Trait for unsafe buffer operations
pub trait UnsafeBuffer: Send + Sync {
    /// Get buffer pointer
    fn as_ptr(&self) -> *const u8;
    
    /// Get mutable buffer pointer
    fn as_mut_ptr(&mut self) -> *mut u8;
    
    /// Get buffer size
    fn size(&self) -> usize;
    
    /// Get buffer capacity
    fn capacity(&self) -> usize;
    
    /// Resize buffer (unsafe)
    unsafe fn resize(&mut self, new_size: usize) -> Result<()>;
    
    /// Reserve capacity (unsafe)
    unsafe fn reserve(&mut self, additional: usize) -> Result<()>;
}

/// Trait for unsafe pointer operations
pub trait UnsafePointer<T>: Send + Sync {
    /// Create from raw pointer
    unsafe fn from_raw(ptr: *mut T) -> Self;
    
    /// Convert to raw pointer
    fn into_raw(self) -> *mut T;
    
    /// Get reference (unsafe)
    unsafe fn as_ref(&self) -> Option<&T>;
    
    /// Get mutable reference (unsafe)
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
    unsafe fn serialize_raw<T>(&self, value: &T) -> Result<Vec<u8>>;
    
    /// Deserialize from raw bytes (unsafe)
    unsafe fn deserialize_raw<T>(&self, data: &[u8]) -> Result<T>;
    
    /// Get serialized size
    fn serialized_size<T>(&self, value: &T) -> usize;
}
