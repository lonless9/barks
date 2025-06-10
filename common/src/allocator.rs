//! Memory allocator abstraction using trait-based design.
//!
//! This module provides a generic memory allocator interface that abstracts over
//! the underlying allocation implementation (bumpalo).

use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::error::Result;

/// Generic memory allocator for bump allocation.
///
/// Note: This is designed for single-threaded use within each allocator instance.
/// For multi-threaded scenarios, create separate allocator instances per thread.
pub trait Allocator: Debug {
    /// Allocate memory for bytes and return a slice reference.
    fn alloc_bytes(&mut self, size: usize) -> &mut [u8];

    /// Allocate memory for a string and return a string slice reference.
    fn alloc_str(&mut self, s: &str) -> &str;

    /// Reset the allocator, freeing all allocated memory.
    fn reset(&mut self);

    /// Get allocation statistics if available.
    fn stats(&self) -> AllocatorStats;

    /// Get the total allocated bytes.
    fn allocated_bytes(&self) -> usize;

    /// Get the remaining capacity in bytes.
    fn remaining_capacity(&self) -> usize;
}

/// Thread-safe wrapper around an allocator.
///
/// This provides a Send + Sync interface by using a mutex to protect
/// the underlying allocator.
#[derive(Debug)]
pub struct ThreadSafeAllocator {
    inner: Arc<Mutex<Box<dyn Allocator + Send>>>,
}

impl ThreadSafeAllocator {
    /// Create a new thread-safe allocator wrapper.
    pub fn new(allocator: Box<dyn Allocator + Send>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(allocator)),
        }
    }

    /// Allocate memory for bytes and return a vector (since we can't return borrowed data across mutex).
    pub fn alloc_bytes(&self, size: usize) -> Vec<u8> {
        let mut guard = self.inner.lock().unwrap();
        let slice = guard.alloc_bytes(size);
        slice.to_vec()
    }

    /// Allocate memory for a string and return an owned string.
    pub fn alloc_str(&self, s: &str) -> String {
        let mut guard = self.inner.lock().unwrap();
        let str_ref = guard.alloc_str(s);
        str_ref.to_string()
    }

    /// Reset the allocator, freeing all allocated memory.
    pub fn reset(&self) {
        let mut guard = self.inner.lock().unwrap();
        guard.reset();
    }

    /// Get allocation statistics if available.
    pub fn stats(&self) -> AllocatorStats {
        let guard = self.inner.lock().unwrap();
        guard.stats()
    }

    /// Get the total allocated bytes.
    pub fn allocated_bytes(&self) -> usize {
        let guard = self.inner.lock().unwrap();
        guard.allocated_bytes()
    }

    /// Get the remaining capacity in bytes.
    pub fn remaining_capacity(&self) -> usize {
        let guard = self.inner.lock().unwrap();
        guard.remaining_capacity()
    }
}

impl Clone for ThreadSafeAllocator {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

/// Allocator statistics information.
#[derive(Debug, Clone, Default)]
pub struct AllocatorStats {
    pub total_allocations: u64,
    pub total_allocated_bytes: u64,
    pub current_allocated_bytes: u64,
    pub reset_count: u64,
}

/// Configuration for allocator creation.
#[derive(Debug, Clone)]
pub struct AllocatorConfig {
    /// Initial capacity of the allocator in bytes.
    pub initial_capacity: Option<usize>,
    /// Whether to track allocation statistics.
    pub track_stats: bool,
}

impl Default for AllocatorConfig {
    fn default() -> Self {
        Self {
            initial_capacity: Some(1024 * 1024), // 1MB default
            track_stats: true,
        }
    }
}

/// Builder for creating allocator instances.
pub struct AllocatorBuilder {
    config: AllocatorConfig,
}

impl AllocatorBuilder {
    /// Create a new allocator builder with default configuration.
    pub fn new() -> Self {
        Self {
            config: AllocatorConfig::default(),
        }
    }

    /// Set the initial capacity of the allocator.
    pub fn initial_capacity(mut self, capacity: usize) -> Self {
        self.config.initial_capacity = Some(capacity);
        self
    }

    /// Enable or disable statistics tracking.
    pub fn track_stats(mut self, track: bool) -> Self {
        self.config.track_stats = track;
        self
    }

    /// Build a single-threaded allocator instance.
    pub fn build(self) -> Result<Box<dyn Allocator + Send>> {
        let allocator = BumpaloAllocatorImpl::new(self.config)?;
        Ok(Box::new(allocator))
    }

    /// Build a thread-safe allocator instance.
    pub fn build_thread_safe(self) -> Result<ThreadSafeAllocator> {
        let allocator = self.build()?;
        Ok(ThreadSafeAllocator::new(allocator))
    }
}

impl Default for AllocatorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Internal statistics tracker for allocation operations.
#[derive(Debug, Default)]
struct InternalAllocatorStats {
    allocations: AtomicU64,
    allocated_bytes: AtomicU64,
    resets: AtomicU64,
}

impl InternalAllocatorStats {
    fn record_allocation(&self, size: usize) {
        self.allocations.fetch_add(1, Ordering::AcqRel);
        self.allocated_bytes
            .fetch_add(size as u64, Ordering::AcqRel);
    }

    fn record_reset(&self) {
        self.resets.fetch_add(1, Ordering::AcqRel);
        // Note: We don't reset allocated_bytes counter to maintain total allocation history
    }

    fn get_stats(&self, current_allocated: u64) -> AllocatorStats {
        AllocatorStats {
            total_allocations: self.allocations.load(Ordering::Acquire),
            total_allocated_bytes: self.allocated_bytes.load(Ordering::Acquire),
            current_allocated_bytes: current_allocated,
            reset_count: self.resets.load(Ordering::Acquire),
        }
    }
}

/// Bumpalo-based allocator implementation.
#[derive(Debug)]
struct BumpaloAllocatorImpl {
    inner: bumpalo::Bump,
    stats: Option<Arc<InternalAllocatorStats>>,
}

impl BumpaloAllocatorImpl {
    fn new(config: AllocatorConfig) -> Result<Self> {
        let mut bump = bumpalo::Bump::new();

        // Set initial capacity if specified
        if let Some(capacity) = config.initial_capacity {
            // Pre-allocate by allocating and immediately resetting
            let _temp: &[u8] = bump.alloc_slice_fill_default(capacity);
            bump.reset();
        }

        let stats = if config.track_stats {
            Some(Arc::new(InternalAllocatorStats::default()))
        } else {
            None
        };

        Ok(Self { inner: bump, stats })
    }
}

impl Allocator for BumpaloAllocatorImpl {
    fn alloc_bytes(&mut self, size: usize) -> &mut [u8] {
        if let Some(ref stats) = self.stats {
            stats.record_allocation(size);
        }
        self.inner.alloc_slice_fill_default(size)
    }

    fn alloc_str(&mut self, s: &str) -> &str {
        let size = s.len();
        if let Some(ref stats) = self.stats {
            stats.record_allocation(size);
        }
        self.inner.alloc_str(s)
    }

    fn reset(&mut self) {
        if let Some(ref stats) = self.stats {
            stats.record_reset();
        }
        self.inner.reset();
    }

    fn stats(&self) -> AllocatorStats {
        if let Some(ref stats) = self.stats {
            stats.get_stats(self.allocated_bytes() as u64)
        } else {
            AllocatorStats::default()
        }
    }

    fn allocated_bytes(&self) -> usize {
        self.inner.allocated_bytes()
    }

    fn remaining_capacity(&self) -> usize {
        // Bumpalo doesn't directly expose remaining capacity,
        // so we estimate based on chunk size
        let chunk_size = self.inner.chunk_capacity();
        let allocated = self.inner.allocated_bytes();
        chunk_size.saturating_sub(allocated)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_allocator_basic_operations() {
        let mut allocator = AllocatorBuilder::new()
            .initial_capacity(1024)
            .build()
            .expect("Failed to create allocator");

        // Test byte allocation
        let bytes = allocator.alloc_bytes(10);
        assert_eq!(bytes.len(), 10);
        bytes[0] = 42;
        assert_eq!(bytes[0], 42);

        // Test string allocation
        let s = allocator.alloc_str("hello world");
        assert_eq!(s, "hello world");

        // Test statistics
        let stats = allocator.stats();
        assert!(stats.total_allocations > 0);
        assert!(stats.total_allocated_bytes > 0);
    }

    #[test]
    fn test_allocator_reset() {
        let mut allocator = AllocatorBuilder::new()
            .initial_capacity(1024)
            .build()
            .expect("Failed to create allocator");

        // Allocate some data
        let _bytes1 = allocator.alloc_bytes(100);
        let _bytes2 = allocator.alloc_bytes(200);

        let allocated_before = allocator.allocated_bytes();
        assert!(allocated_before > 0);

        // Reset the allocator
        allocator.reset();

        let _allocated_after = allocator.allocated_bytes();

        let bytes3 = allocator.alloc_bytes(300);
        assert_eq!(bytes3.len(), 300);
    }

    #[test]
    fn test_allocator_without_stats() {
        let mut allocator = AllocatorBuilder::new()
            .track_stats(false)
            .build()
            .expect("Failed to create allocator");

        let _bytes = allocator.alloc_bytes(42);

        // Stats should be default (zeros) when tracking is disabled
        let stats = allocator.stats();
        assert_eq!(stats.total_allocations, 0);
        assert_eq!(stats.total_allocated_bytes, 0);
    }

    #[test]
    fn test_allocator_configuration() {
        let mut allocator = AllocatorBuilder::new()
            .initial_capacity(2048)
            .track_stats(true)
            .build()
            .expect("Failed to create configured allocator");

        // Test that the allocator works with custom configuration
        let bytes = allocator.alloc_bytes(42);
        assert_eq!(bytes.len(), 42);

        let stats = allocator.stats();
        assert!(stats.total_allocations > 0);
    }

    #[test]
    fn test_allocator_statistics_comprehensive() {
        let mut allocator = AllocatorBuilder::new()
            .initial_capacity(1024)
            .track_stats(true)
            .build()
            .expect("Failed to create allocator");

        // Perform multiple allocations
        let _bytes1 = allocator.alloc_bytes(10);
        let _bytes2 = allocator.alloc_bytes(20);
        let _bytes3 = allocator.alloc_bytes(4);
        let _str = allocator.alloc_str("test");

        let stats_before_reset = allocator.stats();
        assert_eq!(stats_before_reset.total_allocations, 4);
        assert!(stats_before_reset.total_allocated_bytes > 0);
        assert_eq!(stats_before_reset.reset_count, 0);

        // Reset and check statistics
        allocator.reset();

        let stats_after_reset = allocator.stats();
        assert_eq!(stats_after_reset.total_allocations, 4); // Total count preserved
        assert!(stats_after_reset.total_allocated_bytes > 0); // Total bytes preserved

        assert_eq!(stats_after_reset.reset_count, 1);

        let _bytes4 = allocator.alloc_bytes(30);

        let stats_final = allocator.stats();
        assert_eq!(stats_final.total_allocations, 5);
        assert_eq!(stats_final.reset_count, 1);
        assert!(stats_final.current_allocated_bytes > 0);
    }

    #[test]
    fn test_allocator_capacity() {
        let mut allocator = AllocatorBuilder::new()
            .initial_capacity(1024)
            .build()
            .expect("Failed to create allocator");

        let initial_remaining = allocator.remaining_capacity();

        let _bytes = allocator.alloc_bytes(100);

        let remaining_after = allocator.remaining_capacity();

        assert!(remaining_after <= initial_remaining);

        let allocated = allocator.allocated_bytes();
        assert!(allocated >= 100);
    }

    #[test]
    fn test_thread_safe_allocator() {
        let allocator = AllocatorBuilder::new()
            .initial_capacity(1024)
            .build_thread_safe()
            .expect("Failed to create thread-safe allocator");

        // Test basic operations through thread-safe wrapper
        let bytes = allocator.alloc_bytes(10);
        assert_eq!(bytes.len(), 10);

        let s = allocator.alloc_str("hello");
        assert_eq!(s, "hello");

        // Test statistics
        let stats = allocator.stats();
        assert!(stats.total_allocations > 0);

        // Test reset
        let _allocated_before_reset = allocator.allocated_bytes();
        allocator.reset();
        let _allocated_after = allocator.allocated_bytes();
    }

    #[test]
    fn test_allocator_builder_default() {
        // Test Default implementation for AllocatorBuilder
        let builder1 = AllocatorBuilder::default();
        let builder2 = AllocatorBuilder::new();

        // Both should create equivalent builders
        let mut allocator1 = builder1
            .build()
            .expect("Failed to create allocator from default");
        let mut allocator2 = builder2
            .build()
            .expect("Failed to create allocator from new");

        // Test that both allocators work the same way
        let bytes1 = allocator1.alloc_bytes(42);
        let bytes2 = allocator2.alloc_bytes(42);

        assert_eq!(bytes1.len(), bytes2.len());
        assert_eq!(bytes1.len(), 42);
    }
}
