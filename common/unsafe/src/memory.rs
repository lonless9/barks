//! Unsafe memory implementations

use crate::traits::*;
use anyhow::Result;
use std::alloc::{Layout, alloc, dealloc, realloc};
use std::ptr::{NonNull, copy_nonoverlapping, write_bytes};

/// Standard unsafe memory allocator
pub struct StandardUnsafeMemory;

impl UnsafeMemory for StandardUnsafeMemory {
    unsafe fn allocate(&self, size: usize, align: usize) -> Result<NonNull<u8>> {
        unsafe {
            let layout = Layout::from_size_align(size, align)
                .map_err(|e| anyhow::anyhow!("Invalid layout: {}", e))?;

            let ptr = alloc(layout);
            NonNull::new(ptr).ok_or_else(|| anyhow::anyhow!("Allocation failed"))
        }
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, size: usize, align: usize) -> Result<()> {
        unsafe {
            let layout = Layout::from_size_align(size, align)
                .map_err(|e| anyhow::anyhow!("Invalid layout: {}", e))?;

            dealloc(ptr.as_ptr(), layout);
            Ok(())
        }
    }

    unsafe fn reallocate(
        &self,
        ptr: NonNull<u8>,
        old_size: usize,
        new_size: usize,
        align: usize,
    ) -> Result<NonNull<u8>> {
        unsafe {
            let old_layout = Layout::from_size_align(old_size, align)
                .map_err(|e| anyhow::anyhow!("Invalid old layout: {}", e))?;

            let new_ptr = realloc(ptr.as_ptr(), old_layout, new_size);
            NonNull::new(new_ptr).ok_or_else(|| anyhow::anyhow!("Reallocation failed"))
        }
    }

    unsafe fn copy(&self, src: NonNull<u8>, dst: NonNull<u8>, size: usize) -> Result<()> {
        unsafe {
            copy_nonoverlapping(src.as_ptr(), dst.as_ptr(), size);
            Ok(())
        }
    }

    unsafe fn set(&self, ptr: NonNull<u8>, value: u8, size: usize) -> Result<()> {
        unsafe {
            write_bytes(ptr.as_ptr(), value, size);
            Ok(())
        }
    }
}

/// Unsafe buffer implementation
pub struct UnsafeVecBuffer {
    ptr: NonNull<u8>,
    size: usize,
    capacity: usize,
}

impl UnsafeVecBuffer {
    pub fn new(capacity: usize) -> Result<Self> {
        let allocator = StandardUnsafeMemory;
        unsafe {
            let ptr = allocator.allocate(capacity, 1)?;
            Ok(Self {
                ptr,
                size: 0,
                capacity,
            })
        }
    }
}

impl Drop for UnsafeVecBuffer {
    fn drop(&mut self) {
        let allocator = StandardUnsafeMemory;
        unsafe {
            let _ = allocator.deallocate(self.ptr, self.capacity, 1);
        }
    }
}

// SAFETY: UnsafeVecBuffer is designed to be used in unsafe contexts
// where the caller ensures thread safety
unsafe impl Send for UnsafeVecBuffer {}
unsafe impl Sync for UnsafeVecBuffer {}

impl UnsafeBuffer for UnsafeVecBuffer {
    fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    fn size(&self) -> usize {
        self.size
    }

    fn capacity(&self) -> usize {
        self.capacity
    }

    unsafe fn resize(&mut self, new_size: usize) -> Result<()> {
        if new_size > self.capacity {
            anyhow::bail!("New size exceeds capacity");
        }
        self.size = new_size;
        Ok(())
    }

    unsafe fn reserve(&mut self, additional: usize) -> Result<()> {
        unsafe {
            let new_capacity = self.capacity + additional;
            let allocator = StandardUnsafeMemory;

            let new_ptr = allocator.reallocate(self.ptr, self.capacity, new_capacity, 1)?;
            self.ptr = new_ptr;
            self.capacity = new_capacity;
            Ok(())
        }
    }
}
