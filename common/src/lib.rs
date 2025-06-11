//! Common utilities and abstractions for the Barks project.
//!
//! This module provides trait-based abstractions for caching and storage.

pub mod allocator;
pub mod cache;
pub mod compression;
pub mod error;
pub mod storage;

pub use allocator::{Allocator, AllocatorBuilder, AllocatorStats, ThreadSafeAllocator};
pub use cache::{Cache, CacheBuilder};
pub use compression::{CompressionAlgorithm, CompressionBuilder, CompressionStats, Compressor};
pub use error::{BarksError, BarksResult, CommonError, Result};
pub use storage::{BatchOperation, Storage, StorageBuilder, StorageStats};
