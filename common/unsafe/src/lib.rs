//! Unsafe operations module for Barks
//!
//! This module provides safe wrappers around unsafe operations
//! needed for performance-critical code paths.

pub mod memory;
pub mod traits;

pub use memory::*;
pub use traits::*;
