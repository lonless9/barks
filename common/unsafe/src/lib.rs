//! Unsafe operations module for Barks
//!
//! This module provides safe wrappers around unsafe operations
//! needed for performance-critical code paths.
//!
//! Note: The previous manual memory management has been replaced by `bumpalo`.

pub mod traits;

pub use traits::*;
