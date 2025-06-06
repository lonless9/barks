//! Utilities module for Barks
//!
//! This module provides common utility functions and types
//! used across different Barks modules.

pub mod collections;
pub mod io;
pub mod serialization;
pub mod traits;

pub use collections::*;
pub use io::*;
pub use serialization::*;
pub use traits::*;

// Re-export commonly used functions for convenience
pub use collections::vec_utils::partition_evenly;
