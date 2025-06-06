//! Barks MLlib Local module
//!
//! This module provides local machine learning algorithms
//! and utilities for single-machine computations.

pub mod algorithms;
pub mod linalg;
pub mod stats;
pub mod traits;

pub use algorithms::*;
pub use linalg::*;
pub use stats::*;
pub use traits::*;
