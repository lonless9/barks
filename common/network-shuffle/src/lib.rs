//! Network Shuffle module for Barks
//!
//! This module provides shuffle data transfer capabilities
//! for distributed data processing.

pub mod optimizations;
pub mod shuffle;
pub mod traits;

pub use optimizations::*;
pub use shuffle::*;
pub use traits::*;
