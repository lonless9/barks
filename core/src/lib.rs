//! Barks Core module
//!
//! This module provides the core functionality of Barks including
//! RDDs and context management for Phase 0 implementation.

pub mod rdd;
pub mod context;
pub mod traits;

pub use rdd::*;
pub use context::*;
pub use traits::*;
