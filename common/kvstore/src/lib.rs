//! Key-Value Store module for Barks
//!
//! This module provides key-value storage abstractions and implementations
//! for Barks's internal data management needs.

pub mod store;
pub mod traits;

pub use store::*;
pub use traits::*;
