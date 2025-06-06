//! Barks Assembly module
//!
//! This module provides packaging and assembly capabilities
//! for creating distributable Barks applications.

pub mod assembly;
pub mod packaging;
pub mod traits;

pub use assembly::*;
pub use packaging::*;
pub use traits::*;
