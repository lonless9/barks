//! Barks Connect Common module
//!
//! This module provides common components for Barks Connect
//! client-server communication.

pub mod protocol;
pub mod messages;
pub mod traits;

pub use protocol::*;
pub use messages::*;
pub use traits::*;
