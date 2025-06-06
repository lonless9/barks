//! Barks Protobuf Connector module
//!
//! This module provides Protocol Buffers format support for Barks
//! including serialization and deserialization.

pub mod reader;
pub mod writer;
pub mod schema;
pub mod traits;

pub use reader::*;
pub use writer::*;
pub use schema::*;
pub use traits::*;
