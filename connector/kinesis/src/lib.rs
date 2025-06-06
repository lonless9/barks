//! Barks Kinesis Connector module
//!
//! This module provides AWS Kinesis integration for Barks
//! including streaming sources and sinks.

pub mod producer;
pub mod consumer;
pub mod source;
pub mod sink;
pub mod traits;

pub use producer::*;
pub use consumer::*;
pub use source::*;
pub use sink::*;
pub use traits::*;
