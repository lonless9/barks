//! Barks Kafka Connector module
//!
//! This module provides Kafka integration for Barks
//! including producers, consumers, and streaming sources.

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
