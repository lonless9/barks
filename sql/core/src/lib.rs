//! Barks SQL Core module
//!
//! This module provides the core SQL execution engine and runtime components,
//! integrating Apache DataFusion with Barks' distributed execution model.

pub mod columnar;
pub mod datasources;
pub mod execution;
pub mod rdd_exec;
pub mod traits;

#[cfg(test)]
mod tests;

pub use columnar::*;
pub use datasources::*;
pub use execution::*;
pub use rdd_exec::*;
pub use traits::*;

// Re-export commonly used DataFusion types for convenience
pub use datafusion::arrow::record_batch::RecordBatch;
pub use datafusion::execution::context::SessionContext;
pub use datafusion::prelude::*;
