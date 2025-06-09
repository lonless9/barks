//! Barks SQL API module
//!
//! This module provides the public API for Barks SQL functionality
//! including DataFrames, Datasets, and SQL operations powered by Apache DataFusion.

pub mod dataframe;
pub mod dataset;
pub mod functions;
pub mod traits;
pub mod types;

pub use dataframe::*;
pub use dataset::*;
pub use functions::*;
pub use traits::*;
pub use types::*;

// Re-export core SQL functionality
pub use barks_sql_core::{
    DataFusionQueryEngine, DataFusionSession, RecordBatch, SessionContext, SqlConfig, SqlError,
    SqlResult,
};
