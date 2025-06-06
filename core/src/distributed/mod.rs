//! Distributed computing module for Barks
//!
//! This module provides the distributed architecture implementation
//! with Driver-Executor model using gRPC for communication.

pub mod context;
pub mod driver;
pub mod executor;
pub mod task;
pub mod types;

pub use context::*;
pub use driver::*;
pub use executor::*;
pub use task::*;
pub use types::*;

// Re-export generated protobuf types
pub mod proto {
    pub mod driver {
        tonic::include_proto!("barks.driver");
    }

    pub mod executor {
        tonic::include_proto!("barks.executor");
    }
}
