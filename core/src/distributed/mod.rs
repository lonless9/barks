//! Distributed computing module for Barks
//!
//! This module provides the distributed architecture implementation
//! with Driver-Executor model using gRPC for communication.

pub mod driver;
pub mod executor;
pub mod stage;
pub mod task;
pub mod types;

#[cfg(test)]
mod end_to_end_shuffle_test;

pub use driver::*;
pub use executor::*;
pub use stage::*;
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
