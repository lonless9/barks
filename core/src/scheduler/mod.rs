//! Task Scheduler module
//!
//! This module provides task scheduling functionality for parallel execution
//! of RDD operations using Rayon's thread pool.

pub mod local_scheduler;

pub use local_scheduler::*;
