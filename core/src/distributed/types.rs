//! Distributed computing types and utilities
//!
//! This module defines the core types used in distributed execution.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::time::{SystemTime, UNIX_EPOCH};

/// Unique identifier for executors
pub type ExecutorId = String;

/// Unique identifier for tasks
pub type TaskId = String;

/// Unique identifier for stages
pub type StageId = String;

/// Executor information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutorInfo {
    pub executor_id: ExecutorId,
    pub host: String,
    pub port: u16,
    pub cores: u32,
    pub memory_mb: u64,
    pub max_concurrent_tasks: u32,
    pub attributes: HashMap<String, String>,
    pub start_time: u64,
}

impl ExecutorInfo {
    pub fn new(
        executor_id: ExecutorId,
        host: String,
        port: u16,
        cores: u32,
        memory_mb: u64,
    ) -> Self {
        Self {
            executor_id,
            host,
            port,
            cores,
            memory_mb,
            max_concurrent_tasks: cores, // Default to number of cores
            attributes: HashMap::new(),
            start_time: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }

    pub fn with_attributes(mut self, attributes: HashMap<String, String>) -> Self {
        self.attributes = attributes;
        self
    }
}

/// Task execution metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskMetrics {
    pub executor_deserialize_time_ms: u64,
    pub executor_run_time_ms: u64,
    pub result_size_bytes: u64,
    pub jvm_gc_time_ms: u64,
    pub result_serialization_time_ms: u64,
    pub memory_bytes_spilled: u64,
    pub disk_bytes_spilled: u64,
    pub peak_execution_memory_bytes: u64,
}

impl Default for TaskMetrics {
    fn default() -> Self {
        Self {
            executor_deserialize_time_ms: 0,
            executor_run_time_ms: 0,
            result_size_bytes: 0,
            jvm_gc_time_ms: 0,
            result_serialization_time_ms: 0,
            memory_bytes_spilled: 0,
            disk_bytes_spilled: 0,
            peak_execution_memory_bytes: 0,
        }
    }
}

/// Executor metrics for monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutorMetrics {
    pub total_tasks: u64,
    pub failed_tasks: u64,
    pub succeeded_tasks: u64,
    pub total_duration_ms: u64,
    pub total_gc_time_ms: u64,
    pub max_memory_bytes: u64,
    pub memory_used_bytes: u64,
    pub active_tasks: u32,
}

impl Default for ExecutorMetrics {
    fn default() -> Self {
        Self {
            total_tasks: 0,
            failed_tasks: 0,
            succeeded_tasks: 0,
            total_duration_ms: 0,
            total_gc_time_ms: 0,
            max_memory_bytes: 0,
            memory_used_bytes: 0,
            active_tasks: 0,
        }
    }
}

/// Executor status enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(i32)]
pub enum ExecutorStatus {
    Starting = 0,
    Running = 1, // Service is up
    Idle = 2,    // Service is up and waiting for tasks
    Busy = 3,    // Service is up and executing tasks
    Stopping = 4,
    Failed = 5,
}

impl Default for ExecutorStatus {
    fn default() -> Self {
        Self::Starting
    }
}

/// Task state enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(i32)]
pub enum TaskState {
    Pending = 0,
    Running = 1,
    Finished = 2,
    Failed = 3,
    Killed = 4,
}

impl Default for TaskState {
    fn default() -> Self {
        Self::Pending
    }
}

/// Serialization utilities for distributed tasks
pub mod serialization {
    use super::*;
    use anyhow::Result;

    /// Serialize task metadata for network transmission
    /// Note: For now, we'll serialize just the metadata, not the actual data
    pub fn serialize_task_metadata(
        task_id: &TaskId,
        stage_id: &StageId,
        partition_index: usize,
        properties: &HashMap<String, String>,
    ) -> Result<Vec<u8>> {
        let metadata = serde_json::json!({
            "task_id": task_id,
            "stage_id": stage_id,
            "partition_index": partition_index,
            "properties": properties
        });
        serde_json::to_vec(&metadata)
            .map_err(|e| anyhow::anyhow!("Task metadata serialization failed: {}", e))
    }

    /// Serialize task result metadata for network transmission
    pub fn serialize_result_metadata(
        task_id: &TaskId,
        stage_id: &StageId,
        partition_index: usize,
        success: bool,
        error_message: Option<String>,
        metrics: &TaskMetrics,
    ) -> Result<Vec<u8>> {
        let metadata = serde_json::json!({
            "task_id": task_id,
            "stage_id": stage_id,
            "partition_index": partition_index,
            "success": success,
            "error_message": error_message,
            "metrics": metrics
        });
        serde_json::to_vec(&metadata)
            .map_err(|e| anyhow::anyhow!("Result metadata serialization failed: {}", e))
    }
}
