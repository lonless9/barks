//! Distributed computing types and utilities
//!
//! This module defines the core types used in distributed execution.

use crate::traits::RddResult;
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

/// Task definition for distributed execution
#[derive(Debug, Clone)]
pub struct DistributedTask<T>
where
    T: Send + Sync + Clone + Debug,
{
    pub task_id: TaskId,
    pub stage_id: StageId,
    pub partition_index: usize,
    pub partition_data: Vec<T>, // Serialized partition data instead of trait object
    pub task_type: TaskType,
    pub properties: HashMap<String, String>,
}

/// Types of tasks that can be executed
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskType {
    /// Map operation with serialized function representation
    Map { operation_id: String },
    /// Filter operation with serialized predicate representation
    Filter { operation_id: String },
    /// Collect operation
    Collect,
    /// Custom operation with operation identifier
    Custom { operation_id: String },
}

impl<T> DistributedTask<T>
where
    T: Send + Sync + Clone + Debug,
{
    pub fn new(
        task_id: TaskId,
        stage_id: StageId,
        partition_index: usize,
        partition_data: Vec<T>,
        task_type: TaskType,
    ) -> Self {
        Self {
            task_id,
            stage_id,
            partition_index,
            partition_data,
            task_type,
            properties: HashMap::new(),
        }
    }

    pub fn with_properties(mut self, properties: HashMap<String, String>) -> Self {
        self.properties = properties;
        self
    }

    /// Execute the task and return the result
    /// This is a simplified version - in a real implementation,
    /// the task_type would determine the actual computation
    pub fn execute(&self) -> RddResult<Vec<T>> {
        match &self.task_type {
            TaskType::Collect => Ok(self.partition_data.clone()),
            TaskType::Map { operation_id: _ } => {
                // In a real implementation, we would look up the operation
                // and apply it to the partition data
                Ok(self.partition_data.clone())
            }
            TaskType::Filter { operation_id: _ } => {
                // In a real implementation, we would look up the predicate
                // and filter the partition data
                Ok(self.partition_data.clone())
            }
            TaskType::Custom { operation_id: _ } => {
                // In a real implementation, we would look up the custom operation
                Ok(self.partition_data.clone())
            }
        }
    }
}

/// Task result containing the computed data
#[derive(Debug, Clone)]
pub struct TaskResult<T>
where
    T: Send + Sync + Clone + Debug,
{
    pub task_id: TaskId,
    pub stage_id: StageId,
    pub partition_index: usize,
    pub result: RddResult<Vec<T>>,
    pub metrics: TaskMetrics,
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
pub enum ExecutorStatus {
    Starting,
    Running,
    Idle,
    Busy,
    Stopping,
    Failed,
}

impl Default for ExecutorStatus {
    fn default() -> Self {
        ExecutorStatus::Starting
    }
}

/// Task state enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskState {
    Pending,
    Running,
    Finished,
    Failed,
    Killed,
}

impl Default for TaskState {
    fn default() -> Self {
        TaskState::Pending
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
        task_type: &TaskType,
        properties: &HashMap<String, String>,
    ) -> Result<Vec<u8>> {
        let metadata = serde_json::json!({
            "task_id": task_id,
            "stage_id": stage_id,
            "partition_index": partition_index,
            "task_type": task_type,
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
