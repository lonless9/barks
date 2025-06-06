//! Distributed task execution and management
//!
//! This module provides task execution capabilities for the distributed
//! computing framework. It defines a generic, serializable `Task` trait
//! that allows arbitrary computations to be executed by the Executor.
use crate::distributed::types::*;
use anyhow::Result;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info, warn};

/// A trait for any task that can be executed on an executor.
///
/// This trait is the key to solving the hardcoding and generics problem.
/// Any struct implementing this trait can be serialized, sent to an executor,
/// deserialized, and then executed. The `typetag` attribute, when combined
/// with a self-describing format like `serde_json`, makes the trait object serializable.
///
/// The typical serialization pattern is two-layered:
/// 1. The `Box<dyn Task>` object is serialized using `serde_json` to preserve type information.
/// 2. The raw data payload within the task struct (e.g., `partition_data`) is often
///    pre-serialized using an efficient binary format like `bincode`.
#[typetag::serde(tag = "type")]
#[async_trait::async_trait]
pub trait Task: Send + Sync {
    /// Executes the task logic on a partition's data and returns the result as serialized bytes.
    /// This method encapsulates the entire computation for a single partition.
    async fn execute(&self, partition_index: usize) -> Result<Vec<u8>, anyhow::Error>;
}

/// A concrete task that contains data and an operation identifier.
/// This is a flexible task that dispatches operations based on a string identifier.
/// It solves the problem of adding new operations without changing the Executor's code,
/// but it still relies on a match statement. A more advanced design would use
/// separate `Task` structs for each operation, leveraging `typetag` for dispatch.
#[derive(Serialize, Deserialize)]
pub struct DataMapTask {
    // Data is already serialized as Vec<T> into bincode bytes.
    // The `execute` method will deserialize it based on the operation.
    pub partition_data: Vec<u8>,
    pub operation_type: String, // e.g., "collect", "map_double_i32", "filter_even_i32"
}

#[typetag::serde]
#[async_trait::async_trait]
impl Task for DataMapTask {
    async fn execute(&self, _partition_index: usize) -> Result<Vec<u8>, anyhow::Error> {
        // This method demonstrates how to handle different operations on generic data.
        // The key is to deserialize the data *within* the branch for the specific operation,
        // as only that branch knows the concrete type (e.g., `i32`).

        let result_bytes = match self.operation_type.as_str() {
            "collect" => {
                // For collect, we don't need to know the type. Just return the raw bytes.
                self.partition_data.clone()
            }
            "map_double_i32" => {
                let (data, _): (Vec<i32>, _) =
                    bincode::decode_from_slice(&self.partition_data, bincode::config::standard())?;
                let result: Vec<i32> = data.par_iter().map(|x| x * 2).collect();
                bincode::encode_to_vec(&result, bincode::config::standard())
                    .map_err(|e| anyhow::anyhow!("Failed to serialize result: {}", e))?
            }
            "filter_even_i32" => {
                let (data, _): (Vec<i32>, _) =
                    bincode::decode_from_slice(&self.partition_data, bincode::config::standard())?;
                let result: Vec<i32> = data.par_iter().filter(|&x| x % 2 == 0).cloned().collect();
                bincode::encode_to_vec(&result, bincode::config::standard())
                    .map_err(|e| anyhow::anyhow!("Failed to serialize result: {}", e))?
            }
            _ => {
                return Err(anyhow::anyhow!(
                    "Unknown operation type: {}",
                    self.operation_type
                ));
            }
        };

        Ok(result_bytes)
    }
}

/// A task that executes a chain of serializable operations on i32 data.
/// This replaces the less flexible I32OperationTask.
#[derive(Serialize, Deserialize)]
pub struct ChainedI32Task {
    /// Serialized partition data as Vec<i32>
    pub partition_data: Vec<u8>,
    /// The full chain of operations to apply to the partition data.
    pub operations: Vec<crate::operations::SerializableI32Operation>,
}

#[typetag::serde]
#[async_trait::async_trait]
impl Task for ChainedI32Task {
    async fn execute(&self, _partition_index: usize) -> Result<Vec<u8>, anyhow::Error> {
        // 1. Deserialize the initial partition data.
        let (mut current_data, _): (Vec<i32>, _) =
            bincode::decode_from_slice(&self.partition_data, bincode::config::standard())?;

        // 2. Apply each operation in the chain sequentially. The output of one
        //    operation becomes the input for the next.
        for op in &self.operations {
            current_data = match op {
                crate::operations::SerializableI32Operation::Map(map_op) => {
                    current_data // Use Rayon for parallel map
                        .par_iter()
                        .map(|item| map_op.execute(*item))
                        .collect()
                }
                crate::operations::SerializableI32Operation::Filter(filter_op) => {
                    current_data // Use Rayon for parallel filter
                        .par_iter()
                        .filter(|&item| filter_op.test(item))
                        .cloned()
                        .collect()
                }
            };
        }

        // 3. Serialize the final result.
        bincode::encode_to_vec(&current_data, bincode::config::standard())
            .map_err(|e| anyhow::anyhow!("Failed to serialize result: {}", e))
    }
}

/// Task runner for executing distributed tasks
pub struct TaskRunner {
    /// Semaphore to limit concurrent tasks
    semaphore: Arc<tokio::sync::Semaphore>,
}

/// Task execution result
#[derive(Debug)]
pub struct TaskExecutionResult {
    pub state: TaskState,
    pub result: Option<Vec<u8>>,
    pub error_message: Option<String>,
    pub metrics: TaskMetrics,
}

impl TaskRunner {
    /// Create a new task runner
    pub fn new(max_concurrent_tasks: usize) -> Self {
        Self {
            semaphore: Arc::new(tokio::sync::Semaphore::new(max_concurrent_tasks)),
        }
    }

    /// Submit a task for execution
    pub async fn submit_task(
        &self,
        partition_index: usize,
        serialized_task: Vec<u8>,
    ) -> TaskExecutionResult {
        // Acquire a permit to limit concurrency
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("Semaphore closed");

        let start_time = Instant::now();
        let mut metrics = TaskMetrics::default();

        // Execute task and get result bytes or an error
        let execution_result =
            Self::deserialize_and_execute_task(partition_index, &serialized_task, &mut metrics)
                .await;

        metrics.executor_run_time_ms = start_time.elapsed().as_millis() as u64;

        // Drop permit to release the semaphore slot
        drop(permit);

        match execution_result {
            Ok(result_bytes) => TaskExecutionResult {
                state: TaskState::Finished,
                result: Some(result_bytes),
                error_message: None,
                metrics,
            },
            Err(e) => TaskExecutionResult {
                state: TaskState::Failed,
                result: None,
                error_message: Some(e.to_string()),
                metrics,
            },
        }
    }

    /// Kill a running task
    pub async fn kill_task(&self, task_id: &TaskId, _reason: &str) -> Result<(), anyhow::Error> {
        // In this simplified model, we can't easily interrupt the rayon-based
        // computation. A full implementation would require more complex cancellation logic.
        warn!(
            "Task killing is not fully implemented. Task {} may continue to run.",
            task_id
        );
        Ok(())
    }

    /// Deserialize and execute a task
    async fn deserialize_and_execute_task(
        partition_index: usize,
        serialized_task: &[u8],
        metrics: &mut TaskMetrics, // `metrics` is now mutable
    ) -> Result<Vec<u8>, anyhow::Error> {
        let deserialize_start = Instant::now();

        // Deserialize the trait object from JSON. `typetag` injects a "type" field
        // that `serde_json` uses to determine which concrete struct to create.
        // We must use a serde-compatible format like JSON.
        let task: Box<dyn Task> = serde_json::from_slice(serialized_task)
            .map_err(|e| anyhow::anyhow!("Failed to deserialize task with serde_json: {}", e))?;

        metrics.executor_deserialize_time_ms = deserialize_start.elapsed().as_millis() as u64;

        let execution_start = Instant::now();

        // The actual execution happens here. The `Task` object contains all necessary logic.
        let result_bytes = match task.execute(partition_index).await {
            Ok(bytes) => bytes,
            Err(e) => {
                return Err(e);
            }
        };

        metrics.executor_run_time_ms = execution_start.elapsed().as_millis() as u64;
        metrics.result_size_bytes = result_bytes.len() as u64;

        Ok(result_bytes)
    }
}

/// Task scheduler for distributing tasks to executors
#[derive(Clone)]
pub struct TaskScheduler {
    /// Available executors
    executors: Arc<tokio::sync::Mutex<HashMap<ExecutorId, ExecutorInfo>>>,
    /// Pending tasks queue (FIFO)
    pending_tasks: Arc<tokio::sync::Mutex<Vec<PendingTask>>>,
}

/// Pending task information
#[derive(Debug, Clone)]
pub struct PendingTask {
    pub task_id: TaskId,
    pub stage_id: StageId,
    pub partition_index: usize,
    // This will now be the serialized `Box<dyn Task>`
    pub serialized_task: Vec<u8>,
    pub preferred_executor: Option<ExecutorId>,
}

impl TaskScheduler {
    /// Create a new task scheduler
    pub fn new() -> Self {
        Self {
            executors: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            pending_tasks: Arc::new(tokio::sync::Mutex::new(Vec::new())),
        }
    }

    /// Register an executor
    pub async fn register_executor(&self, executor_info: ExecutorInfo) {
        let mut executors = self.executors.lock().await;
        info!("Registering executor: {}", executor_info.executor_id);
        executors.insert(executor_info.executor_id.clone(), executor_info);
    }

    /// Unregister an executor
    pub async fn unregister_executor(&self, executor_id: &ExecutorId) {
        let mut executors = self.executors.lock().await;
        if executors.remove(executor_id).is_some() {
            info!("Unregistered executor: {}", executor_id);
        }
    }

    /// Submit a task for scheduling
    pub async fn submit_pending_task(&self, pending_task: PendingTask) {
        let mut pending_tasks = self.pending_tasks.lock().await;
        // Add to the front for quick retry, or back for fairness. Let's add to back.
        pending_tasks.push(pending_task);
        debug!("Re-queued task for scheduling");
    }

    /// Get the next task from the queue for an executor
    pub async fn get_next_task(&self) -> Option<PendingTask> {
        let mut pending_tasks = self.pending_tasks.lock().await;

        // Simple FIFO for now
        if !pending_tasks.is_empty() {
            Some(pending_tasks.remove(0))
        } else {
            None
        }
    }

    /// Get the number of registered executors
    pub async fn executor_count(&self) -> usize {
        self.executors.lock().await.len()
    }

    /// Get the number of pending tasks
    pub async fn pending_task_count(&self) -> usize {
        self.pending_tasks.lock().await.len()
    }
}

impl Default for TaskScheduler {
    fn default() -> Self {
        Self::new()
    }
}
