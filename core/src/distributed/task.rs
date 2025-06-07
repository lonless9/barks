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
/// deserialized, and then executed.
///
/// ## Serialization Pattern
/// A two-layer serialization approach is used for efficiency and correctness:
/// 1.  **Outer Layer (Trait Object):** The `Box<dyn Task>` is serialized using a self-describing
///     format like `serde_json`. The `typetag` crate automatically adds a `type` field to the
///     JSON output, allowing the correct concrete struct to be instantiated during deserialization.
/// 2.  **Inner Layer (Data Payload):** Large data payloads within the task struct (e.g., `partition_data`)
///     should be stored as `Vec<u8>`, pre-serialized with an efficient binary format like `bincode`.
///
/// This combination provides the flexibility of JSON for the trait object and the performance of
/// bincode for the bulk data.
#[typetag::serde(tag = "type")]
pub trait Task: Send + Sync {
    /// Executes the task logic on a partition's data and returns the result as serialized bytes.
    /// This method encapsulates the entire computation for a single partition.
    fn execute(&self, partition_index: usize) -> Result<Vec<u8>, String>;
}

/// A task that executes a chain of serializable operations on i32 data.
/// This replaces the less flexible I32OperationTask.
/// This struct is a concrete implementation of the `Task` trait and serves as the primary
/// model for how to package a stage of computation for an RDD partition.
#[derive(Serialize, Deserialize, Debug)]
pub struct ChainedI32Task {
    /// Serialized partition data as Vec<i32>
    pub partition_data: Vec<u8>,
    /// The full chain of operations to apply to the partition data.
    pub operations: Vec<crate::operations::SerializableI32Operation>,
}

#[typetag::serde]
impl Task for ChainedI32Task {
    fn execute(&self, _partition_index: usize) -> Result<Vec<u8>, String> {
        // 1. Deserialize the initial partition data.
        let (mut current_data, _): (Vec<i32>, _) =
            bincode::decode_from_slice(&self.partition_data, bincode::config::standard())
                .map_err(|e| format!("Failed to deserialize partition data: {}", e))?;

        // 2. Apply each operation in the chain sequentially. The output of one
        //    operation becomes the input for the next.
        for op in &self.operations {
            current_data = match op {
                crate::operations::SerializableI32Operation::Map(map_op) => current_data
                    .par_iter()
                    .map(|item| map_op.execute(*item))
                    .collect(),
                crate::operations::SerializableI32Operation::Filter(filter_op) => current_data
                    .par_iter()
                    .filter(|&item| filter_op.test(item))
                    .cloned()
                    .collect(),
            };
        }

        // 3. Serialize the final result.
        bincode::encode_to_vec(&current_data, bincode::config::standard())
            .map_err(|e| format!("Failed to serialize result: {}", e))
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

        let _start_time = Instant::now();
        let mut metrics = TaskMetrics::default();

        // Execute the task in a separate blocking thread and handle all possible outcomes,
        // including panics, to prevent the executor from crashing.
        let execution_result = tokio::task::spawn_blocking(move || {
            Self::deserialize_and_execute_task(partition_index, &serialized_task, &mut metrics)
        })
        .await
        .unwrap_or_else(|join_error| {
            // This case handles panics within the blocking task.
            Err(format!("Task execution panicked: {}", join_error))
        });

        // Drop permit to release the semaphore slot
        drop(permit);

        match execution_result {
            Ok((result_bytes, task_metrics)) => TaskExecutionResult {
                state: TaskState::Finished,
                result: Some(result_bytes),
                error_message: None,
                metrics: task_metrics,
            },
            Err(e) => TaskExecutionResult {
                state: TaskState::Failed,
                result: None,
                error_message: Some(e.to_string()),
                metrics: TaskMetrics::default(),
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

    // This is now a blocking function suitable for `spawn_blocking`
    fn deserialize_and_execute_task(
        partition_index: usize,
        serialized_task: &Vec<u8>,
        metrics: &mut TaskMetrics,
    ) -> Result<(Vec<u8>, TaskMetrics), String> {
        let deserialize_start = Instant::now();
        let task: Box<dyn Task> = serde_json::from_slice(serialized_task)
            .map_err(|e| format!("Failed to deserialize task with serde_json: {}", e))?;
        metrics.executor_deserialize_time_ms = deserialize_start.elapsed().as_millis() as u64;

        let execution_start = Instant::now();
        let result_bytes = task.execute(partition_index)?; // Errors are now String

        metrics.executor_run_time_ms = execution_start.elapsed().as_millis() as u64;
        metrics.result_size_bytes = result_bytes.len() as u64;

        Ok((result_bytes, metrics.clone()))
    }
}

/// Task scheduler for distributing tasks to executors
#[derive(Clone)]
pub struct TaskScheduler {
    /// Available executors
    executors: Arc<tokio::sync::Mutex<HashMap<ExecutorId, ExecutorInfo>>>,
    /// Pending tasks queue (FIFO)
    // Using VecDeque for more efficient queue operations (pop_front)
    pending_tasks: Arc<tokio::sync::Mutex<std::collections::VecDeque<PendingTask>>>,
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
    pub retries: u32,
}

impl TaskScheduler {
    /// Create a new task scheduler
    pub fn new() -> Self {
        Self {
            executors: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            pending_tasks: Arc::new(tokio::sync::Mutex::new(std::collections::VecDeque::new())),
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
        // Add to the back for fairness (FIFO)
        pending_tasks.push_back(pending_task);
        debug!(
            "Submitted task to pending queue. Queue size: {}",
            pending_tasks.len()
        );
    }

    /// Get the next task from the queue for an executor
    pub async fn get_next_task(&self) -> Option<PendingTask> {
        let mut pending_tasks = self.pending_tasks.lock().await;
        pending_tasks.pop_front()
    }

    /// Finds and removes a pending task by its ID.
    /// This is not very efficient but is needed for the simple re-queueing logic.
    /// A better implementation would use a HashMap for quick lookups inside the Driver.
    pub async fn get_pending_task_by_id(&self, task_id: &TaskId) -> Option<PendingTask> {
        let mut pending_tasks = self.pending_tasks.lock().await;
        if let Some(pos) = pending_tasks.iter().position(|t| &t.task_id == task_id) {
            pending_tasks.remove(pos)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operations::{DoubleOperation, GreaterThanPredicate, SerializableI32Operation};

    #[tokio::test]
    async fn test_chained_i32_task_execution() {
        // Test the ChainedI32Task with a chain of operations
        let data = vec![1, 2, 3, 4, 5, 10, 11, 12, 13, 14, 15];
        let serialized_data = bincode::encode_to_vec(&data, bincode::config::standard()).unwrap();

        let operations = vec![
            SerializableI32Operation::Map(Box::new(DoubleOperation)),
            SerializableI32Operation::Filter(Box::new(GreaterThanPredicate { threshold: 20 })),
        ];

        let task = ChainedI32Task {
            partition_data: serialized_data,
            operations,
        };

        let result_bytes = task.execute(0).unwrap();
        let (result, _): (Vec<i32>, _) =
            bincode::decode_from_slice(&result_bytes, bincode::config::standard()).unwrap();

        // Expected: [1,2,3,4,5,10,11,12,13,14,15] -> double -> [2,4,6,8,10,20,22,24,26,28,30] -> filter > 20 -> [22,24,26,28,30]
        let expected: Vec<i32> = data.iter().map(|x| x * 2).filter(|&x| x > 20).collect();

        assert_eq!(result.len(), expected.len());
        for item in &expected {
            assert!(result.contains(item));
        }
    }

    #[tokio::test]
    async fn test_chained_i32_task_serialization() {
        // Test that ChainedI32Task can be serialized and deserialized
        let data = vec![1, 2, 3];
        let serialized_data = bincode::encode_to_vec(&data, bincode::config::standard()).unwrap();

        let operations = vec![SerializableI32Operation::Map(Box::new(DoubleOperation))];

        let task = ChainedI32Task {
            partition_data: serialized_data,
            operations,
        };

        // Serialize the task as a trait object
        let task_box: Box<dyn Task> = Box::new(task);
        let serialized_task = serde_json::to_vec(&task_box).unwrap();

        // Deserialize the task
        let deserialized_task: Box<dyn Task> = serde_json::from_slice(&serialized_task).unwrap();

        // Execute the deserialized task
        let result_bytes = deserialized_task.execute(0).unwrap();
        let (result, _): (Vec<i32>, _) =
            bincode::decode_from_slice(&result_bytes, bincode::config::standard()).unwrap();

        assert_eq!(result, vec![2, 4, 6]);
    }

    /// A custom task for testing extensibility, as shown in examples.
    #[derive(Serialize, Deserialize)]
    pub struct CustomSquareTask {
        pub partition_data: Vec<u8>, // bincode-serialized Vec<i32>
    }

    #[typetag::serde]
    impl Task for CustomSquareTask {
        fn execute(&self, _partition_index: usize) -> Result<Vec<u8>, String> {
            let (data, _): (Vec<i32>, usize) =
                bincode::decode_from_slice(&self.partition_data, bincode::config::standard())
                    .map_err(|e| format!("Failed to deserialize partition data: {}", e))?;
            let result: Vec<i32> = data.iter().map(|x| x * x).collect();
            bincode::encode_to_vec(&result, bincode::config::standard())
                .map_err(|e| format!("Failed to serialize result: {}", e))
        }
    }

    #[tokio::test]
    async fn test_custom_task_serialization_and_execution() {
        let data = vec![1, 2, 3, 4, 5];
        let serialized_data = bincode::encode_to_vec(&data, bincode::config::standard()).unwrap();

        // This test demonstrates how to extend the framework with custom logic.
        // a different task type, demonstrating the flexibility of the `Task` trait system.
        // This is a powerful pattern for extending the framework with custom logic.

        // Create a custom task trait object
        let task: Box<dyn Task> = Box::new(CustomSquareTask {
            partition_data: serialized_data,
        });

        // Serialize it
        let serialized_task = serde_json::to_vec(&task).unwrap();

        // Deserialize it
        let deserialized_task: Box<dyn Task> = serde_json::from_slice(&serialized_task).unwrap();

        let result_bytes = deserialized_task.execute(0).unwrap();
        let (result, _): (Vec<i32>, _) =
            bincode::decode_from_slice(&result_bytes, bincode::config::standard()).unwrap();
        assert_eq!(result, vec![1, 4, 9, 16, 25]);
    }
}
