//! Distributed task execution and management
//!
//! This module provides task execution capabilities for the distributed
//! computing framework, including task runners and result handling.

use crate::distributed::driver::{RddOperation, TaskData};
use crate::distributed::types::*;

use rayon::prelude::*;

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info};

/// Task runner for executing distributed tasks
pub struct TaskRunner {
    /// Maximum number of concurrent tasks
    max_concurrent_tasks: usize,
    /// Task execution channel
    task_sender: mpsc::UnboundedSender<TaskExecution>,
    /// Running tasks tracking
    running_tasks: Arc<tokio::sync::Mutex<HashMap<TaskId, TaskHandle>>>,
    /// Semaphore to limit concurrent tasks
    semaphore: Arc<tokio::sync::Semaphore>,
}

/// Task execution request
struct TaskExecution {
    task_id: TaskId,
    stage_id: StageId,
    partition_index: usize,
    task_data: Vec<u8>,
    result_sender: oneshot::Sender<TaskExecutionResult>,
}

/// Task execution result
#[derive(Debug)]
pub struct TaskExecutionResult {
    pub task_id: TaskId,
    pub stage_id: StageId,
    pub partition_index: usize,
    pub state: TaskState,
    pub result: Option<Vec<u8>>,
    pub error_message: Option<String>,
    pub metrics: TaskMetrics,
}

/// Handle for a running task
struct TaskHandle {
    task_id: TaskId,
    start_time: Instant,
    cancel_sender: Option<oneshot::Sender<()>>,
}

impl TaskRunner {
    /// Create a new task runner
    pub fn new(max_concurrent_tasks: usize) -> Self {
        let (task_sender, mut task_receiver) = mpsc::unbounded_channel::<TaskExecution>();
        let running_tasks = Arc::new(tokio::sync::Mutex::new(HashMap::new()));

        // Create a semaphore to limit concurrency
        let semaphore = Arc::new(tokio::sync::Semaphore::new(max_concurrent_tasks));

        let running_tasks_clone = Arc::clone(&running_tasks);
        let semaphore_clone = Arc::clone(&semaphore);

        // Spawn task execution loop
        tokio::spawn(async move {
            while let Some(task_execution) = task_receiver.recv().await {
                let running_tasks = Arc::clone(&running_tasks_clone);
                let semaphore_clone = Arc::clone(&semaphore_clone);

                tokio::spawn(async move {
                    // Acquire a permit from the semaphore
                    let _permit = semaphore_clone.acquire().await.unwrap();
                    Self::execute_task_internal(task_execution, running_tasks).await;
                    // Permit is released when `_permit` goes out of scope
                });
            }
        });

        Self {
            max_concurrent_tasks,
            task_sender,
            running_tasks,
            semaphore,
        }
    }

    /// Submit a task for execution
    pub async fn submit_task(
        &self,
        task_id: TaskId,
        stage_id: StageId,
        partition_index: usize,
        task_data: Vec<u8>,
    ) -> Result<TaskExecutionResult, anyhow::Error> {
        let (result_sender, result_receiver) = oneshot::channel();

        let task_execution = TaskExecution {
            task_id,
            stage_id,
            partition_index,
            task_data,
            result_sender,
        };

        self.task_sender
            .send(task_execution)
            .map_err(|e| anyhow::anyhow!("Failed to submit task: {}", e))?;

        result_receiver
            .await
            .map_err(|e| anyhow::anyhow!("Failed to receive task result: {}", e))
    }

    /// Kill a running task
    pub async fn kill_task(&self, task_id: &TaskId, reason: &str) -> Result<(), anyhow::Error> {
        let mut running_tasks = self.running_tasks.lock().await;

        if let Some(task_handle) = running_tasks.remove(task_id) {
            if let Some(cancel_sender) = task_handle.cancel_sender {
                let _ = cancel_sender.send(());
                info!("Killed task {} with reason: {}", task_id, reason);
            }
        }

        Ok(())
    }

    /// Get the number of running tasks
    pub async fn running_task_count(&self) -> usize {
        self.running_tasks.lock().await.len()
    }

    /// Get information about running tasks
    pub async fn get_running_tasks(&self) -> Vec<(TaskId, Duration)> {
        let running_tasks = self.running_tasks.lock().await;
        running_tasks
            .values()
            .map(|handle| (handle.task_id.clone(), handle.start_time.elapsed()))
            .collect()
    }

    /// Internal task execution logic
    async fn execute_task_internal(
        task_execution: TaskExecution,
        running_tasks: Arc<tokio::sync::Mutex<HashMap<TaskId, TaskHandle>>>,
    ) {
        let start_time = Instant::now();
        let mut metrics = TaskMetrics::default(); // Will be filled by execution logic

        // Execute task and get result bytes or an error
        let execution_result =
            Self::deserialize_and_execute_task(&task_execution.task_data, &mut metrics).await;

        // Update metrics
        metrics.executor_run_time_ms = start_time.elapsed().as_millis() as u64;

        // Assemble the final result struct
        let final_result = match execution_result {
            Ok(result_bytes) => TaskExecutionResult {
                task_id: task_execution.task_id.clone(),
                stage_id: task_execution.stage_id.clone(),
                partition_index: task_execution.partition_index,
                state: TaskState::Finished,
                result: Some(result_bytes),
                error_message: None,
                metrics,
            },
            Err(e) => TaskExecutionResult {
                task_id: task_execution.task_id.clone(),
                stage_id: task_execution.stage_id.clone(),
                partition_index: task_execution.partition_index,
                state: TaskState::Failed,
                result: None,
                error_message: Some(e.to_string()),
                metrics,
            },
        };

        // Remove from running tasks
        {
            let mut running_tasks = running_tasks.lock().await;
            running_tasks.remove(&final_result.task_id);
        }

        // Send result back
        if task_execution.result_sender.send(final_result).is_err() {
            error!("Failed to send task result back to caller");
        }
    }

    /// Deserialize and execute a task
    async fn deserialize_and_execute_task(
        task_data: &[u8],
        metrics: &mut TaskMetrics,
    ) -> Result<Vec<u8>, anyhow::Error> {
        let deserialize_start = Instant::now();

        // Deserialize the task data, which is expected to be a `TaskData` struct.
        let (rdd_task_data, _): (TaskData, _) =
            bincode::decode_from_slice(task_data, bincode::config::standard())
                .map_err(|e| anyhow::anyhow!("Failed to deserialize task into TaskData: {}", e))?;

        metrics.executor_deserialize_time_ms = deserialize_start.elapsed().as_millis() as u64;

        // Execute RDD task with rayon parallel processing. This function now returns
        // the already serialized result.
        let execution_start = Instant::now();
        let serialized_result = Self::execute_rdd_task_with_rayon(&rdd_task_data).await?;
        metrics.executor_run_time_ms = execution_start.elapsed().as_millis() as u64;

        metrics.result_size_bytes = serialized_result.len() as u64;

        Ok(serialized_result)
    }

    /// Execute an RDD task with rayon parallel processing
    async fn execute_rdd_task_with_rayon(task_data: &TaskData) -> Result<Vec<u8>, anyhow::Error> {
        // =================================================================================
        // !!! ARCHITECTURAL NOTE: This function is a major simplification. !!!
        //
        // In a real distributed computing framework like Spark, this is where the magic
        // of executing arbitrary user code would happen. This would involve:
        //
        // 1.  **Deserializing Closures**: The `RddOperation`'s `closure_data` would contain
        //     a serialized function (e.g., from a `map` or `filter` call). This is very
        //     challenging in Rust due to its complex type and lifetime system. Libraries
        //     like `serde_closure` attempt to solve this but have limitations.
        // 2.  **Generic Data Types**: The function is currently hardcoded for `Vec<i32>`. A
        //     real implementation would be generic over `T` where `T` is serializable.
        // 3.  **Applying the Closure**: The deserialized closure would be applied to each
        //     element of the deserialized `partition_data`.
        //
        // The current implementation uses a pattern match on `RddOperation` to execute
        // hardcoded logic. This is a placeholder to demonstrate the Driver-Executor RPC
        // flow but does not provide the flexibility of a real RDD system.
        // =================================================================================
        info!("Executing RDD task with rayon parallel processing");

        // Get the operation directly from task data
        let operation = &task_data.operation;

        // For demonstration, we'll work with Vec<i32> data
        // In a real implementation, this would be generic over the data type
        let (partition_data, _): (Vec<i32>, _) =
            bincode::decode_from_slice(&task_data.partition_data, bincode::config::standard())
                .map_err(|e| anyhow::anyhow!("Failed to deserialize partition data: {}", e))?;

        debug!(
            "Processing {} elements with operation: {:?}",
            partition_data.len(),
            operation
        );
        debug!("Input partition data: {:?}", partition_data);

        // Execute the operation using rayon for parallel processing
        let result: Vec<i32> = match *operation {
            RddOperation::Map { closure_data: _ } => {
                // For demonstration, apply a simple map operation
                partition_data
                    .into_par_iter()
                    .map(|x| x * 2) // Simple doubling operation
                    .collect()
            }
            RddOperation::Filter { predicate_data: _ } => {
                // For demonstration, filter even numbers
                partition_data
                    .into_par_iter()
                    .filter(|&x| x % 2 == 0)
                    .collect()
            }
            RddOperation::Collect => {
                // Simply return the data as-is
                partition_data
            }
            RddOperation::Reduce { function_data: _ } => {
                // For demonstration, sum all elements and return as single-element vector
                let sum = partition_data.into_par_iter().sum::<i32>();
                vec![sum]
            }
            RddOperation::FlatMap { closure_data: _ } => {
                // For demonstration, duplicate each element
                partition_data
                    .into_par_iter()
                    .flat_map(|x| vec![x, x])
                    .collect()
            }
        };

        info!("RDD task completed, result size: {} elements", result.len());
        debug!("Output result data: {:?}", result);

        // Serialize the result
        let serialized_result = bincode::encode_to_vec(&result, bincode::config::standard())
            .map_err(|e| anyhow::anyhow!("Failed to serialize result: {}", e))?;

        Ok(serialized_result)
    }
}

/// Task scheduler for distributing tasks to executors
#[derive(Clone)]
pub struct TaskScheduler {
    /// Available executors
    executors: Arc<tokio::sync::Mutex<HashMap<ExecutorId, ExecutorInfo>>>,
    /// Pending tasks queue
    pending_tasks: Arc<tokio::sync::Mutex<Vec<PendingTask>>>,
}

/// Pending task information
#[derive(Debug, Clone)]
pub struct PendingTask {
    pub task_id: TaskId,
    pub stage_id: StageId,
    pub partition_index: usize,
    pub task_data: Vec<u8>,
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
    pub async fn submit_task(
        &self,
        task_id: TaskId,
        stage_id: StageId,
        partition_index: usize,
        task_data: Vec<u8>,
        preferred_executor: Option<ExecutorId>,
    ) {
        let pending_task = PendingTask {
            task_id: task_id.clone(),
            stage_id,
            partition_index,
            task_data,
            preferred_executor,
        };

        let mut pending_tasks = self.pending_tasks.lock().await;
        pending_tasks.push(pending_task);

        debug!("Submitted task {} for scheduling", task_id);
    }

    /// Submit a pending task back to the queue (e.g., on failure)
    pub async fn submit_pending_task(&self, pending_task: PendingTask) {
        let mut pending_tasks = self.pending_tasks.lock().await;
        // Add to the front for quick retry, or back for fairness. Let's add to back.
        pending_tasks.push(pending_task);
        debug!("Re-queued task for scheduling");
    }

    /// Get the next task for an executor
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
