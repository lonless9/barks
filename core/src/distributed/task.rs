//! Distributed task execution and management
//!
//! This module provides task execution capabilities for the distributed
//! computing framework, including task runners and result handling.

use crate::distributed::types::*;
use crate::traits::{Partition, RddResult};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, warn};

/// Task runner for executing distributed tasks
pub struct TaskRunner {
    /// Maximum number of concurrent tasks
    max_concurrent_tasks: usize,
    /// Task execution channel
    task_sender: mpsc::UnboundedSender<TaskExecution>,
    /// Running tasks tracking
    running_tasks: Arc<tokio::sync::Mutex<HashMap<TaskId, TaskHandle>>>,
}

/// Task execution request
struct TaskExecution {
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

        let running_tasks_clone = Arc::clone(&running_tasks);

        // Spawn task execution loop
        tokio::spawn(async move {
            while let Some(task_execution) = task_receiver.recv().await {
                let running_tasks = Arc::clone(&running_tasks_clone);

                // Execute task in a separate task
                tokio::spawn(async move {
                    Self::execute_task_internal(task_execution, running_tasks).await;
                });
            }
        });

        Self {
            max_concurrent_tasks,
            task_sender,
            running_tasks,
        }
    }

    /// Submit a task for execution
    pub async fn submit_task(
        &self,
        task_data: Vec<u8>,
    ) -> Result<TaskExecutionResult, anyhow::Error> {
        let (result_sender, result_receiver) = oneshot::channel();

        let task_execution = TaskExecution {
            task_data,
            result_sender,
        };

        self.task_sender
            .send(task_execution)
            .map_err(|_| anyhow::anyhow!("Failed to submit task"))?;

        result_receiver
            .await
            .map_err(|_| anyhow::anyhow!("Failed to receive task result"))
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
        let mut metrics = TaskMetrics::default();

        // Deserialize task
        let task_result =
            match Self::deserialize_and_execute_task(&task_execution.task_data, &mut metrics).await
            {
                Ok(result) => result,
                Err(e) => {
                    error!("Task execution failed: {}", e);
                    TaskExecutionResult {
                        task_id: "unknown".to_string(),
                        stage_id: "unknown".to_string(),
                        partition_index: 0,
                        state: TaskState::Failed,
                        result: None,
                        error_message: Some(e.to_string()),
                        metrics: metrics.clone(),
                    }
                }
            };

        // Update metrics
        metrics.executor_run_time_ms = start_time.elapsed().as_millis() as u64;

        // Remove from running tasks
        {
            let mut running_tasks = running_tasks.lock().await;
            running_tasks.remove(&task_result.task_id);
        }

        // Send result back
        let _ = task_execution.result_sender.send(task_result);
    }

    /// Deserialize and execute a task
    async fn deserialize_and_execute_task(
        task_data: &[u8],
        metrics: &mut TaskMetrics,
    ) -> Result<TaskExecutionResult, anyhow::Error> {
        let deserialize_start = Instant::now();

        // Try to deserialize as RDD task first, then fall back to simple task
        if let Ok((rdd_task_data, _)) = bincode::decode_from_slice::<
            crate::distributed::driver::TaskData,
            _,
        >(task_data, bincode::config::standard())
        {
            metrics.executor_deserialize_time_ms = deserialize_start.elapsed().as_millis() as u64;

            // Execute RDD task with rayon parallel processing
            let execution_start = Instant::now();
            let result = Self::execute_rdd_task_with_rayon(&rdd_task_data).await?;

            metrics.executor_run_time_ms = execution_start.elapsed().as_millis() as u64;

            // Serialize result
            let serialize_start = Instant::now();
            let serialized_result = bincode::encode_to_vec(&result, bincode::config::standard())
                .map_err(|e| anyhow::anyhow!("Failed to serialize result: {}", e))?;

            metrics.result_serialization_time_ms = serialize_start.elapsed().as_millis() as u64;
            metrics.result_size_bytes = serialized_result.len() as u64;

            Ok(TaskExecutionResult {
                task_id: "rdd_task".to_string(), // TODO: Extract from task data
                stage_id: "rdd_stage".to_string(),
                partition_index: 0,
                state: TaskState::Finished,
                result: Some(serialized_result),
                error_message: None,
                metrics: metrics.clone(),
            })
        } else {
            // Fall back to simple task execution
            let (task_info, _): (SimpleTaskInfo, _) =
                bincode::decode_from_slice(task_data, bincode::config::standard())
                    .map_err(|e| anyhow::anyhow!("Failed to deserialize task: {}", e))?;

            metrics.executor_deserialize_time_ms = deserialize_start.elapsed().as_millis() as u64;

            let execution_start = Instant::now();
            let result = Self::execute_simple_task(&task_info).await?;
            metrics.executor_run_time_ms = execution_start.elapsed().as_millis() as u64;

            // Serialize result using latest bincode API
            let serialize_start = Instant::now();
            let serialized_result = bincode::encode_to_vec(&result, bincode::config::standard())
                .map_err(|e| anyhow::anyhow!("Failed to serialize result: {}", e))?;

            metrics.result_serialization_time_ms = serialize_start.elapsed().as_millis() as u64;
            metrics.result_size_bytes = serialized_result.len() as u64;

            Ok(TaskExecutionResult {
                task_id: task_info.task_id,
                stage_id: task_info.stage_id,
                partition_index: task_info.partition_index,
                state: TaskState::Finished,
                result: Some(serialized_result),
                error_message: None,
                metrics: metrics.clone(),
            })
        }
    }

    /// Execute a simple task (placeholder implementation)
    async fn execute_simple_task(task_info: &SimpleTaskInfo) -> Result<Vec<i32>, anyhow::Error> {
        // This is a simplified task execution for demonstration
        // In a real implementation, this would execute the actual RDD computation

        debug!(
            "Executing task {} for partition {}",
            task_info.task_id, task_info.partition_index
        );

        // Simulate some computation using rayon for parallel processing
        let result: Vec<i32> = (0..task_info.data_size)
            .into_par_iter()
            .map(|i| (i as i32) * 2 + task_info.partition_index as i32)
            .collect();

        info!(
            "Task {} completed with {} elements",
            task_info.task_id,
            result.len()
        );

        Ok(result)
    }

    /// Execute an RDD task with rayon parallel processing
    async fn execute_rdd_task_with_rayon(
        task_data: &crate::distributed::driver::TaskData,
    ) -> Result<Vec<u8>, anyhow::Error> {
        info!("Executing RDD task with rayon parallel processing");

        // Deserialize the operation
        let (operation, _): (crate::distributed::driver::RddOperation, _) =
            bincode::decode_from_slice(&task_data.operation, bincode::config::standard())
                .map_err(|e| anyhow::anyhow!("Failed to deserialize operation: {}", e))?;

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
        let result: Vec<i32> = match operation {
            crate::distributed::driver::RddOperation::Map { closure_data: _ } => {
                // For demonstration, apply a simple map operation
                partition_data
                    .into_par_iter()
                    .map(|x| x * 2) // Simple doubling operation
                    .collect()
            }
            crate::distributed::driver::RddOperation::Filter { predicate_data: _ } => {
                // For demonstration, filter even numbers
                partition_data
                    .into_par_iter()
                    .filter(|&x| x % 2 == 0)
                    .collect()
            }
            crate::distributed::driver::RddOperation::Collect => {
                // Simply return the data as-is
                partition_data
            }
            crate::distributed::driver::RddOperation::Reduce { function_data: _ } => {
                // For demonstration, sum all elements and return as single-element vector
                let sum = partition_data.into_par_iter().sum::<i32>();
                vec![sum]
            }
            crate::distributed::driver::RddOperation::FlatMap { closure_data: _ } => {
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

    /// Execute an RDD task with proper operation handling
    async fn execute_rdd_task(task_info: &RddTaskInfo) -> Result<Vec<u8>, anyhow::Error> {
        debug!(
            "Executing RDD task {} for partition {} with operation {:?}",
            task_info.task_id, task_info.partition_index, task_info.operation_type
        );

        // For now, we'll implement basic operations
        // In a real implementation, this would deserialize the partition data
        // and apply the appropriate RDD operation
        match &task_info.operation_type {
            RddOperationType::Collect => {
                // Simply return the partition data as-is
                Ok(task_info.serialized_partition_data.clone())
            }
            RddOperationType::Map { function_id } => {
                info!("Applying map operation with function_id: {}", function_id);
                // For demonstration, we'll just return the data
                // In reality, we'd deserialize, apply the function, and re-serialize
                Ok(task_info.serialized_partition_data.clone())
            }
            RddOperationType::Filter { predicate_id } => {
                info!(
                    "Applying filter operation with predicate_id: {}",
                    predicate_id
                );
                // For demonstration, we'll just return the data
                // In reality, we'd deserialize, apply the predicate, and re-serialize
                Ok(task_info.serialized_partition_data.clone())
            }
            RddOperationType::Reduce { function_id } => {
                info!(
                    "Applying reduce operation with function_id: {}",
                    function_id
                );
                // For demonstration, we'll just return the data
                // In reality, we'd deserialize, apply the reduction, and re-serialize
                Ok(task_info.serialized_partition_data.clone())
            }
            RddOperationType::Custom {
                operation_id,
                function_data,
            } => {
                info!(
                    "Applying custom operation {} with {} bytes of function data",
                    operation_id,
                    function_data.len()
                );
                // For demonstration, we'll just return the data
                // In reality, we'd deserialize the function and apply it
                Ok(task_info.serialized_partition_data.clone())
            }
        }
    }
}

/// Simplified task information for demonstration
#[derive(Debug, Clone, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
pub struct SimpleTaskInfo {
    pub task_id: TaskId,
    pub stage_id: StageId,
    pub partition_index: usize,
    pub data_size: usize,
}

/// RDD task information for distributed execution
/// For now, we'll use a simplified approach with serialized data
#[derive(Debug, Clone, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
pub struct RddTaskInfo {
    pub task_id: TaskId,
    pub stage_id: StageId,
    pub partition_index: usize,
    pub serialized_partition_data: Vec<u8>,
    pub operation_type: RddOperationType,
}

/// Types of RDD operations that can be executed distributedly
#[derive(Debug, Clone, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
pub enum RddOperationType {
    /// Map operation - applies a function to each element
    Map { function_id: String },
    /// Filter operation - filters elements based on a predicate
    Filter { predicate_id: String },
    /// Collect operation - gathers all elements
    Collect,
    /// Reduce operation - reduces elements using an associative function
    Reduce { function_id: String },
    /// Custom operation with serialized function
    Custom {
        operation_id: String,
        function_data: Vec<u8>,
    },
}

impl SimpleTaskInfo {
    pub fn new(
        task_id: TaskId,
        stage_id: StageId,
        partition_index: usize,
        data_size: usize,
    ) -> Self {
        Self {
            task_id,
            stage_id,
            partition_index,
            data_size,
        }
    }
}

/// Task scheduler for distributing tasks to executors
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

    /// Get the next task for an executor
    pub async fn get_next_task(&self, executor_id: &ExecutorId) -> Option<PendingTask> {
        let mut pending_tasks = self.pending_tasks.lock().await;

        // First, try to find a task with preferred executor
        if let Some(pos) = pending_tasks
            .iter()
            .position(|task| task.preferred_executor.as_ref() == Some(executor_id))
        {
            return Some(pending_tasks.remove(pos));
        }

        // Otherwise, return any available task
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
