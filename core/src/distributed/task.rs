//! Distributed task execution and management
//!
//! This module provides task execution capabilities for the distributed
//! computing framework. It defines a generic, serializable `Task` trait
//! that allows arbitrary computations to be executed by the Executor.
use crate::distributed::proto::driver::TaskState;
use crate::distributed::types::*;
use crate::operations::RddDataType;
use anyhow::Result;
use bumpalo::Bump;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info, warn};

// Import shuffle-related types
use barks_network_shuffle::traits::MapStatus;

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
    /// The arena parameter provides efficient memory allocation for intermediate computations.
    fn execute(&self, partition_index: usize, arena: &Bump) -> Result<Vec<u8>, String>;
}

/// A generic task that executes a chain of serializable operations on data of type T.
/// This struct is a concrete implementation of the Task trait and serves as the primary
/// model for how to package a stage of computation for an RDD partition.
#[derive(Serialize, Deserialize, Debug)]
pub struct ChainedTask<T: crate::operations::RddDataType> {
    /// Serialized partition data as Vec<T>
    pub partition_data: Vec<u8>,
    /// The full chain of operations to apply to the partition data.
    pub operations: Vec<T::SerializableOperation>,
    /// Phantom data to make the struct generic over T.
    #[serde(skip)]
    _marker: std::marker::PhantomData<T>,
}

impl<T: crate::operations::RddDataType> ChainedTask<T> {
    pub fn new(partition_data: Vec<u8>, operations: Vec<T::SerializableOperation>) -> Self {
        Self {
            partition_data,
            operations,
            _marker: std::marker::PhantomData,
        }
    }
}

// Generic implementation for any type that implements RddDataType
// Note: We can't use #[typetag::serde] with generic implementations,
// so we need specific implementations for each concrete type.

#[typetag::serde(name = "ChainedTaskI32")]
impl Task for ChainedTask<i32> {
    fn execute(&self, _partition_index: usize, arena: &Bump) -> Result<Vec<u8>, String> {
        // 1. Deserialize the initial partition data.
        let (mut current_data, _): (Vec<i32>, _) =
            bincode::decode_from_slice(&self.partition_data, bincode::config::standard())
                .map_err(|e| format!("Failed to deserialize partition data: {}", e))?;

        // 2. Apply each operation in the chain sequentially. The output of one
        //    operation becomes the input for the next.
        for op in &self.operations {
            current_data = i32::apply_operation(op, current_data, arena);
        }

        // 3. Serialize the final result.
        bincode::encode_to_vec(&current_data, bincode::config::standard())
            .map_err(|e| format!("Failed to serialize result: {}", e))
    }
}

#[typetag::serde(name = "ChainedTaskString")]
impl Task for ChainedTask<String> {
    fn execute(&self, _partition_index: usize, arena: &Bump) -> Result<Vec<u8>, String> {
        // 1. Deserialize the initial partition data.
        let (mut current_data, _): (Vec<String>, _) =
            bincode::decode_from_slice(&self.partition_data, bincode::config::standard())
                .map_err(|e| format!("Failed to deserialize partition data: {}", e))?;

        // 2. Apply each operation in the chain sequentially.
        for op in &self.operations {
            current_data = String::apply_operation(op, current_data, arena);
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
                state: TaskState::TaskFinished,
                result: Some(result_bytes),
                error_message: None,
                metrics: task_metrics,
            },
            Err(e) => TaskExecutionResult {
                state: TaskState::TaskFailed,
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
        serialized_task: &[u8],
        metrics: &mut TaskMetrics,
    ) -> Result<(Vec<u8>, TaskMetrics), String> {
        let deserialize_start = Instant::now();
        let task: Box<dyn Task> = serde_json::from_slice(serialized_task)
            .map_err(|e| format!("Failed to deserialize task with serde_json: {}", e))?;
        metrics.executor_deserialize_time_ms = deserialize_start.elapsed().as_millis() as u64;

        let execution_start = Instant::now();

        // Create a new Bump arena for this specific task.
        let arena = Bump::new();
        // Pass the arena to the execute method.
        let result_bytes = task.execute(partition_index, &arena)?; // Errors are now String

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

    /// Get the next task from the queue for an executor
    /// It prioritizes tasks that have a preference for the given executor.
    pub async fn get_next_task_for_executor(
        &self,
        executor_id: &ExecutorId,
    ) -> Option<PendingTask> {
        let mut pending_tasks = self.pending_tasks.lock().await;
        if pending_tasks.is_empty() {
            return None;
        }

        // Pass 1: Find a task with a matching locality preference.
        // We search from the front to maintain FIFO for local tasks.
        if let Some(pos) = pending_tasks
            .iter()
            .position(|t| t.preferred_executor.as_ref() == Some(executor_id))
        {
            return pending_tasks.remove(pos);
        }

        // Pass 2: If no local task is found, return the next task from the front of the queue.
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

    /// Get a list of all registered executor IDs.
    pub async fn list_executor_ids(&self) -> Vec<ExecutorId> {
        self.executors.lock().await.keys().cloned().collect()
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
        // Test the ChainedTask<i32> with a chain of operations
        let data = vec![1, 2, 3, 4, 5, 10, 11, 12, 13, 14, 15];
        let operations = vec![
            SerializableI32Operation::Map(Box::new(DoubleOperation)),
            SerializableI32Operation::Filter(Box::new(GreaterThanPredicate { threshold: 20 })),
        ];

        let task = ChainedTask::<i32>::new(
            bincode::encode_to_vec(&data, bincode::config::standard()).unwrap(),
            operations,
        );

        let arena = Bump::new();
        let result_bytes = task.execute(0, &arena).unwrap();
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
        // Test that ChainedTask<i32> can be serialized and deserialized
        let data = vec![1, 2, 3];
        let operations = vec![SerializableI32Operation::Map(Box::new(DoubleOperation))];

        let task = ChainedTask::<i32>::new(
            bincode::encode_to_vec(&data, bincode::config::standard()).unwrap(),
            operations,
        );

        // Serialize the task as a trait object
        let task_box: Box<dyn Task> = Box::new(task);
        let serialized_task = serde_json::to_vec(&task_box).unwrap();

        // Deserialize the task
        let deserialized_task: Box<dyn Task> = serde_json::from_slice(&serialized_task).unwrap();

        // Execute the deserialized task
        let arena = Bump::new();
        let result_bytes = deserialized_task.execute(0, &arena).unwrap();
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
        fn execute(&self, _partition_index: usize, _arena: &Bump) -> Result<Vec<u8>, String> {
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

        let arena = Bump::new();
        let result_bytes = deserialized_task.execute(0, &arena).unwrap();
        let (result, _): (Vec<i32>, _) =
            bincode::decode_from_slice(&result_bytes, bincode::config::standard()).unwrap();
        assert_eq!(result, vec![1, 4, 9, 16, 25]);
    }
}

/// A task that performs shuffle map operations.
/// This task partitions data by key and writes shuffle blocks for reduce tasks.
#[derive(Serialize, Deserialize, Debug)]
pub struct ShuffleMapTask<K, V>
where
    K: crate::traits::Data,
    V: crate::traits::Data,
{
    /// Serialized partition data as Vec<(K, V)>
    pub partition_data: Vec<u8>,
    /// Shuffle ID for this shuffle operation
    pub shuffle_id: u32,
    /// Number of reduce partitions
    pub num_reduce_partitions: u32,
    /// Phantom data to make the struct generic over K and V
    #[serde(skip)]
    _marker: std::marker::PhantomData<(K, V)>,
}

impl<K, V> ShuffleMapTask<K, V>
where
    K: crate::traits::Data,
    V: crate::traits::Data,
{
    pub fn new(partition_data: Vec<u8>, shuffle_id: u32, num_reduce_partitions: u32) -> Self {
        Self {
            partition_data,
            shuffle_id,
            num_reduce_partitions,
            _marker: std::marker::PhantomData,
        }
    }
}

// Specific implementations for concrete types
#[typetag::serde(name = "ShuffleMapTaskStringI32")]
impl Task for ShuffleMapTask<String, i32> {
    fn execute(&self, partition_index: usize, _arena: &Bump) -> Result<Vec<u8>, String> {
        // In a real executor, the block manager would be injected/retrieved from context.
        // For this task, we assume a local temp directory for shuffle files.
        let temp_dir = tempfile::tempdir()
            .map_err(|e| format!("Failed to create temp dir for shuffle: {}", e))?;
        let block_manager = Arc::new(
            barks_network_shuffle::FileShuffleBlockManager::new(temp_dir.path())
                .map_err(|e| format!("Failed to create FileShuffleBlockManager: {}", e))?,
        );

        // 1. Deserialize the partition data
        let (data, _): (Vec<(String, i32)>, _) =
            bincode::decode_from_slice(&self.partition_data, bincode::config::standard())
                .map_err(|e| format!("Failed to deserialize partition data: {}", e))?;

        // 2. Create a shuffle writer
        let partitioner = Arc::new(crate::shuffle::HashPartitioner::new(
            self.num_reduce_partitions,
        ));
        let _writer: barks_network_shuffle::BytewaxShuffleWriter<String, i32> =
            barks_network_shuffle::BytewaxShuffleWriter::new(
                self.shuffle_id,
                partition_index as u32,
                partitioner,
                block_manager,
            );

        // 3. For now, we'll simulate the shuffle write operation without async
        // In a real implementation, this would use the actual shuffle writer
        // but we need to avoid creating nested runtimes

        // Simulate partitioning the data
        let mut partitioned_data: std::collections::HashMap<u32, Vec<(String, i32)>> =
            std::collections::HashMap::new();

        for (key, value) in data {
            // Simple hash partitioning
            let partition_id = {
                use std::hash::{Hash, Hasher};
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                key.hash(&mut hasher);
                (hasher.finish() % self.num_reduce_partitions as u64) as u32
            };

            partitioned_data
                .entry(partition_id)
                .or_default()
                .push((key, value));
        }

        // 4. Create MapStatus with block sizes
        let map_status = MapStatus::new({
            let mut block_sizes = std::collections::HashMap::new();
            for (partition_id, partition_data) in &partitioned_data {
                let serialized_size =
                    bincode::encode_to_vec(partition_data, bincode::config::standard())
                        .map_err(|e| format!("Failed to serialize partition data: {}", e))?
                        .len() as u64;
                block_sizes.insert(*partition_id, serialized_size);
            }
            block_sizes
        });

        // 5. Serialize and return the MapStatus
        bincode::encode_to_vec(&map_status, bincode::config::standard())
            .map_err(|e| format!("Failed to serialize MapStatus: {}", e))
    }
}

/// A task that performs shuffle reduce operations.
/// This task reads shuffle blocks from multiple map tasks and aggregates values by key.
#[derive(Serialize, Deserialize, Debug)]
pub struct ShuffleReduceTask<K, V, C, A>
where
    K: crate::traits::Data,
    V: crate::traits::Data,
    C: crate::traits::Data,
    A: crate::shuffle::Aggregator<K, V, C>,
{
    /// Shuffle ID for this shuffle operation
    pub shuffle_id: u32,
    /// Reduce partition ID that this task will process
    pub reduce_partition_id: u32,
    /// Locations of map task outputs: a list of (executor_address, map_id)
    /// The executor address should be in "host:shuffle_port" format.
    pub map_output_locations: Vec<(String, u32)>,
    /// Serialized aggregator function
    pub aggregator_data: Vec<u8>,
    /// Phantom data to make the struct generic over K, V, C, A
    #[serde(skip)]
    _marker: std::marker::PhantomData<(K, V, C, A)>,
}

impl<K, V, C, A> ShuffleReduceTask<K, V, C, A>
where
    K: crate::traits::Data,
    V: crate::traits::Data,
    C: crate::traits::Data,
    A: crate::shuffle::Aggregator<K, V, C>,
{
    pub fn new(
        shuffle_id: u32,
        reduce_partition_id: u32,
        map_output_locations: Vec<(String, u32)>,
        aggregator_data: Vec<u8>,
    ) -> Self {
        Self {
            shuffle_id,
            reduce_partition_id,
            map_output_locations,
            aggregator_data,
            _marker: std::marker::PhantomData,
        }
    }
}

#[typetag::serde(name = "ShuffleReduceTaskStringI32")]
impl Task for ShuffleReduceTask<String, i32, i32, crate::shuffle::ReduceAggregator<i32>> {
    fn execute(&self, _partition_index: usize, _arena: &Bump) -> Result<Vec<u8>, String> {
        // In a real implementation, this would:
        // 1. Use map_output_locations to fetch shuffle blocks from remote executors
        // 2. Deserialize the aggregator from aggregator_data
        // 3. Group all values by key across all fetched blocks
        // 4. Apply the aggregator to combine values for each key
        // 5. Return the final aggregated results

        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| format!("Failed to create Tokio runtime: {}", e))?;

        rt.block_on(async {
            // 1. Create the aggregator (for now, we'll use a simple addition aggregator)
            // TODO: Implement proper aggregator serialization/deserialization
            let aggregator = crate::shuffle::ReduceAggregator::new(|a, b| a + b);

            // 2. Create a shuffle reader
            let reader = barks_network_shuffle::shuffle::BytewaxShuffleReader::<String, i32>::new();

            // 3. Fetch all blocks for this reduce partition
            let all_blocks = reader
                .read_partition(
                    self.shuffle_id,
                    self.reduce_partition_id,
                    &self.map_output_locations,
                )
                .await
                .map_err(|e| format!("Failed to read shuffle partition: {}", e))?;

            // 4. Aggregate the key-value pairs
            let mut combiners = HashMap::<String, i32>::new();
            for block in all_blocks {
                for (key, value) in block {
                    match combiners.get_mut(&key) {
                        Some(combiner) => {
                            *combiner = <crate::shuffle::ReduceAggregator<i32> as crate::shuffle::aggregator::Aggregator<String, i32, i32>>::merge_value(&aggregator, *combiner, value);
                        }
                        None => {
                            let new_combiner = <crate::shuffle::ReduceAggregator<i32> as crate::shuffle::aggregator::Aggregator<String, i32, i32>>::create_combiner(&aggregator, value);
                            combiners.insert(key, new_combiner);
                        }
                    }
                }
            }

            // 5. Convert to vector of tuples for serialization
            let result: Vec<(String, i32)> = combiners.into_iter().collect();

            // 6. Serialize and return the final results
            bincode::encode_to_vec(&result, bincode::config::standard())
                .map_err(|e| format!("Failed to serialize reduce result: {}", e))
        })
    }
}
