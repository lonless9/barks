//! Distributed task execution and management
//!
//! This module provides task execution capabilities for the distributed
//! computing framework. It defines a generic, serializable `Task` trait
//! that allows arbitrary computations to be executed by the Executor.
use crate::distributed::proto::driver::TaskState;
use crate::distributed::types::*;
use crate::operations::RddDataType;
use anyhow::Result;
use barks_network_shuffle::traits::ShuffleBlockManager;
use bumpalo::Bump;
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
#[async_trait::async_trait]
pub trait Task: Send + Sync {
    /// Executes the task logic on a partition's data and returns the result as serialized bytes.
    /// This method encapsulates the entire computation for a single partition.
    /// The block_manager parameter provides access to shuffle data storage.
    async fn execute(
        &self,
        partition_index: usize,
        block_manager: Arc<dyn barks_network_shuffle::traits::ShuffleBlockManager>,
    ) -> Result<Vec<u8>, String>;
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
macro_rules! impl_chained_task {
    ($type:ty, $name:literal) => {
        #[typetag::serde(name = $name)]
        #[async_trait::async_trait]
        impl Task for ChainedTask<$type> {
            async fn execute(
                &self,
                _partition_index: usize,
                _block_manager: Arc<dyn ShuffleBlockManager>,
            ) -> Result<Vec<u8>, String> {
                // This is a CPU-bound operation, so we run it in a blocking thread
                // to avoid starving the async runtime.
                let partition_data = self.partition_data.clone();
                let operations = self.operations.clone();

                tokio::task::spawn_blocking(move || {
                    // Each blocking task gets its own arena.
                    let arena = Bump::new();

                    // 1. Deserialize the initial partition data.
                    let (mut current_data, _): (Vec<$type>, _) =
                        bincode::decode_from_slice(&partition_data, bincode::config::standard())
                            .map_err(|e| format!("Failed to deserialize partition data: {}", e))?;

                    // 2. Apply each operation in the chain sequentially.
                    for op in &operations {
                        current_data = <$type>::apply_operation(op, current_data, &arena);
                    }

                    // 3. Serialize the final result.
                    bincode::encode_to_vec(&current_data, bincode::config::standard())
                        .map_err(|e| format!("Failed to serialize result: {}", e))
                })
                .await
                .map_err(|e| format!("Task panicked: {}", e))?
            }
        }
    };
}

impl_chained_task!(i32, "ChainedTaskI32");
impl_chained_task!(String, "ChainedTaskString");
impl_chained_task!((String, i32), "ChainedTaskStringI32Tuple");
impl_chained_task!((i32, String), "ChainedTaskI32StringTuple");

/// Task runner for executing distributed tasks
pub struct TaskRunner {
    /// Semaphore to limit concurrent tasks
    semaphore: Arc<tokio::sync::Semaphore>,
    /// Shared shuffle block manager from the executor
    block_manager: Arc<dyn ShuffleBlockManager>,
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
    pub fn new(max_concurrent_tasks: usize, block_manager: Arc<dyn ShuffleBlockManager>) -> Self {
        Self {
            semaphore: Arc::new(tokio::sync::Semaphore::new(max_concurrent_tasks)),
            block_manager,
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

        let mut metrics = TaskMetrics::default();

        // Deserialize the task first. This is quick and done on the async thread.
        let deserialize_start = Instant::now();
        let task: Box<dyn Task> = match serde_json::from_slice(&serialized_task) {
            Ok(task) => task,
            Err(e) => {
                return TaskExecutionResult {
                    state: TaskState::TaskFailed,
                    result: None,
                    error_message: Some(format!(
                        "Failed to deserialize task with serde_json: {}",
                        e
                    )),
                    metrics,
                };
            }
        };
        metrics.executor_deserialize_time_ms = deserialize_start.elapsed().as_millis() as u64;

        // Now execute the task asynchronously.
        let execution_start = Instant::now();
        let execution_result = task
            .execute(partition_index, self.block_manager.clone())
            .await;

        metrics.executor_run_time_ms = execution_start.elapsed().as_millis() as u64;

        // Drop permit to release the semaphore slot
        drop(permit);

        match execution_result {
            Ok(result_bytes) => {
                metrics.result_size_bytes = result_bytes.len() as u64;
                TaskExecutionResult {
                    state: TaskState::TaskFinished,
                    result: Some(result_bytes),
                    error_message: None,
                    metrics,
                }
            }
            Err(e) => TaskExecutionResult {
                state: TaskState::TaskFailed,
                result: None,
                error_message: Some(e),
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

        // Create a mock block manager for testing
        let block_manager = Arc::new(barks_network_shuffle::shuffle::MemoryShuffleManager::new());
        let result_bytes = task.execute(0, block_manager).await.unwrap();
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
        let block_manager = Arc::new(barks_network_shuffle::shuffle::MemoryShuffleManager::new());
        let result_bytes = deserialized_task.execute(0, block_manager).await.unwrap();
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
    #[async_trait::async_trait]
    impl Task for CustomSquareTask {
        async fn execute(
            &self,
            _partition_index: usize,
            _block_manager: Arc<dyn ShuffleBlockManager>,
        ) -> Result<Vec<u8>, String> {
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

        let block_manager = Arc::new(barks_network_shuffle::shuffle::MemoryShuffleManager::new());
        let result_bytes = deserialized_task.execute(0, block_manager).await.unwrap();
        let (result, _): (Vec<i32>, _) =
            bincode::decode_from_slice(&result_bytes, bincode::config::standard()).unwrap();
        assert_eq!(result, vec![1, 4, 9, 16, 25]);
    }
}

/// A task that performs shuffle map operations.
/// This task partitions data by key and writes shuffle blocks for reduce tasks.
#[derive(Serialize, Deserialize, Debug)]
pub struct ShuffleMapTask<T>
where
    T: RddDataType,
{
    /// Serialized partition data for the parent RDD stage
    pub parent_partition_data: Vec<u8>,
    /// The chain of operations for the parent RDD stage
    pub parent_operations: Vec<T::SerializableOperation>,
    /// Shuffle ID for this shuffle operation
    pub shuffle_id: u32,
    /// Number of reduce partitions
    pub num_reduce_partitions: u32,
    /// Phantom data to make the struct generic over T
    #[serde(skip)]
    _marker: std::marker::PhantomData<T>,
}

impl<T> ShuffleMapTask<T>
where
    T: RddDataType,
{
    pub fn new(
        parent_partition_data: Vec<u8>,
        parent_operations: Vec<T::SerializableOperation>,
        shuffle_id: u32,
        num_reduce_partitions: u32,
    ) -> Self {
        Self {
            parent_partition_data,
            parent_operations,
            shuffle_id,
            num_reduce_partitions,
            _marker: std::marker::PhantomData,
        }
    }
}

// Specific implementations for concrete types
#[typetag::serde(name = "ShuffleMapTaskStringI32")]
#[async_trait::async_trait]
impl Task for ShuffleMapTask<(String, i32)> {
    async fn execute(
        &self,
        partition_index: usize,
        block_manager: Arc<dyn ShuffleBlockManager>,
    ) -> Result<Vec<u8>, String> {
        // 1. Run the parent RDD's computation logic first.
        let parent_result = tokio::task::spawn_blocking({
            let partition_data = self.parent_partition_data.clone();
            let operations = self.parent_operations.clone();
            move || {
                let arena = bumpalo::Bump::new();
                let (mut current_data, _): (Vec<(String, i32)>, _) =
                    bincode::decode_from_slice(&partition_data, bincode::config::standard())
                        .map_err(|e| format!("Failed to deserialize parent data: {}", e))?;

                for op in &operations {
                    current_data = <(String, i32)>::apply_operation(op, current_data, &arena);
                }

                Ok::<Vec<(String, i32)>, String>(current_data)
            }
        })
        .await
        .map_err(|e| format!("Parent task panicked: {}", e))??;

        // 2. Create a shuffle writer
        // In a real system, the partitioner would also be serialized as part of the task
        let partitioner = Arc::new(crate::shuffle::HashPartitioner::new(
            self.num_reduce_partitions,
        ));
        let mut writer: barks_network_shuffle::optimizations::HashShuffleWriter<String, i32> =
            barks_network_shuffle::optimizations::HashShuffleWriter::new(
                self.shuffle_id,
                partition_index as u32,
                partitioner,
                block_manager,
                barks_network_shuffle::optimizations::ShuffleConfig::default(), // Use default shuffle config
            );

        // 3. Write all records to the shuffle writer
        for record in parent_result {
            writer
                .write(record)
                .await
                .map_err(|e| format!("Failed to write shuffle record: {}", e))?;
        }

        // 4. Close the writer to flush all buffers and get the MapStatus
        let map_status = writer
            .close()
            .await
            .map_err(|e| format!("Failed to close shuffle writer: {}", e))?;

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
#[async_trait::async_trait]
impl Task for ShuffleReduceTask<String, i32, i32, crate::shuffle::ReduceAggregator<i32>> {
    async fn execute(
        &self,
        _partition_index: usize,
        _block_manager: Arc<dyn ShuffleBlockManager>,
    ) -> Result<Vec<u8>, String> {
        // In a real implementation, this would:
        // 1. Deserialize the aggregator from aggregator_data
        // 2. Use map_output_locations to fetch shuffle blocks from remote executors
        // 3. Group all values by key across all fetched blocks
        // 4. Apply the aggregator to combine values for each key
        // 5. Return the final aggregated results

        // 1. Deserialize the aggregator. This determines the aggregation logic.
        let aggregator = if self.aggregator_data.is_empty() {
            // Default to addition aggregator for backward compatibility
            crate::shuffle::ReduceAggregator::new(|a, b| a + b)
        } else {
            // Try to deserialize the aggregator
            match crate::shuffle::SerializableAggregator::deserialize(&self.aggregator_data) {
                Ok(sa) => match sa {
                    crate::shuffle::SerializableAggregator::AddI32 => {
                        crate::shuffle::SerializableAggregator::create_add_i32_aggregator()
                    }
                    crate::shuffle::SerializableAggregator::SumI32 => {
                        // SumAggregator and ReduceAggregator<i32> with `+` are logically equivalent
                        crate::shuffle::SerializableAggregator::create_add_i32_aggregator()
                    }
                    // Other i32 aggregators would go here.
                    _ => return Err(format!("Unsupported aggregator for i32 value: {:?}", sa)),
                },
                Err(e) => return Err(e),
            }
        };

        // 2. Create an HTTP shuffle reader to fetch data from other executors.
        let reader = barks_network_shuffle::shuffle::HttpShuffleReader::<String, i32>::new();

        // 3. Fetch all shuffle blocks for this reduce partition from all map tasks.
        let all_blocks = reader
            .read_partition(
                self.shuffle_id,
                self.reduce_partition_id,
                &self.map_output_locations,
            )
            .await
            .map_err(|e| format!("Failed to read shuffle partition: {}", e))?;

        // 4. Aggregate the key-value pairs from all blocks.
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

        // 5. Convert the aggregated HashMap to a Vec for the final result.
        let result: Vec<(String, i32)> = combiners.into_iter().collect();

        // 6. Serialize the final result and return it.
        bincode::encode_to_vec(&result, bincode::config::standard())
            .map_err(|e| format!("Failed to serialize reduce result: {}", e))
    }
}

// Additional type implementations for more key-value pairs
// NOTE: This implementation is now broken because it depends on the old ShuffleMapTask structure.
// A full implementation would require (i32, String) to implement RddDataType fully
// and then ShuffleMapTask<(i32, String)> would be implemented similarly to ShuffleMapTask<(String, i32)>.
// For the purpose of this request, we will focus on the (String, i32) case and remove this broken implementation.
/*
#[typetag::serde(name = "ShuffleMapTaskI32String")]
#[async_trait::async_trait]
impl Task for ShuffleMapTask<i32, String> {
    async fn execute(
        &self,
        partition_index: usize,
        block_manager: Arc<dyn ShuffleBlockManager>,
    ) -> Result<Vec<u8>, String> {
        // 1. Run parent computation
        let parent_result = tokio::task::spawn_blocking({
            let partition_data = self.parent_partition_data.clone();
            let operations = self.parent_operations.clone();
            move || {
                // This block requires (i32, String) to be a full RddDataType
                // which is beyond the scope of the current request.
                // We assume it's implemented for demonstration.
                // ... computation logic ...
                Err("Computation for (i32, String) not fully implemented".to_string())
            }
        }).await.map_err(|e| format!("Task panicked: {}", e))??;

        // 2. Shuffle write
        let partitioner = Arc::new(crate::shuffle::HashPartitioner::new(
            self.num_reduce_partitions,
        ));
        let mut writer: barks_network_shuffle::optimizations::HashShuffleWriter<i32, String> =
            barks_network_shuffle::optimizations::HashShuffleWriter::new(
                self.shuffle_id,
                partition_index as u32,
                partitioner,
                block_manager,
                barks_network_shuffle::optimizations::ShuffleConfig::default(),
            );

        for record in parent_result {
            writer
                .write(record)
                .await
                .map_err(|e| format!("Failed to write shuffle record: {}", e))?;
        }

        let map_status = writer
            .close()
            .await
            .map_err(|e| format!("Failed to close shuffle writer: {}", e))?;

        bincode::encode_to_vec(&map_status, bincode::config::standard())
            .map_err(|e| format!("Failed to serialize MapStatus: {}", e))
    }
}*/

// ShuffleReduceTask for (i32, String) pairs
#[typetag::serde(name = "ShuffleReduceTaskI32String")]
#[async_trait::async_trait]
impl Task for ShuffleReduceTask<i32, String, String, crate::shuffle::ReduceAggregator<String>> {
    async fn execute(
        &self,
        _partition_index: usize,
        _block_manager: Arc<dyn ShuffleBlockManager>,
    ) -> Result<Vec<u8>, String> {
        // Deserialize the aggregator or use default string concatenation
        let aggregator = if self.aggregator_data.is_empty() {
            // Default to string concatenation aggregator
            crate::shuffle::ReduceAggregator::new(|a: String, b: String| format!("{},{}", a, b))
        } else {
            // Try to deserialize the aggregator
            match crate::shuffle::SerializableAggregator::deserialize(&self.aggregator_data) {
                Ok(crate::shuffle::SerializableAggregator::ConcatString) => {
                    crate::shuffle::SerializableAggregator::create_concat_string_aggregator()
                }
                _ => {
                    // Fallback to default
                    crate::shuffle::ReduceAggregator::new(|a: String, b: String| {
                        format!("{},{}", a, b)
                    })
                }
            }
        };

        let reader = barks_network_shuffle::shuffle::HttpShuffleReader::<i32, String>::new();

        let all_blocks = reader
            .read_partition(
                self.shuffle_id,
                self.reduce_partition_id,
                &self.map_output_locations,
            )
            .await
            .map_err(|e| format!("Failed to read shuffle partition: {}", e))?;

        let mut combiners = HashMap::<i32, String>::new();
        for block in all_blocks {
            for (key, value) in block {
                match combiners.get_mut(&key) {
                    Some(combiner) => {
                        *combiner = <crate::shuffle::ReduceAggregator<String> as crate::shuffle::aggregator::Aggregator<i32, String, String>>::merge_value(&aggregator, combiner.clone(), value);
                    }
                    None => {
                        let new_combiner = <crate::shuffle::ReduceAggregator<String> as crate::shuffle::aggregator::Aggregator<i32, String, String>>::create_combiner(&aggregator, value);
                        combiners.insert(key, new_combiner);
                    }
                }
            }
        }

        let result: Vec<(i32, String)> = combiners.into_iter().collect();
        bincode::encode_to_vec(&result, bincode::config::standard())
            .map_err(|e| format!("Failed to serialize reduce result: {}", e))
    }
}
