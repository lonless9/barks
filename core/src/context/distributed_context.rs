//! Distributed context for managing distributed RDD operations
//!
//! This module provides the DistributedContext which extends FlowContext
//! to support distributed execution using the Driver-Executor model.

use crate::distributed::driver::Driver;
use crate::distributed::executor::Executor;

use crate::distributed::types::*;
use crate::rdd::{DistributedRdd, SimpleRdd};
use crate::traits::RddResult;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Distributed context for managing RDD operations across a cluster
pub struct DistributedContext {
    /// Application name
    app_name: String,
    /// Driver instance (if running in driver mode)
    driver: Option<Arc<Driver>>,
    /// Executor instance (if running in executor mode)
    executor: Option<Arc<Mutex<Executor>>>,
    /// Execution mode
    mode: ExecutionMode,
    /// Unique shuffle ID generator
    next_shuffle_id: Arc<AtomicUsize>,
    /// Configuration
    config: DistributedConfig,
}

/// Execution mode for the context
#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionMode {
    /// Driver mode - coordinates executors and schedules tasks
    Driver,
    /// Executor mode - executes tasks assigned by driver
    Executor,
    /// Local mode - single machine execution (fallback)
    Local,
}

/// Configuration for distributed execution
#[derive(Debug, Clone)]
pub struct DistributedConfig {
    /// Driver address
    pub driver_addr: Option<SocketAddr>,
    /// Executor configuration
    pub executor_config: ExecutorConfig,
    /// Default parallelism
    pub default_parallelism: usize,
    /// Maximum result size in bytes
    pub max_result_size: usize,
    /// Executor heartbeat interval in seconds
    pub executor_heartbeat_interval_secs: u64,
    /// Seconds driver waits for heartbeat before marking executor as failed
    pub executor_liveness_timeout_secs: u64,
    /// Maximum number of times a task will be retried on failure
    pub task_max_retries: u32,
}

/// Executor configuration
#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    /// Number of CPU cores
    pub cores: u32,
    /// Memory in MB
    pub memory_mb: u64,
    /// Maximum concurrent tasks
    pub max_concurrent_tasks: usize,
    /// Executor attributes
    pub attributes: HashMap<String, String>,
}

impl Default for DistributedConfig {
    fn default() -> Self {
        Self {
            driver_addr: None,
            executor_config: ExecutorConfig::default(),
            default_parallelism: num_cpus::get(),
            max_result_size: 1024 * 1024 * 128,   // 128MB
            executor_heartbeat_interval_secs: 10, // 10 seconds
            executor_liveness_timeout_secs: 30,   // 30 seconds
            task_max_retries: 3,
        }
    }
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            cores: num_cpus::get() as u32,
            memory_mb: 2048, // 2GB default
            max_concurrent_tasks: num_cpus::get(),
            attributes: HashMap::new(),
        }
    }
}

impl DistributedContext {
    /// Create a new distributed context in driver mode
    pub fn new_driver(app_name: String, config: DistributedConfig) -> Self {
        let driver_id = format!("{}-driver-{}", app_name, Uuid::new_v4());
        let driver = Arc::new(Driver::new(driver_id, config.clone()));

        Self {
            app_name,
            driver: Some(driver),
            executor: None,
            mode: ExecutionMode::Driver,
            next_shuffle_id: Arc::new(AtomicUsize::new(0)),
            config,
        }
    }

    /// Create a new distributed context in executor mode
    pub fn new_executor(
        app_name: String,
        executor_id: String,
        host: String,
        port: u16,
        config: DistributedConfig,
    ) -> Self {
        let mut executor_info = ExecutorInfo::new(
            executor_id,
            host,
            port,
            config.executor_config.cores,
            config.executor_config.memory_mb,
        )
        .with_attributes(config.executor_config.attributes.clone());
        executor_info.max_concurrent_tasks = config.executor_config.max_concurrent_tasks as u32;

        let executor = Arc::new(Mutex::new(Executor::new(
            executor_info,
            config.executor_config.max_concurrent_tasks,
            config.clone(),
        )));

        Self {
            app_name,
            driver: None,
            executor: Some(executor),
            mode: ExecutionMode::Executor,
            next_shuffle_id: Arc::new(AtomicUsize::new(0)),
            config,
        }
    }

    /// Create a new distributed context in local mode (fallback)
    pub fn new_local(app_name: String) -> Self {
        Self {
            app_name,
            driver: None,
            executor: None,
            mode: ExecutionMode::Local,
            next_shuffle_id: Arc::new(AtomicUsize::new(0)),
            config: DistributedConfig::default(),
        }
    }

    /// Get the application name
    pub fn app_name(&self) -> &str {
        &self.app_name
    }

    /// Get the execution mode
    pub fn mode(&self) -> &ExecutionMode {
        &self.mode
    }

    fn new_shuffle_id(&self) -> usize {
        self.next_shuffle_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Convenience method to run distributed computation without requiring Arc wrapping
    pub async fn run_distributed_simple<T>(&self, rdd: DistributedRdd<T>) -> RddResult<Vec<T>>
    where
        T: crate::operations::RddDataType + bincode::Encode + bincode::Decode<()>,
    {
        // Create an Arc wrapper and delegate to the main run_distributed method
        let arc_self = Arc::new(DistributedContext {
            app_name: self.app_name.clone(),
            driver: self.driver.clone(),
            executor: self.executor.clone(),
            mode: self.mode.clone(),
            next_shuffle_id: self.next_shuffle_id.clone(),
            config: self.config.clone(),
        });
        arc_self.run_distributed(rdd).await
    }

    /// Start the context (driver or executor service)
    pub async fn start(&self, addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
        match &self.mode {
            ExecutionMode::Driver => {
                if let Some(driver) = &self.driver {
                    info!("Starting driver on {}", addr);
                    driver.start(addr).await?;
                } else {
                    return Err("No driver instance available".into());
                }
            }
            ExecutionMode::Executor => {
                if let Some(executor) = &self.executor {
                    info!("Starting executor on {}", addr);
                    let executor = executor.lock().await;
                    executor.start(addr).await?;
                } else {
                    return Err("No executor instance available".into());
                }
            }
            ExecutionMode::Local => {
                warn!("Local mode does not require starting a service");
            }
        }
        Ok(())
    }

    /// Register executor with driver (for executor mode)
    pub async fn register_with_driver(
        &self,
        driver_addr: String,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let ExecutionMode::Executor = self.mode {
            if let Some(executor) = &self.executor {
                let executor = executor.lock().await;
                executor.register_with_driver(driver_addr.clone()).await?;
                executor.start_heartbeat().await?;
                info!("Executor registered with driver and heartbeat started");
            } else {
                return Err("No executor instance available".into());
            }
        } else {
            return Err("Can only register with driver in executor mode".into());
        }
        Ok(())
    }

    /// Create an RDD from a vector of data with specified partitions
    pub fn parallelize_with_partitions<T>(
        &self,
        data: Vec<T>,
        num_partitions: usize,
    ) -> SimpleRdd<T>
    where
        T: Send + Sync + Clone + Serialize + for<'de> Deserialize<'de> + Debug + 'static,
    {
        // For now, create a simple RDD regardless of mode
        // In a full implementation, this would create distributed RDDs in cluster mode
        SimpleRdd::from_vec_with_partitions(data, num_partitions)
    }

    /// Create an RDD from a vector of data
    pub fn parallelize<T>(&self, data: Vec<T>) -> SimpleRdd<T>
    where
        T: Send + Sync + Clone + Serialize + for<'de> Deserialize<'de> + Debug + 'static,
    {
        self.parallelize_with_partitions(data, self.config.default_parallelism)
    }

    /// Create a distributed RDD from a vector with specified partitions
    pub fn parallelize_distributed<T: crate::operations::RddDataType>(
        &self,
        data: Vec<T>,
        num_partitions: usize,
    ) -> DistributedRdd<T> {
        DistributedRdd::from_vec_with_partitions(data, num_partitions)
    }

    /// Run an RDD computation and collect results
    pub async fn run<T>(&self, rdd: SimpleRdd<T>) -> RddResult<Vec<T>>
    where
        T: Send
            + Sync
            + Clone
            + Serialize
            + for<'de> Deserialize<'de>
            + Debug
            + 'static
            + bincode::Encode
            + bincode::Decode<()>,
    {
        match &self.mode {
            ExecutionMode::Driver => {
                if let Some(driver) = &self.driver {
                    let executor_count = driver.executor_count().await;
                    // If we have executors and the RDD has non-serializable transformations, it's an error.
                    if executor_count > 0 && rdd.is_transformed() {
                        error!("Attempted to run a transformed SimpleRdd in a distributed context with active executors.");
                        return Err(crate::traits::RddError::ContextError(
                            "SimpleRdd with non-serializable closures cannot be executed in distributed mode. \
                             Use DistributedRdd with context.run_distributed() instead.".to_string(),
                        ));
                    } else {
                        // Otherwise, run locally. This covers:
                        // 1. Base SimpleRdd (Vec) which is always local to the driver.
                        // 2. No executors are available, so we fall back to local execution.
                        if executor_count == 0 {
                            warn!("No executors available. Running SimpleRdd job locally on the driver.");
                        }
                        self.run_local(rdd).await
                    }
                } else {
                    Err(crate::traits::RddError::ContextError(
                        "Driver mode selected, but no driver instance available.".to_string(),
                    ))
                }
            }
            ExecutionMode::Local => {
                // Fallback to local execution
                self.run_local(rdd).await
            }
            ExecutionMode::Executor => {
                // Executors don't run RDDs directly, they execute tasks
                Err(crate::traits::RddError::ContextError(
                    "Executors cannot run RDDs directly".to_string(),
                ))
            }
        }
    }

    /// The main entry point for running a job on the cluster.
    /// This method analyzes the RDD dependency graph and executes it in stages.
    pub async fn run_distributed<T>(self: Arc<Self>, rdd: DistributedRdd<T>) -> RddResult<Vec<T>>
    where
        T: crate::operations::RddDataType + bincode::Encode + bincode::Decode<()>,
    {
        match &self.mode {
            ExecutionMode::Driver => {
                if self.driver.is_none() {
                    return Err(crate::traits::RddError::ContextError(
                        "Driver not initialized.".to_string(),
                    ));
                }
                if self.driver.as_ref().unwrap().executor_count().await == 0 {
                    warn!("No executors available. Falling back to local computation.");
                    return Ok(rdd.collect()?);
                }

                // For now, we handle a simple case: the final RDD is either a standard RDD or a ShuffledRdd.
                // A full DAG scheduler would handle arbitrary stage graphs.
                if let Some(shuffle_dep) = rdd.shuffle_dependency() {
                    // It's a shuffle operation, run as Map-Reduce stages.
                    info!("Detected ShuffleDependency. Running as a two-stage job.");
                    self.run_shuffle_job(shuffle_dep).await
                } else {
                    // No shuffle, run as a single ResultStage.
                    info!("No ShuffleDependency detected. Running as a single stage job.");
                    self.run_result_stage(rdd).await
                }
            }
            _ => Err(crate::traits::RddError::ContextError(
                "run_distributed can only be called in Driver mode.".to_string(),
            )),
        }
    }

    /// Executes a single-stage job that doesn't involve a shuffle.
    async fn run_result_stage<T>(&self, rdd: DistributedRdd<T>) -> RddResult<Vec<T>>
    where
        T: crate::operations::RddDataType,
    {
        let (base_data, num_partitions, operations) = rdd.analyze_lineage();
        info!(
            "RDD lineage analyzed: {} operations found.",
            operations.len()
        );

        let data_slice = base_data.as_ref();
        let num_items = data_slice.len();
        let effective_num_partitions = std::cmp::min(num_partitions, num_items.max(1));
        let partition_size = (num_items + effective_num_partitions - 1) / effective_num_partitions;

        let mut result_futures = Vec::new();

        for i in 0..effective_num_partitions {
            let start = i * partition_size;
            let end = std::cmp::min(start + partition_size, num_items);
            if start >= end {
                continue;
            }

            let chunk = data_slice[start..end].to_vec();
            let serialized_partition_data =
                bincode::encode_to_vec(&chunk, bincode::config::standard())
                    .map_err(|e| crate::traits::RddError::SerializationError(e.to_string()))?;

            let task = T::create_chained_task(serialized_partition_data, operations.clone())?;
            let task_id = format!("result-task-{}", i);

            let future = self
                .driver
                .as_ref()
                .unwrap()
                .submit_task(task_id, "result-stage".to_string(), i, task, None)
                .await
                .map_err(|e| crate::traits::RddError::ContextError(e.to_string()))?;
            result_futures.push(future);
        }

        let mut collected_results = Vec::new();
        for future in result_futures {
            match future.await {
                Ok(crate::distributed::driver::TaskResult::Success(bytes)) => {
                    let (partition_result, _): (Vec<T>, _) =
                        bincode::decode_from_slice(&bytes, bincode::config::standard()).map_err(
                            |e| crate::traits::RddError::SerializationError(e.to_string()),
                        )?;
                    collected_results.extend(partition_result);
                }
                Ok(crate::distributed::driver::TaskResult::Failure(err)) => {
                    return Err(crate::traits::RddError::ComputationError(err))
                }
                Err(e) => {
                    return Err(crate::traits::RddError::ContextError(format!(
                        "Driver communication failed: {}",
                        e
                    )))
                }
            }
        }
        Ok(collected_results)
    }

    /// Executes a two-stage shuffle job.
    async fn run_shuffle_job<T: 'static>(
        &self,
        _shuffle_dep: Arc<dyn std::any::Any + Send + Sync>,
    ) -> RddResult<Vec<T>>
    where
        T: crate::operations::RddDataType + bincode::Encode + bincode::Decode<()>,
    {
        unimplemented!("Shuffle job execution is not yet implemented.");
        // The logic will be:
        // 1. Downcast shuffle_dep to its concrete type, e.g., ShuffleDependency<K, V, C>.
        // 2. Get the parent RDD from the dependency.
        // 3. Run the parent RDD as a ShuffleMapStage, creating ShuffleMapTasks.
        // 4. Collect MapStatus results.
        // 5. Build MapOutputLocations (map_id -> executor_address).
        // 6. Create ShuffleReduceTasks for the ReduceStage, passing the locations and aggregator.
        // 7. Run the reduce tasks and collect final results.
    }

    /// Run RDD computation in local mode
    async fn run_local<T>(&self, rdd: SimpleRdd<T>) -> RddResult<Vec<T>>
    where
        T: Send + Sync + Clone + Serialize + for<'de> Deserialize<'de> + Debug + 'static,
    {
        debug!("Running RDD computation in local mode");
        rdd.collect()
    }

    /// Run distributed RDD computation in distributed mode
    async fn run_distributed_rdd<T>(&self, rdd: DistributedRdd<T>) -> RddResult<Vec<T>>
    where
        T: crate::operations::RddDataType + bincode::Encode + bincode::Decode<()>,
    {
        if let Some(driver) = &self.driver {
            // Check if we have any executors
            let executor_count = driver.executor_count().await;
            if executor_count == 0 {
                warn!("No executors available, cannot run distributed job");
                return Err(crate::traits::RddError::ContextError(
                    "No executors available. Cannot run distributed job.".to_string(),
                ));
            }
            info!(
                "Running RDD computation in distributed mode with {} executor(s)",
                executor_count
            );

            // 1. Analyze the RDD lineage to get base data and the chain of operations.
            let (base_data, num_partitions, operations) = rdd.analyze_lineage();
            info!(
                "RDD lineage analyzed: {} operations found.",
                operations.len()
            );

            // Get available executor IDs for locality-aware scheduling (round-robin for now)
            let executor_ids = driver.get_executor_ids().await;
            if executor_ids.is_empty() {
                warn!("No executors available to assign preferred location.");
            }

            // 2. Partition the base data for distribution.
            // Iterate through partitions and clone only the necessary slice for each task.
            let stage_id = format!("stage-{}", uuid::Uuid::new_v4());
            let data_slice = base_data.as_ref();
            let num_items = data_slice.len();
            let effective_num_partitions = std::cmp::min(num_partitions, num_items.max(1));
            let partition_size =
                (num_items + effective_num_partitions - 1) / effective_num_partitions;

            let mut result_futures = Vec::new();

            // 3. For each partition, create a task with the data and the *full* operation chain.
            for i in 0..effective_num_partitions {
                let start = i * partition_size;
                let end = std::cmp::min(start + partition_size, num_items);
                if start >= end {
                    continue;
                }

                let chunk = data_slice[start..end].to_vec();
                let task_id = format!("task-{}-{}", stage_id, i);

                let serialized_partition_data =
                    bincode::encode_to_vec(&chunk, bincode::config::standard())
                        .map_err(|e| crate::traits::RddError::SerializationError(e.to_string()))?;

                // Create the new generic chained task. It contains the data for one partition
                // and the entire sequence of operations to be applied to it.
                let task: Box<dyn crate::distributed::task::Task> =
                    Self::create_task_for_type::<T>(serialized_partition_data, operations.clone())?;

                // Assign a preferred executor in a round-robin fashion to test locality
                let preferred_executor = if !executor_ids.is_empty() {
                    executor_ids.get(i % executor_ids.len()).cloned()
                } else {
                    None
                };

                let result_future = driver
                    .submit_task(task_id, stage_id.clone(), i, task, preferred_executor)
                    .await
                    .map_err(|e| crate::traits::RddError::SerializationError(e.to_string()))?;
                result_futures.push(result_future);
            }

            info!("Submitted {} tasks for execution", result_futures.len());

            // 4. Wait for all results and aggregate them.
            let mut collected_results = Vec::new();
            for future in result_futures {
                match future.await {
                    Ok(crate::distributed::driver::TaskResult::Success(bytes)) => {
                        let (partition_result, _): (Vec<T>, _) =
                            bincode::decode_from_slice(&bytes, bincode::config::standard())
                                .map_err(|e| {
                                    crate::traits::RddError::SerializationError(e.to_string())
                                })?;
                        collected_results.extend(partition_result);
                    }
                    Ok(crate::distributed::driver::TaskResult::Failure(err_msg)) => {
                        return Err(crate::traits::RddError::ComputationError(err_msg));
                    }
                    Err(e) => {
                        return Err(crate::traits::RddError::ContextError(format!(
                            "Driver communication failed: {}",
                            e
                        )));
                    }
                }
            }

            Ok(collected_results)
        } else {
            Err(crate::traits::RddError::ContextError(
                "No driver instance available".to_string(),
            ))
        }
    }

    /// Get driver statistics (for driver mode)
    pub async fn get_driver_stats(&self) -> Option<DriverStats> {
        if let Some(driver) = &self.driver {
            Some(DriverStats {
                executor_count: driver.executor_count().await,
                pending_task_count: driver.pending_task_count().await,
            })
        } else {
            None
        }
    }

    /// Create a task for the given type T using a trait-based approach
    /// This eliminates hardcoded type checks by leveraging the RddDataType trait
    fn create_task_for_type<T>(
        serialized_partition_data: Vec<u8>,
        operations: Vec<T::SerializableOperation>,
    ) -> crate::traits::RddResult<Box<dyn crate::distributed::task::Task>>
    where
        T: crate::operations::RddDataType + 'static,
    {
        // Use a trait-based approach to create tasks
        T::create_chained_task(serialized_partition_data, operations)
    }
}

/// Driver statistics
#[derive(Debug, Clone)]
pub struct DriverStats {
    pub executor_count: usize,
    pub pending_task_count: usize,
}
