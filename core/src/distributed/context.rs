//! Distributed context for managing distributed RDD operations
//!
//! This module provides the DistributedContext which extends FlowContext
//! to support distributed execution using the Driver-Executor model.

use crate::distributed::driver::Driver;
use crate::distributed::executor::Executor;

use crate::distributed::types::*;
use crate::rdd::{DistributedI32Rdd, SimpleRdd};
use crate::traits::RddResult;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};
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
    /// Heartbeat interval in seconds
    pub heartbeat_interval_secs: u64,
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
            max_result_size: 1024 * 1024 * 128, // 128MB
            heartbeat_interval_secs: 10,
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
        let driver = Arc::new(Driver::new(driver_id));

        Self {
            app_name,
            driver: Some(driver),
            executor: None,
            mode: ExecutionMode::Driver,
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
        let executor_info = ExecutorInfo::new(
            executor_id,
            host,
            port,
            config.executor_config.cores,
            config.executor_config.memory_mb,
        )
        .with_attributes(config.executor_config.attributes.clone());

        let executor = Arc::new(Mutex::new(Executor::new(
            executor_info,
            config.executor_config.max_concurrent_tasks,
        )));

        Self {
            app_name,
            driver: None,
            executor: Some(executor),
            mode: ExecutionMode::Executor,
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
                let mut executor = executor.lock().await;
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

    /// Create a distributed RDD from a vector of i32 data with specified partitions
    pub fn parallelize_i32_with_partitions(
        &self,
        data: Vec<i32>,
        num_partitions: usize,
    ) -> DistributedI32Rdd {
        DistributedI32Rdd::from_vec_with_partitions(data, num_partitions)
    }

    /// Create a distributed RDD from a vector of i32 data
    pub fn parallelize_i32(&self, data: Vec<i32>) -> DistributedI32Rdd {
        self.parallelize_i32_with_partitions(data, self.config.default_parallelism)
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
            ExecutionMode::Driver => self.run_distributed(rdd).await,
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

    /// Run a distributed i32 RDD computation and collect results
    pub async fn run_i32(&self, rdd: DistributedI32Rdd) -> RddResult<Vec<i32>> {
        match &self.mode {
            ExecutionMode::Driver => self.run_i32_distributed(rdd).await,
            ExecutionMode::Local => {
                // Fallback to local execution
                rdd.collect()
            }
            ExecutionMode::Executor => {
                // Executors don't run RDDs directly, they execute tasks
                Err(crate::traits::RddError::ContextError(
                    "Executors cannot run RDDs directly".to_string(),
                ))
            }
        }
    }

    /// Run RDD computation in distributed mode
    async fn run_distributed<T>(&self, rdd: SimpleRdd<T>) -> RddResult<Vec<T>>
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
        if let Some(driver) = &self.driver {
            // Check if we have any executors
            let executor_count = driver.executor_count().await;
            if executor_count == 0 {
                warn!("No executors available, falling back to local execution");
                return self.run_local(rdd).await;
            }
            info!(
                "Running RDD computation in distributed mode with {} executor(s)",
                executor_count
            );

            // This is a critical simplification. In a real system, the RDD's computation
            // graph (lineage) would be serialized and sent to executors. Here, we only
            // support distributing the data from a base RDD created with `parallelize`.
            // Executing transformed RDDs (map, filter, etc.) is not supported in distributed
            // mode yet because it requires serializing closures, which is a complex problem.
            // The original implementation (`rdd.collect()?`) was incorrect as it centralized
            // all data on the driver, defeating the purpose of distributed computing.

            // A full implementation would require serializing the RDD and its closures.
            // We will collect the data on the driver and distribute it.
            // This simulates a "shuffle" read where the driver is the source.

            let stage_id = format!("stage-{}", Uuid::new_v4());

            let (base_data, num_partitions) = if let SimpleRdd::Vec {
                data,
                num_partitions,
            } = rdd
            {
                (data, num_partitions)
            } else {
                // This is a key limitation of the current implementation.
                // We return an error to make it clear that distributed execution of
                // complex RDDs (with map/filter) is not yet supported.
                return Err(crate::traits::RddError::ContextError(
                    "Distributed execution of transformed RDDs is not yet supported. Only base RDDs from `parallelize` can be run.".to_string(),
                ));
            };

            let chunks = barks_utils::vec_utils::partition_evenly(
                base_data.as_ref().clone(),
                num_partitions,
            );

            let mut result_futures = Vec::new();

            for (i, chunk) in chunks.into_iter().enumerate() {
                let task_id = format!("task-{}-{}", stage_id, i);

                // Create a task that contains the data for the partition and the operation to perform.
                // For a simple `run()` (collect), the operation is just to return the data.
                // We serialize the partition data here using bincode.
                let serialized_partition_data =
                    bincode::encode_to_vec(&chunk, bincode::config::standard())
                        .map_err(|e| crate::traits::RddError::SerializationError(e.to_string()))?;

                let task: Box<dyn crate::distributed::task::Task> =
                    Box::new(crate::distributed::task::DataMapTask {
                        partition_data: serialized_partition_data,
                        operation_type: "collect".to_string(), // For a simple .run(), we just collect
                    });

                let result_future = driver
                    .submit_task(task_id, stage_id.clone(), i, task, None)
                    .await
                    .map_err(|e| crate::traits::RddError::SerializationError(e.to_string()))?;
                result_futures.push(result_future);
            }

            info!("Submitted {} tasks for execution", result_futures.len());

            // Wait for all results
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
                        // RecvError
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

    /// Run RDD computation in local mode
    async fn run_local<T>(&self, rdd: SimpleRdd<T>) -> RddResult<Vec<T>>
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
        debug!("Running RDD computation in local mode");

        // Use the existing collect implementation
        rdd.collect()
    }

    /// Run i32 RDD computation in distributed mode
    async fn run_i32_distributed(&self, rdd: DistributedI32Rdd) -> RddResult<Vec<i32>> {
        if let Some(driver) = &self.driver {
            // Check if we have any executors
            let executor_count = driver.executor_count().await;
            if executor_count == 0 {
                warn!("No executors available, falling back to local execution");
                return rdd.collect();
            }
            info!(
                "Running i32 RDD computation in distributed mode with {} executor(s)",
                executor_count
            );

            // 1. Analyze the RDD lineage to get base data and the chain of operations.
            let (base_data, num_partitions, operations) = rdd.analyze_lineage();
            info!(
                "RDD lineage analyzed: {} operations found.",
                operations.len()
            );

            // 2. Partition the base data for distribution.
            let stage_id = format!("stage-{}", uuid::Uuid::new_v4());
            let chunks = barks_utils::vec_utils::partition_evenly(
                base_data.as_ref().clone(),
                num_partitions,
            );

            let mut result_futures = Vec::new();

            // 3. For each partition, create a task with the data and the *full* operation chain.
            for (i, chunk) in chunks.into_iter().enumerate() {
                let task_id = format!("task-{}-{}", stage_id, i);

                let serialized_partition_data =
                    bincode::encode_to_vec(&chunk, bincode::config::standard())
                        .map_err(|e| crate::traits::RddError::SerializationError(e.to_string()))?;

                // Create the new chained task. It contains the data for one partition
                // and the entire sequence of operations to be applied to it.
                let task: Box<dyn crate::distributed::task::Task> =
                    Box::new(crate::distributed::task::ChainedI32Task {
                        partition_data: serialized_partition_data,
                        // Clone the operation chain for each task. This is efficient
                        // because the operations themselves are small structs.
                        operations: operations.clone(),
                    });

                let result_future = driver
                    .submit_task(task_id, stage_id.clone(), i, task, None)
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
                        let (partition_result, _): (Vec<i32>, _) =
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
}

/// Driver statistics
#[derive(Debug, Clone)]
pub struct DriverStats {
    pub executor_count: usize,
    pub pending_task_count: usize,
}
