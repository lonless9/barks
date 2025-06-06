//! Distributed context for managing distributed RDD operations
//!
//! This module provides the DistributedContext which extends FlowContext
//! to support distributed execution using the Driver-Executor model.

use crate::distributed::driver::Driver;
use crate::distributed::executor::Executor;

use crate::distributed::types::*;
use crate::rdd::SimpleRdd;
use crate::traits::RddResult;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

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
        let driver_id = format!("{}-driver-{}", app_name, uuid::Uuid::new_v4());
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
                "Running RDD computation in distributed mode with {} executors",
                executor_count
            );

            // For now, this only supports a single-stage computation on a VecRdd.
            // A full implementation would require serializing the RDD and its closures.
            // We will collect the data on the driver and distribute it.
            // This simulates a "shuffle" read where the driver is the source.
            let data_to_distribute = rdd.collect()?;

            let stage_id = format!("stage-{}", uuid::Uuid::new_v4());

            let num_partitions =
                std::cmp::min(data_to_distribute.len(), self.config.default_parallelism);
            let chunks =
                barks_utils::vec_utils::partition_evenly(data_to_distribute, num_partitions);

            let mut result_futures = Vec::new();

            for (i, chunk) in chunks.into_iter().enumerate() {
                let task_id = format!("task-{}-{}", stage_id, i);

                // We assume a simple "Collect" operation for demonstration
                // where the executor just returns the data it received.
                let partition_data = bincode::encode_to_vec(&chunk, bincode::config::standard())
                    .map_err(|e| crate::traits::RddError::SerializationError(e.to_string()))?;

                let task_payload = crate::distributed::driver::TaskData {
                    partition_data,
                    operation: crate::distributed::driver::RddOperation::Collect,
                };

                let task_data = bincode::encode_to_vec(&task_payload, bincode::config::standard())
                    .map_err(|e| crate::traits::RddError::SerializationError(e.to_string()))?;

                let result_future = driver
                    .submit_task(task_id, stage_id.clone(), i, task_data, None)
                    .await;
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

// Add uuid dependency for generating unique IDs
mod uuid {
    use std::fmt;

    pub struct Uuid(u128);

    impl Uuid {
        pub fn new_v4() -> Self {
            use std::time::{SystemTime, UNIX_EPOCH};
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            Self(timestamp)
        }
    }

    impl fmt::Display for Uuid {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{:032x}", self.0)
        }
    }
}

// Add num_cpus functionality
mod num_cpus {
    pub fn get() -> usize {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
    }
}
