//! Distributed context for managing distributed RDD operations
//!
//! This module provides the DistributedContext which extends FlowContext
//! to support distributed execution using the Driver-Executor model.

use crate::distributed::driver::Driver;
use crate::distributed::executor::Executor;
use crate::distributed::types::*;
use crate::rdd::DistributedRdd;
use crate::traits::RddResult;

use std::collections::HashMap;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
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
            port + 1000, // Convention: shuffle port is executor port + 1000
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

    #[allow(dead_code)]
    fn new_shuffle_id(&self) -> usize {
        self.next_shuffle_id.fetch_add(1, Ordering::SeqCst)
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
    ) -> DistributedRdd<T>
    where
        T: crate::operations::RddDataType,
    {
        // Create a distributed RDD that can be executed locally or in cluster mode
        DistributedRdd::from_vec_with_partitions(data, num_partitions)
    }

    /// Create an RDD from a vector of data
    pub fn parallelize<T>(&self, data: Vec<T>) -> DistributedRdd<T>
    where
        T: crate::operations::RddDataType,
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
    pub async fn run<T>(
        self: Arc<Self>,
        rdd: Arc<dyn crate::traits::RddBase<Item = T>>,
    ) -> RddResult<Vec<T>>
    where
        T: crate::operations::RddDataType + bincode::Encode + bincode::Decode<()>,
    {
        match &self.mode {
            ExecutionMode::Driver => {
                if let Some(driver) = &self.driver {
                    let executor_count = driver.executor_count().await;
                    if executor_count > 0 {
                        // Run in distributed mode using the driver
                        self.run_distributed(rdd).await
                    } else {
                        // No executors available, fall back to local execution
                        warn!("No executors available. Running RDD job locally on the driver.");
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
    pub async fn run_distributed<T>(
        self: Arc<Self>,
        rdd: Arc<dyn crate::traits::RddBase<Item = T>>,
    ) -> RddResult<Vec<T>>
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
                    warn!("No executors available. Running job locally on the driver.");
                    return self.run_local(rdd).await;
                }

                // Simplified DAG logic: Check dependencies of the final RDD.
                let deps = rdd.dependencies();
                if deps
                    .iter()
                    .any(|d| matches!(d, crate::traits::Dependency::Shuffle(_)))
                {
                    // It's a shuffle operation, run as Map-Reduce stages.
                    info!("Detected ShuffleDependency. Running shuffle job.");
                    self.run_shuffle_job(rdd).await
                } else {
                    // No shuffle, run as a single ResultStage.
                    // This part now needs to handle the Arc<dyn RddBase>
                    // We can try to downcast it back to DistributedRdd for the existing logic to work.
                    // This is a temporary hack before a full DAGScheduler is in place.
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
    async fn run_result_stage<T>(
        &self,
        rdd: Arc<dyn crate::traits::RddBase<Item = T>>,
    ) -> RddResult<Vec<T>>
    where
        T: crate::operations::RddDataType + bincode::Encode + bincode::Decode<()>,
    {
        // HACK: Downcast to DistributedRdd to use its analyze_lineage method.
        // A proper DAG scheduler would build the task chain differently.
        let distributed_rdd = rdd
            .as_any()
            .downcast_ref::<DistributedRdd<T>>()
            .ok_or_else(|| {
                crate::traits::RddError::ContextError(
                    "Result stage can only be run on a DistributedRdd (for now)".to_string(),
                )
            })?
            .clone();
        let (base_data, num_partitions, operations) = distributed_rdd.analyze_lineage();
        info!(
            "RDD lineage analyzed: {} operations found.",
            operations.len()
        );

        let data_slice = base_data.as_ref();
        let num_items = data_slice.len();
        let effective_num_partitions = std::cmp::min(num_partitions, num_items.max(1));
        let partition_size = num_items.div_ceil(effective_num_partitions);

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
            match future
                .await
                .map_err(|e| crate::traits::RddError::ContextError(e.to_string()))?
            {
                (crate::distributed::driver::TaskResult::Success(bytes), _exec_id) => {
                    let (partition_result, _): (Vec<T>, _) =
                        bincode::decode_from_slice(&bytes, bincode::config::standard()).map_err(
                            |e| crate::traits::RddError::SerializationError(e.to_string()),
                        )?;
                    collected_results.extend(partition_result);
                }
                (crate::distributed::driver::TaskResult::Failure(err), exec_id) => {
                    return Err(crate::traits::RddError::ComputationError(format!(
                        "Task failed on {}: {}",
                        exec_id, err
                    )));
                }
            }
        }
        Ok(collected_results)
    }

    /// Executes a two-stage shuffle job.
    async fn run_shuffle_job<T>(
        &self,
        rdd: Arc<dyn crate::traits::RddBase<Item = T>>,
    ) -> RddResult<Vec<T>>
    where
        T: crate::operations::RddDataType + bincode::Encode + bincode::Decode<()> + 'static,
    {
        // This is a simplified DAG scheduler. It uses downcasting to identify the type of shuffle.
        // A full implementation would analyze the RDD dependency graph more abstractly.

        // --- POC for a specific shuffle RDD type ---
        // Handle ShuffledRdd for (String, i32) -> (String, i32), typical of reduceByKey.
        type K = String;
        type V = i32;
        type C = i32;

        if let Some(shuffled_rdd) = rdd
            .as_any()
            .downcast_ref::<crate::rdd::ShuffledRdd<K, V, C>>()
        {
            info!("Executing shuffle for ShuffledRdd<String, i32, i32>");
            let parent_rdd = shuffled_rdd.parent.clone();
            let _aggregator = shuffled_rdd.aggregator.clone();
            let partitioner = shuffled_rdd.partitioner.clone();
            let shuffle_id = self.new_shuffle_id() as u32;
            let num_reduce_partitions = partitioner.num_partitions();

            // --- MAP STAGE ---
            info!("Starting Map Stage for shuffle {}", shuffle_id);
            let map_output_locations = self
                .execute_map_stage(parent_rdd, shuffle_id, num_reduce_partitions)
                .await?;

            // --- REDUCE STAGE ---
            info!("Starting Reduce Stage for shuffle {}", shuffle_id);
            // This is a placeholder for serializing a real aggregator.
            // In a real system, the aggregator itself would be serialized.
            let aggregator_data = crate::shuffle::SerializableAggregator::AddI32
                .serialize()
                .unwrap();

            let reduce_results = self
                .execute_reduce_stage::<K, V, C>(
                    shuffle_id,
                    map_output_locations,
                    aggregator_data,
                    partitioner,
                )
                .await?;

            // The result is Vec<Vec<(K, C)>>. Flatten it and cast to Vec<T>.
            // This cast is safe because we checked the RDD type at the beginning.
            let final_results: Vec<T> = reduce_results
                .into_iter()
                .flatten()
                .map(|item| {
                    *(Box::new(item) as Box<dyn std::any::Any>)
                        .downcast::<T>()
                        .unwrap()
                })
                .collect();

            return Ok(final_results);
        }

        warn!("No distributed shuffle implementation found for this RDD type. Falling back to local execution.");
        self.run_local(rdd).await
    }

    /// Helper to execute the map stage of a shuffle.
    async fn execute_map_stage<K, V>(
        &self,
        parent_rdd: Arc<dyn crate::traits::RddBase<Item = (K, V)>>,
        shuffle_id: u32,
        num_reduce_partitions: u32,
    ) -> RddResult<Vec<(String, Vec<(u32, barks_network_shuffle::traits::MapStatus)>)>>
    where
        K: crate::traits::Data + std::hash::Hash + Ord,
        V: crate::traits::Data,
        (K, V): crate::operations::RddDataType,
    {
        // Downcast to get lineage information. This is a simplification.
        let distributed_rdd = parent_rdd
            .as_any()
            .downcast_ref::<DistributedRdd<(K, V)>>()
            .ok_or_else(|| {
                crate::traits::RddError::ContextError(
                    "Shuffle parent must be a DistributedRdd (for now)".to_string(),
                )
            })?
            .clone();

        let (base_data, num_partitions, operations) = distributed_rdd.analyze_lineage();

        // Create and submit ShuffleMapTasks for each partition
        let mut map_task_futures = Vec::new();

        for partition_index in 0..num_partitions {
            // Calculate partition data slice
            let partition_size = base_data.len() / num_partitions;
            let start = partition_index * partition_size;
            let end = if partition_index == num_partitions - 1 {
                base_data.len()
            } else {
                start + partition_size
            };

            let partition_data = if start < base_data.len() {
                base_data[start..end.min(base_data.len())].to_vec()
            } else {
                Vec::new()
            };

            // Serialize partition data
            let serialized_partition_data =
                bincode::encode_to_vec(&partition_data, bincode::config::standard())
                    .map_err(|e| crate::traits::RddError::SerializationError(e.to_string()))?;

            // Create ShuffleMapTask - for now we only support (String, i32) type
            // In a full implementation, this would be more generic
            if std::any::TypeId::of::<(K, V)>() == std::any::TypeId::of::<(String, i32)>() {
                // Cast the operations to the concrete type
                let concrete_operations: Vec<
                    crate::operations::SerializableStringI32TupleOperation,
                > = unsafe { std::mem::transmute(operations.clone()) };

                let task = crate::distributed::task::ShuffleMapTask::<(String, i32)>::new(
                    serialized_partition_data,
                    concrete_operations,
                    shuffle_id,
                    num_reduce_partitions,
                );

                let task_id = format!("shuffle-map-{}-{}", shuffle_id, partition_index);
                let stage_id = format!("shuffle-map-stage-{}", shuffle_id);

                // Submit task to driver
                let future = self
                    .driver
                    .as_ref()
                    .unwrap()
                    .submit_task(task_id, stage_id, partition_index, Box::new(task), None)
                    .await
                    .map_err(|e| crate::traits::RddError::ContextError(e.to_string()))?;

                map_task_futures.push((partition_index, future));
            } else {
                return Err(crate::traits::RddError::ContextError(
                    "Unsupported type for shuffle map task. Only (String, i32) is currently supported.".to_string()
                ));
            }
        }

        // Collect results from all map tasks
        let mut map_output_locations_map: std::collections::HashMap<
            String,
            Vec<(u32, barks_network_shuffle::traits::MapStatus)>,
        > = std::collections::HashMap::new();
        for (partition_index, future) in map_task_futures {
            match future
                .await
                .map_err(|e| crate::traits::RddError::ContextError(e.to_string()))?
            {
                (crate::distributed::driver::TaskResult::Success(bytes), executor_id) => {
                    let (map_status, _): (barks_network_shuffle::traits::MapStatus, _) =
                        bincode::decode_from_slice(&bytes, bincode::config::standard()).map_err(
                            |e| crate::traits::RddError::SerializationError(e.to_string()),
                        )?;

                    map_output_locations_map
                        .entry(executor_id)
                        .or_default()
                        .push((partition_index as u32, map_status));
                }
                (crate::distributed::driver::TaskResult::Failure(err), executor_id) => {
                    return Err(crate::traits::RddError::ComputationError(format!(
                        "Map task failed on executor {}: {}",
                        executor_id, err
                    )));
                }
            }
        }

        let map_output_locations: Vec<(
            String,
            Vec<(u32, barks_network_shuffle::traits::MapStatus)>,
        )> = map_output_locations_map.into_iter().collect();

        info!(
            "Map stage completed successfully with {} map outputs",
            map_output_locations.len()
        );
        Ok(map_output_locations)
    }

    /// Helper to execute the reduce stage of a shuffle.
    async fn execute_reduce_stage<K, V, C>(
        &self,
        shuffle_id: u32,
        map_output_locations: Vec<(String, Vec<(u32, barks_network_shuffle::traits::MapStatus)>)>,
        aggregator_data: Vec<u8>,
        partitioner: Arc<dyn crate::shuffle::Partitioner<K>>,
    ) -> RddResult<Vec<Vec<(K, C)>>>
    where
        K: crate::traits::Data + std::hash::Hash,
        V: crate::traits::Data,
        C: crate::traits::Data,
        (K, C): crate::operations::RddDataType,
        crate::rdd::ShuffledRdd<K, V, C>: crate::traits::RddBase<Item = (K, C)>,
    {
        let driver = self.driver.as_ref().unwrap();
        let num_reduce_partitions = partitioner.num_partitions() as usize;
        let mut reduce_task_futures = Vec::new();

        // First, resolve all executor IDs to shuffle addresses to avoid repeated lookups.
        let mut executor_shuffle_addrs = std::collections::HashMap::new();
        for (exec_id, _) in &map_output_locations {
            if !executor_shuffle_addrs.contains_key(exec_id) {
                if let Some(info) = driver.get_executor_info(exec_id).await {
                    let addr = format!("{}:{}", info.host, info.shuffle_port);
                    executor_shuffle_addrs.insert(exec_id.clone(), addr);
                } else {
                    return Err(crate::traits::RddError::ContextError(format!(
                        "Could not find info for executor {}",
                        exec_id
                    )));
                }
            }
        }

        // Build the complete list of map output locations for the reduce tasks.
        // This is the same for all reduce tasks.
        let locations_for_task: Vec<(String, u32)> = map_output_locations
            .iter()
            .flat_map(|(exec_id, map_statuses)| {
                let addr = executor_shuffle_addrs.get(exec_id).unwrap().clone();
                map_statuses
                    .iter()
                    .map(move |(map_id, _map_status)| (addr.clone(), *map_id))
            })
            .collect();

        // Create and submit ShuffleReduceTasks for each reduce partition
        for reduce_partition_id in 0..num_reduce_partitions {
            // Due to `typetag` limitations, we use a type check. A full implementation
            // would require a more advanced task serialization system.
            if std::any::TypeId::of::<(K, C)>() == std::any::TypeId::of::<(String, i32)>() {
                let task = crate::distributed::task::ShuffleReduceTask::<
                    String,
                    i32,
                    i32,
                    crate::shuffle::ReduceAggregator<i32>,
                >::new(
                    shuffle_id,
                    reduce_partition_id as u32,
                    locations_for_task.clone(),
                    aggregator_data.clone(),
                );

                let task_id = format!("shuffle-reduce-{}-{}", shuffle_id, reduce_partition_id);
                let stage_id = format!("shuffle-reduce-stage-{}", shuffle_id);

                let future = driver
                    .submit_task(task_id, stage_id, reduce_partition_id, Box::new(task), None)
                    .await
                    .map_err(|e| crate::traits::RddError::ContextError(e.to_string()))?;

                reduce_task_futures.push(future);
            } else {
                return Err(crate::traits::RddError::ContextError(
                    "Unsupported type for shuffle reduce task. Only (String, i32) is currently supported."
                        .to_string(),
                ));
            }
        }

        // Collect results from all reduce tasks
        let mut reduce_results = Vec::new();
        for future in reduce_task_futures {
            match future
                .await
                .map_err(|e| crate::traits::RddError::ContextError(e.to_string()))?
            {
                (crate::distributed::driver::TaskResult::Success(bytes), _executor_id) => {
                    let (partition_result, _): (Vec<(K, C)>, _) =
                        bincode::decode_from_slice(&bytes, bincode::config::standard()).map_err(
                            |e| crate::traits::RddError::SerializationError(e.to_string()),
                        )?;
                    reduce_results.push(partition_result);
                }
                (crate::distributed::driver::TaskResult::Failure(err), executor_id) => {
                    return Err(crate::traits::RddError::ComputationError(format!(
                        "Reduce task failed on executor {}: {}",
                        executor_id, err
                    )));
                }
            }
        }

        info!(
            "Reduce stage completed successfully with {} partitions",
            reduce_results.len()
        );
        Ok(reduce_results)
    }

    /// Run RDD computation in local mode
    async fn run_local<T>(
        &self,
        rdd: Arc<dyn crate::traits::RddBase<Item = T>>,
    ) -> RddResult<Vec<T>>
    where
        T: crate::operations::RddDataType,
    {
        debug!("Running RDD computation in local mode");
        let mut result = Vec::new();
        for p in rdd.partitions() {
            result.extend(rdd.compute(p.as_ref())?);
        }
        Ok(result)
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
