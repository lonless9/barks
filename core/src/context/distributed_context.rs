//! Distributed context for managing distributed RDD operations
//!
//! This module provides the DistributedContext which extends FlowContext
//! to support distributed execution using the Driver-Executor model.

use crate::distributed::driver::Driver;
use crate::distributed::executor::Executor;
use crate::distributed::stage::{DAGScheduler, Stage};
use crate::distributed::types::*;
use crate::rdd::DistributedRdd;
use crate::traits::{RddError, RddResult};
use barks_network_shuffle::traits::MapStatus;

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
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
    /// DAG scheduler for managing stages
    #[allow(dead_code)] // Will be used in future implementations
    dag_scheduler: Arc<DAGScheduler>,
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
    /// Maximum number of times a stage will be retried on failure.
    pub stage_max_retries: u32,
    /// Configuration for shuffle behavior
    pub shuffle_config: barks_network_shuffle::optimizations::ShuffleConfig,
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
            stage_max_retries: 3,
            shuffle_config: Default::default(),
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
            dag_scheduler: Arc::new(DAGScheduler::new()),
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
            dag_scheduler: Arc::new(DAGScheduler::new()),
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
            dag_scheduler: Arc::new(DAGScheduler::new()),
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
        match self.mode {
            ExecutionMode::Driver => {
                if let Some(driver) = &self.driver {
                    let executor_count = driver.executor_count().await;
                    if executor_count > 0 && self.config.driver_addr.is_some() {
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
                let driver = self.driver.as_ref().ok_or_else(|| {
                    crate::traits::RddError::ContextError("Driver not initialized.".to_string())
                })?;

                if driver.executor_count().await == 0 {
                    warn!("No executors available. Running job locally on the driver.");
                    return self.run_local(rdd).await;
                }

                let job_id = Uuid::new_v4().to_string();
                info!("Starting job {} for RDD {}", job_id, rdd.id());

                // 1. Build the stage graph from the final RDD using the DAGScheduler.
                // The `as_any` is necessary for the `Stage` struct which stores the RDD as `Any`.
                let rdd_any: Arc<dyn std::any::Any + Send + Sync> =
                    unsafe { std::mem::transmute(rdd.clone()) };
                let final_stage = self.dag_scheduler.new_result_stage(rdd_any, rdd, &job_id);

                // 2. Initialize job execution state.
                let mut all_stages: HashMap<usize, Arc<Stage>> = HashMap::new();
                let mut waiting: HashSet<usize> = HashSet::new();
                let mut running: HashSet<usize> = HashSet::new();
                let mut completed_map_outputs: HashMap<usize, Vec<MapStatus>> = HashMap::new();
                let mut failed_attempts: HashMap<usize, u32> = HashMap::new(); // stage_id -> attempt_count

                // Helper to traverse the stage graph and populate `all_stages`.
                let mut q = VecDeque::new();
                q.push_back(Arc::new(final_stage.stage.clone()));
                all_stages.insert(final_stage.stage.id, Arc::new(final_stage.stage.clone()));
                while let Some(s) = q.pop_front() {
                    for parent in &s.parents {
                        if all_stages.insert(parent.id, parent.clone()).is_none() {
                            q.push_back(parent.clone());
                        }
                    }
                }

                waiting.extend(all_stages.keys());

                // 3. Main execution loop.
                while !completed_map_outputs.contains_key(&final_stage.stage.id) {
                    let mut ready_stages = Vec::new();
                    for stage_id in &waiting {
                        let stage = all_stages.get(stage_id).unwrap();
                        let parents_completed = stage
                            .parents
                            .iter()
                            .all(|p| completed_map_outputs.contains_key(&p.id));
                        if parents_completed {
                            ready_stages.push(stage.clone());
                        }
                    }

                    if ready_stages.is_empty() && running.is_empty() && !waiting.is_empty() {
                        return Err(RddError::ContextError(format!(
                            "Job {} failed. Deadlocked with waiting stages: {:?}. Failed stages: {:?}",
                            job_id, waiting, failed_attempts
                        )));
                    }

                    for stage in ready_stages {
                        waiting.remove(&stage.id);
                        running.insert(stage.id);

                        info!(
                            "Submitting stage {} (attempt {})",
                            stage.id,
                            stage.attempt_id.load(Ordering::SeqCst)
                        );
                        let stage_id_str = format!("stage-{}-{}", stage.id, stage.new_attempt_id());

                        // Create tasks for the stage. This needs parent results for shuffle dependencies.
                        // Note: The downcast here assumes the RDD item type is consistent, a limitation of the current design.
                        let tasks = stage
                            .rdd
                            .downcast_ref::<Arc<dyn crate::traits::RddBase<Item = T>>>()
                            .unwrap()
                            .create_tasks(stage_id_str.clone())?;

                        let mut futures = Vec::new();
                        for (i, task) in tasks.into_iter().enumerate() {
                            let task_id = format!("task-{}-{}", stage_id_str, i);
                            let future = driver
                                .submit_task(task_id, stage_id_str.clone(), i, task, None)
                                .await
                                .map_err(|e| {
                                    RddError::ContextError(format!("Failed to submit task: {}", e))
                                })?;
                            futures.push(future);
                        }

                        let mut task_results = Vec::new();
                        let mut stage_failed = false;
                        for future in futures {
                            match future.await {
                                Ok((result, _)) => {
                                    if let crate::distributed::driver::TaskResult::Failure(err) =
                                        &result
                                    {
                                        error!(
                                            "Task failed for stage {}, aborting stage. Reason: {}",
                                            stage.id, err
                                        );
                                        stage_failed = true;
                                        break;
                                    }
                                    task_results.push(result);
                                }
                                Err(e) => {
                                    error!("Task future failed for stage {}: {}", stage.id, e);
                                    stage_failed = true;
                                    break;
                                }
                            }
                        }

                        running.remove(&stage.id);

                        if stage_failed {
                            let attempt = failed_attempts.entry(stage.id).or_insert(0);
                            *attempt += 1;
                            if *attempt > self.config.stage_max_retries {
                                error!(
                                    "Stage {} failed permanently after {} retries. Aborting job {}.",
                                    stage.id, *attempt, job_id
                                );
                                return Err(RddError::ComputationError(format!(
                                    "Stage {} failed permanently",
                                    stage.id
                                )));
                            }
                            warn!(
                                "Stage {} failed, attempt {}. Re-submitting parent stages.",
                                stage.id, *attempt
                            );
                            // Re-submit this stage and its parents to the waiting pool
                            let mut q = VecDeque::new();
                            q.push_back(stage.id);
                            while let Some(s_id) = q.pop_front() {
                                waiting.insert(s_id);
                                completed_map_outputs.remove(&s_id); // Invalidate parent results
                                if let Some(s) = all_stages.get(&s_id) {
                                    for p in &s.parents {
                                        q.push_back(p.id);
                                    }
                                }
                            }
                        } else {
                            // Stage succeeded. Process results.
                            let is_final_stage = stage.id == final_stage.stage.id;
                            if is_final_stage {
                                let collected_results = task_results
                                    .iter()
                                    .flat_map(|task_result| match task_result {
                                        crate::distributed::driver::TaskResult::Success(bytes) => {
                                            let (partition_result, _): (Vec<T>, _) =
                                                bincode::decode_from_slice(
                                                    bytes,
                                                    bincode::config::standard(),
                                                )
                                                .unwrap();
                                            partition_result
                                        }
                                        _ => vec![],
                                    })
                                    .collect();
                                return Ok(collected_results);
                            } else {
                                // It's a ShuffleMapStage, collect MapStatus
                                let map_statuses = task_results
                                    .into_iter()
                                    .map(|r| {
                                        match r {
                                            crate::distributed::driver::TaskResult::Success(
                                                bytes,
                                            ) => {
                                                bincode::decode_from_slice(
                                                    &bytes,
                                                    bincode::config::standard(),
                                                )
                                                .unwrap()
                                                .0
                                            }
                                            _ => unreachable!(), // Failures are handled above
                                        }
                                    })
                                    .collect();
                                completed_map_outputs.insert(stage.id, map_statuses);
                            }
                        }
                    }

                    // If we have no running stages and no ready stages, but are not done, wait.
                    if running.is_empty() && !waiting.is_empty() {
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                }

                // Should not be reached if logic is correct
                Err(RddError::ContextError(
                    "Job execution loop finished without completing the final stage.".to_string(),
                ))
            }
            _ => Err(crate::traits::RddError::ContextError(
                "run_distributed can only be called in Driver mode.".to_string(),
            )),
        }
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
