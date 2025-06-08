//! Executor implementation for distributed computing
//!
//! The Executor receives tasks from the Driver, executes them using
//! rayon thread pools, and reports results back.

use crate::context::DistributedConfig;
use crate::distributed::proto::driver::{
    driver_service_client::DriverServiceClient, ExecutorStatus, HeartbeatRequest,
    RegisterExecutorRequest, TaskMetrics as ProtoTaskMetrics, TaskState,
    TaskStatusRequest as DriverTaskStatusRequest,
};
use crate::distributed::proto::executor::{
    executor_service_server::{ExecutorService, ExecutorServiceServer},
    ExecutorInfo as ProtoExecutorInfo, ExecutorMetrics as ProtoExecutorMetrics, GetStatusRequest,
    GetStatusResponse, KillTaskRequest, KillTaskResponse, LaunchTaskRequest, LaunchTaskResponse,
    ShutdownRequest, ShutdownResponse, TaskInfo as ProtoTaskInfo,
};
use crate::distributed::task::TaskRunner;
use crate::distributed::types::{ExecutorInfo, ExecutorMetrics, TaskId};
use barks_network_shuffle::{shuffle::server::ShuffleServer, FileShuffleBlockManager};
use barks_utils::current_timestamp_secs;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::sync::Mutex;
use tokio::time::interval;
use tonic::{transport::Server, Request, Response, Status};
use tracing::{error, info, warn};

/// Task information for tracking running tasks
#[derive(Debug, Clone)]
struct TaskInfo {
    task_id: String,
    stage_id: String,
    partition_index: u32,
    start_time: u64,
    status: TaskState,
}

/// Executor service implementation
#[derive(Clone)]
pub struct ExecutorServiceImpl {
    /// Executor information
    executor_info: ExecutorInfo,
    /// Current executor status
    status: Arc<Mutex<ExecutorStatus>>,
    /// Client for communicating with the driver
    driver_client: Arc<Mutex<Option<DriverServiceClient<tonic::transport::Channel>>>>,
    /// Task runner for executing tasks
    task_runner: Arc<TaskRunner>,
    /// Executor metrics
    metrics: Arc<Mutex<ExecutorMetrics>>,
    /// Running tasks information
    running_tasks: Arc<Mutex<HashMap<TaskId, TaskInfo>>>,
}

impl ExecutorServiceImpl {
    /// Create a new executor service
    pub fn new(
        executor_info: ExecutorInfo,
        max_concurrent_tasks: usize,
        driver_client: Arc<Mutex<Option<DriverServiceClient<tonic::transport::Channel>>>>,
    ) -> Self {
        Self {
            executor_info,
            status: Arc::new(Mutex::new(ExecutorStatus::Starting)),
            driver_client,
            task_runner: Arc::new(TaskRunner::new(max_concurrent_tasks)),
            metrics: Arc::new(Mutex::new(ExecutorMetrics::default())),
            running_tasks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Start the executor service
    pub async fn start(&self, addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
        info!("Starting Executor service on {}", addr);

        // Update status to running
        {
            let mut status = self.status.lock().await;
            *status = ExecutorStatus::Running;
        }

        // Start gRPC server in background
        let service = self.clone();
        tokio::spawn(async move {
            if let Err(e) = Server::builder()
                .add_service(ExecutorServiceServer::new(service))
                .serve(addr)
                .await
            {
                error!("Executor gRPC server failed: {}", e);
            }
        });

        // Give the server a moment to start
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        Ok(())
    }

    /// Get current timestamp
    fn current_timestamp() -> u64 {
        current_timestamp_secs()
    }
}

#[tonic::async_trait]
impl ExecutorService for ExecutorServiceImpl {
    /// Launch a task on this executor
    async fn launch_task(
        &self,
        request: Request<LaunchTaskRequest>,
    ) -> Result<Response<LaunchTaskResponse>, Status> {
        let req = request.into_inner();

        info!("Launching task: {}", req.task_id);

        let task_id = req.task_id.clone();
        let stage_id = req.stage_id.clone();
        let partition_index = req.partition_index;
        let serialized_task = req.serialized_task;
        let max_result_size = req.max_result_size_bytes;

        // Clone necessary Arcs to move into the spawned task
        let status_arc = Arc::clone(&self.status);
        let running_tasks_arc = Arc::clone(&self.running_tasks);
        let metrics_arc = Arc::clone(&self.metrics);
        let task_runner_arc = Arc::clone(&self.task_runner);
        let driver_client_arc = Arc::clone(&self.driver_client);
        let _executor_info_arc = self.executor_info.clone();
        let executor_id = self.executor_info.executor_id.clone();

        // Update status to busy
        {
            let mut status = self.status.lock().await;
            *status = ExecutorStatus::Busy;
        }
        let task_id_clone = task_id.clone();

        // Add to running tasks map for tracking
        {
            let mut running_tasks = self.running_tasks.lock().await;
            let task_info = TaskInfo {
                task_id: task_id_clone.clone(),
                stage_id: stage_id.clone(),
                partition_index,
                start_time: Self::current_timestamp(),
                status: TaskState::TaskRunning,
            };
            running_tasks.insert(task_id_clone.clone(), task_info);
        }

        // Spawn the task execution in the background and return immediately.
        // The spawned task will report the status to the driver upon completion.
        tokio::spawn(async move {
            // Submit task to task runner
            let task_result = task_runner_arc
                .submit_task(partition_index as usize, serialized_task)
                .await;

            let mut final_task_result = task_result;
            // Check if result size exceeds the limit
            if let Some(ref result_bytes) = final_task_result.result {
                if result_bytes.len() as u64 > max_result_size {
                    let error_msg = format!(
                        "Task {} result size ({} bytes) exceeds limit ({} bytes)",
                        task_id,
                        result_bytes.len(),
                        max_result_size
                    );
                    warn!("{}", error_msg);
                    final_task_result.state = TaskState::TaskFailed;
                    final_task_result.result = None;
                    final_task_result.error_message = Some(error_msg);
                }
            }

            info!(
                "Task {} completed with state: {:?}",
                task_id, final_task_result.state
            );

            // Update internal metrics
            {
                let mut metrics = metrics_arc.lock().await;
                metrics.total_tasks += 1;
                if final_task_result.state == TaskState::TaskFinished {
                    metrics.succeeded_tasks += 1;
                } else {
                    metrics.failed_tasks += 1;
                }
                metrics.total_duration_ms += final_task_result.metrics.executor_run_time_ms;
            }

            // Atomically update running tasks map and executor status
            {
                let mut running_tasks = running_tasks_arc.lock().await;
                running_tasks.remove(&task_id);
                if running_tasks.is_empty() {
                    *status_arc.lock().await = ExecutorStatus::Idle;
                }
            }

            // Report task status to driver
            let mut driver_client_guard = driver_client_arc.lock().await;
            if let Some(client) = driver_client_guard.as_mut() {
                let status_request = DriverTaskStatusRequest {
                    executor_id,
                    task_id: task_id.clone(),
                    state: final_task_result.state as i32,
                    result: final_task_result.result.unwrap_or_default(),
                    error_message: final_task_result.error_message.unwrap_or_default(),
                    task_metrics: Some(ProtoTaskMetrics {
                        executor_deserialize_time_ms: final_task_result
                            .metrics
                            .executor_deserialize_time_ms,
                        executor_deserialize_cpu_time_ms: 0, // TODO: Implement CPU time tracking
                        executor_run_time_ms: final_task_result.metrics.executor_run_time_ms,
                        executor_cpu_time_ms: 0, // TODO: Implement CPU time tracking
                        result_size_bytes: final_task_result.metrics.result_size_bytes,
                        jvm_gc_time_ms: final_task_result.metrics.jvm_gc_time_ms,
                        result_serialization_time_ms: final_task_result
                            .metrics
                            .result_serialization_time_ms,
                        memory_bytes_spilled: final_task_result.metrics.memory_bytes_spilled,
                        disk_bytes_spilled: final_task_result.metrics.disk_bytes_spilled,
                        peak_execution_memory_bytes: final_task_result
                            .metrics
                            .peak_execution_memory_bytes,
                        input_bytes_read: 0,     // TODO: Implement input tracking
                        input_records_read: 0,   // TODO: Implement input tracking
                        output_bytes_written: 0, // TODO: Implement output tracking
                        output_records_written: 0, // TODO: Implement output tracking
                        shuffle_read_bytes: 0,   // TODO: Implement shuffle tracking
                        shuffle_read_records: 0, // TODO: Implement shuffle tracking
                        shuffle_write_bytes: 0,  // TODO: Implement shuffle tracking
                        shuffle_write_records: 0, // TODO: Implement shuffle tracking
                        shuffle_write_time_ms: 0, // TODO: Implement shuffle tracking
                    }),
                };
                if let Err(e) = client.report_task_status(status_request).await {
                    error!(
                        "Failed to report task status for {} to driver: {}",
                        task_id, e
                    );
                }
            } else {
                warn!(
                    "Cannot report status for task {}, no driver client.",
                    task_id
                );
            }
        });

        let response = LaunchTaskResponse {
            success: true,
            message: "Task launched successfully".to_string(),
        };

        Ok(Response::new(response))
    }

    /// Kill a running task
    async fn kill_task(
        &self,
        request: Request<KillTaskRequest>,
    ) -> Result<Response<KillTaskResponse>, Status> {
        let req = request.into_inner();

        info!("Killing task: {} with reason: {}", req.task_id, req.reason);

        match self.task_runner.kill_task(&req.task_id, &req.reason).await {
            Ok(_) => {
                // Remove from running tasks
                {
                    let mut running_tasks = self.running_tasks.lock().await;
                    running_tasks.remove(&req.task_id);
                }

                let response = KillTaskResponse {
                    success: true,
                    message: "Task killed successfully".to_string(),
                };

                Ok(Response::new(response))
            }
            Err(e) => {
                error!("Failed to kill task {}: {}", req.task_id, e);

                let response = KillTaskResponse {
                    success: false,
                    message: format!("Failed to kill task: {}", e),
                };

                Ok(Response::new(response))
            }
        }
    }

    /// Get executor status and metrics
    async fn get_status(
        &self,
        request: Request<GetStatusRequest>,
    ) -> Result<Response<GetStatusResponse>, Status> {
        let req = request.into_inner();

        let status = *self.status.lock().await;
        let metrics = if req.include_metrics {
            Some(self.metrics.lock().await.clone())
        } else {
            None
        };

        let proto_info = ProtoExecutorInfo {
            executor_id: self.executor_info.executor_id.clone(),
            host: self.executor_info.host.clone(),
            port: self.executor_info.port as u32,
            cores: self.executor_info.cores,
            memory_mb: self.executor_info.memory_mb,
            attributes: self.executor_info.attributes.clone(),
            start_time: self.executor_info.start_time,
        };

        let proto_metrics = metrics.map(|m| ProtoExecutorMetrics {
            total_tasks: m.total_tasks,
            failed_tasks: m.failed_tasks,
            succeeded_tasks: m.succeeded_tasks,
            total_duration_ms: m.total_duration_ms,
            total_gc_time_ms: m.total_gc_time_ms,
            total_input_bytes: 0,         // TODO: Implement
            total_shuffle_read_bytes: 0,  // TODO: Implement
            total_shuffle_write_bytes: 0, // TODO: Implement
            max_memory_bytes: m.max_memory_bytes,
            memory_used_bytes: m.memory_used_bytes,
            active_tasks: m.active_tasks,
        });

        let proto_running_tasks = self.get_proto_running_tasks().await;

        let response = GetStatusResponse {
            status: status as i32,
            info: Some(proto_info),
            metrics: proto_metrics,
            running_tasks: proto_running_tasks,
        };

        Ok(Response::new(response))
    }

    /// Shutdown the executor
    async fn shutdown(
        &self,
        request: Request<ShutdownRequest>,
    ) -> Result<Response<ShutdownResponse>, Status> {
        let req = request.into_inner();

        info!("Shutting down executor with reason: {}", req.reason);

        // Update status to stopping
        {
            let mut status = self.status.lock().await;
            *status = ExecutorStatus::Stopping;
        }

        // TODO: Gracefully stop all running tasks
        // TODO: Cleanup resources

        let response = ShutdownResponse {
            success: true,
            message: "Executor shutdown initiated".to_string(),
        };

        Ok(Response::new(response))
    }

    async fn cleanup_shuffle(
        &self,
        request: Request<super::proto::executor::CleanupShuffleRequest>,
    ) -> Result<Response<super::proto::executor::CleanupShuffleResponse>, Status> {
        let req = request.into_inner();
        let shuffle_id = req.shuffle_id;

        info!("Received request to clean up shuffle {}", shuffle_id);

        // For now, we'll just return success since we don't have access to the shuffle block manager here
        // In a real implementation, we would need to pass the block manager to the executor service
        // or implement a cleanup mechanism through the executor
        Ok(Response::new(
            super::proto::executor::CleanupShuffleResponse {
                success: true,
                message: format!(
                    "Shuffle {} cleanup requested (placeholder implementation).",
                    shuffle_id
                ),
            },
        ))
    }
}

impl ExecutorServiceImpl {
    async fn get_proto_running_tasks(&self) -> Vec<ProtoTaskInfo> {
        let running_tasks = self.running_tasks.lock().await;
        running_tasks
            .values()
            .map(|task| ProtoTaskInfo {
                task_id: task.task_id.clone(),
                stage_id: task.stage_id.clone(),
                partition_index: task.partition_index,
                start_time: task.start_time,
                status: task.status as i32,
            })
            .collect()
    }
}

/// Executor manager for high-level executor operations
#[derive(Clone)]
pub struct Executor {
    service: Arc<ExecutorServiceImpl>,
    config: DistributedConfig,
}

impl Executor {
    /// Create a new executor
    pub fn new(
        executor_info: ExecutorInfo,
        max_concurrent_tasks: usize,
        config: DistributedConfig,
    ) -> Self {
        let driver_client = Arc::new(Mutex::new(None));
        let service = Arc::new(ExecutorServiceImpl::new(
            executor_info.clone(),
            max_concurrent_tasks,
            driver_client,
        ));

        Self { service, config }
    }

    /// Register with the driver with retry logic and backoff strategy
    ///
    /// This method implements a robust connection strategy that retries
    /// connection attempts with exponential backoff, making the system
    /// more resilient to timing issues during cluster startup.
    pub async fn register_with_driver(
        &self,
        driver_addr: String,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut attempts = 0;
        let max_attempts = 5;
        let base_retry_delay = Duration::from_secs(2);

        loop {
            info!(
                "Connecting to driver at: {} (attempt {}/{})",
                driver_addr,
                attempts + 1,
                max_attempts
            );

            match DriverServiceClient::connect(driver_addr.clone()).await {
                Ok(mut client) => {
                    let request = RegisterExecutorRequest {
                        executor_id: self.service.executor_info.executor_id.clone(),
                        host: self.service.executor_info.host.clone(),
                        port: self.service.executor_info.port as u32,
                        cores: self.service.executor_info.cores,
                        memory_mb: self.service.executor_info.memory_mb,
                        max_concurrent_tasks: self.service.executor_info.max_concurrent_tasks,
                        attributes: self.service.executor_info.attributes.clone(),
                        shuffle_port: (self.service.executor_info.port + 1000) as u32, // Use port + 1000 for shuffle service
                    };

                    match client.register_executor(request).await {
                        Ok(response) => {
                            let resp = response.into_inner();
                            if resp.success {
                                info!("Successfully registered with driver: {}", resp.driver_id);
                                *self.service.driver_client.lock().await = Some(client);
                                return Ok(());
                            } else {
                                return Err(format!(
                                    "Failed to register with driver: {}",
                                    resp.message
                                )
                                .into());
                            }
                        }
                        Err(e) => {
                            warn!("Registration request failed: {}", e);
                            attempts += 1;
                            if attempts >= max_attempts {
                                error!(
                                    "Failed to register with driver after {} attempts: {}",
                                    max_attempts, e
                                );
                                return Err(e.into());
                            }
                        }
                    }
                }
                Err(e) => {
                    attempts += 1;
                    if attempts >= max_attempts {
                        error!(
                            "Failed to connect to driver after {} attempts: {}",
                            max_attempts, e
                        );
                        return Err(e.into());
                    }

                    // Exponential backoff: 2s, 4s, 8s, 16s
                    let retry_delay = base_retry_delay * 2_u32.pow(attempts - 1);
                    warn!(
                        "Failed to connect to driver, will retry in {:?}: {}",
                        retry_delay, e
                    );
                    tokio::time::sleep(retry_delay).await;
                }
            }
        }
    }

    /// Start the executor service
    pub async fn start(&self, addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
        // Start the main executor gRPC service
        let service = Arc::clone(&self.service);

        // Start the Shuffle Service on a different port
        let shuffle_port = addr.port() + 1000;
        let shuffle_addr_str = format!("{}:{}", addr.ip(), shuffle_port);
        let shuffle_addr: SocketAddr = shuffle_addr_str.parse()?;

        // Each executor needs its own root directory for shuffle files.
        // In a real deployment, this would be a configured path.
        let temp_dir = tempdir()?;
        let shuffle_root_dir = temp_dir.keep();
        info!("Shuffle root directory: {:?}", shuffle_root_dir);
        let block_manager = Arc::new(FileShuffleBlockManager::new(shuffle_root_dir)?);
        let shuffle_server = ShuffleServer::new(shuffle_addr, block_manager);

        tokio::spawn(async move {
            if let Err(e) = shuffle_server.start().await {
                error!("Shuffle server failed to start: {}", e);
            }
        });

        service.start(addr).await
    }

    /// Start heartbeat loop
    pub async fn start_heartbeat(&self) -> Result<(), Box<dyn std::error::Error>> {
        if self.service.driver_client.lock().await.is_some() {
            let driver_client_arc = Arc::clone(&self.service.driver_client);
            let executor_id = self.service.executor_info.executor_id.clone();
            let status = Arc::clone(&self.service.status);
            let metrics = Arc::clone(&self.service.metrics);
            let running_tasks = Arc::clone(&self.service.running_tasks);
            let heartbeat_interval =
                Duration::from_secs(self.config.executor_heartbeat_interval_secs);

            tokio::spawn(async move {
                let mut interval = interval(heartbeat_interval);

                loop {
                    interval.tick().await;

                    let active_tasks_count = running_tasks.lock().await.len() as u32;
                    let current_status = *status.lock().await;
                    let current_metrics = metrics.lock().await.clone();

                    let heartbeat_request = HeartbeatRequest {
                        executor_id: executor_id.clone(),
                        timestamp: Executor::current_timestamp(),
                        status: current_status as i32,
                        accumulator_updates: vec![], // TODO: Implement accumulator updates
                        metrics: Some(crate::distributed::proto::driver::ExecutorMetrics {
                            total_tasks: current_metrics.total_tasks,
                            failed_tasks: current_metrics.failed_tasks,
                            succeeded_tasks: current_metrics.succeeded_tasks,
                            total_duration_ms: current_metrics.total_duration_ms,
                            total_gc_time_ms: current_metrics.total_gc_time_ms,
                            total_input_bytes: 0,         // TODO: Implement
                            total_shuffle_read_bytes: 0,  // TODO: Implement
                            total_shuffle_write_bytes: 0, // TODO: Implement
                            max_memory_bytes: current_metrics.max_memory_bytes,
                            memory_used_bytes: current_metrics.memory_used_bytes,
                            active_tasks: active_tasks_count,
                        }),
                    };

                    let mut driver_client_guard = driver_client_arc.lock().await;
                    if let Some(client) = driver_client_guard.as_mut() {
                        match client.heartbeat(heartbeat_request).await {
                            Ok(response) => {
                                let resp = response.into_inner();
                                if !resp.success {
                                    warn!("Heartbeat failed: {}", resp.message);
                                    if resp.should_reregister {
                                        error!("Driver requested re-registration");
                                        // TODO: Handle re-registration
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Failed to send heartbeat: {}", e);
                                // TODO: Handle connection failure
                            }
                        }
                    } else {
                        warn!("No driver client available for heartbeat, skipping.");
                    }
                }
            });

            Ok(())
        } else {
            Err("No driver client available for heartbeat".into())
        }
    }
}

impl Executor {
    // Helper for getting current timestamp, moved from service to be accessible here
    fn current_timestamp() -> u64 {
        current_timestamp_secs()
    }
}
