//! Executor implementation for distributed computing
//!
//! The Executor receives tasks from the Driver, executes them using
//! rayon thread pools, and reports results back.

use crate::distributed::proto::driver::{
    HeartbeatRequest, RegisterExecutorRequest, TaskStatusRequest as DriverTaskStatusRequest,
    driver_service_client::DriverServiceClient,
};
use crate::distributed::proto::executor::{
    ExecutorInfo as ProtoExecutorInfo, ExecutorMetrics as ProtoExecutorMetrics,
    ExecutorStatus as ProtoExecutorStatus, GetStatusRequest, GetStatusResponse, KillTaskRequest,
    KillTaskResponse, LaunchTaskRequest, LaunchTaskResponse, ShutdownRequest, ShutdownResponse,
    TaskInfo as ProtoTaskInfo, TaskStatus as ProtoTaskStatus,
    executor_service_server::{ExecutorService, ExecutorServiceServer},
};
use crate::distributed::task::TaskRunner;
use crate::distributed::types::{ExecutorInfo, ExecutorMetrics, ExecutorStatus, TaskId, TaskState};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, mpsc};
use tokio::time::{interval, sleep};
use tonic::{Request, Response, Status, transport::Server};
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
    /// Task runner for executing tasks
    task_runner: Arc<TaskRunner>,
    /// Executor metrics
    metrics: Arc<Mutex<ExecutorMetrics>>,
    /// Running tasks information
    running_tasks: Arc<Mutex<HashMap<TaskId, TaskInfo>>>,
}

impl ExecutorServiceImpl {
    /// Create a new executor service
    pub fn new(executor_info: ExecutorInfo, max_concurrent_tasks: usize) -> Self {
        Self {
            executor_info,
            status: Arc::new(Mutex::new(ExecutorStatus::Starting)),
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

        // Start gRPC server
        Server::builder()
            .add_service(ExecutorServiceServer::new(self.clone()))
            .serve(addr)
            .await?;

        Ok(())
    }

    /// Get current timestamp
    fn current_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    /// Convert internal ExecutorStatus to protobuf
    fn convert_to_proto_status(status: ExecutorStatus) -> i32 {
        match status {
            ExecutorStatus::Starting => 0,
            ExecutorStatus::Running => 1,
            ExecutorStatus::Idle => 2,
            ExecutorStatus::Busy => 3,
            ExecutorStatus::Stopping => 4,
            ExecutorStatus::Failed => 5,
        }
    }

    /// Convert internal TaskStatus to protobuf
    fn convert_to_proto_task_status(status: TaskState) -> i32 {
        match status {
            TaskState::Pending => 0,
            TaskState::Running => 1,
            TaskState::Finished => 2,
            TaskState::Failed => 3,
            TaskState::Killed => 4,
        }
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

        // Update status to busy
        {
            let mut status = self.status.lock().await;
            *status = ExecutorStatus::Busy;
        }

        // Add to running tasks
        {
            let mut running_tasks = self.running_tasks.lock().await;
            let task_info = TaskInfo {
                task_id: req.task_id.clone(),
                stage_id: req.stage_id.clone(),
                partition_index: req.partition_index,
                start_time: Self::current_timestamp(),
                status: TaskState::Running,
            };
            running_tasks.insert(req.task_id.clone(), task_info);
        }

        // Create task data for execution
        let task_data = req.serialized_partition;

        // Submit task to task runner
        let task_runner = Arc::clone(&self.task_runner);
        let task_id = req.task_id.clone();
        let running_tasks = Arc::clone(&self.running_tasks);
        let metrics = Arc::clone(&self.metrics);
        let status = Arc::clone(&self.status);

        tokio::spawn(async move {
            match task_runner.submit_task(task_data).await {
                Ok(result) => {
                    info!("Task {} completed successfully", task_id);

                    // Update metrics
                    {
                        let mut metrics = metrics.lock().await;
                        metrics.total_tasks += 1;
                        metrics.succeeded_tasks += 1;
                        metrics.total_duration_ms += result.metrics.executor_run_time_ms;
                    }

                    // Remove from running tasks
                    {
                        let mut running_tasks = running_tasks.lock().await;
                        running_tasks.remove(&task_id);
                    }

                    // Update status back to idle if no more tasks
                    {
                        let running_count = running_tasks.lock().await.len();
                        if running_count == 0 {
                            let mut status = status.lock().await;
                            *status = ExecutorStatus::Idle;
                        }
                    }

                    // Report task completion to driver
                    // This would be implemented by sending a status update to the driver
                    // For now, we'll log the completion
                    info!(
                        "Task {} completed, result size: {} bytes",
                        task_id,
                        result.result.as_ref().map(|r| r.len()).unwrap_or(0)
                    );
                }
                Err(e) => {
                    error!("Task {} failed: {}", task_id, e);

                    // Update metrics
                    {
                        let mut metrics = metrics.lock().await;
                        metrics.total_tasks += 1;
                        metrics.failed_tasks += 1;
                    }

                    // Remove from running tasks
                    {
                        let mut running_tasks = running_tasks.lock().await;
                        running_tasks.remove(&task_id);
                    }

                    // Update status back to idle
                    {
                        let running_count = running_tasks.lock().await.len();
                        if running_count == 0 {
                            let mut status = status.lock().await;
                            *status = ExecutorStatus::Idle;
                        }
                    }

                    // Report task failure to driver
                    // This would be implemented by sending a status update to the driver
                    // For now, we'll log the failure
                    error!(
                        "Task {} failed and will be reported to driver: {}",
                        task_id, e
                    );
                }
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

        let running_tasks: Vec<TaskInfo> =
            self.running_tasks.lock().await.values().cloned().collect();

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

        let proto_running_tasks: Vec<ProtoTaskInfo> = running_tasks
            .into_iter()
            .map(|task| ProtoTaskInfo {
                task_id: task.task_id,
                stage_id: task.stage_id,
                partition_index: task.partition_index,
                start_time: task.start_time,
                status: Self::convert_to_proto_task_status(task.status),
            })
            .collect();

        let response = GetStatusResponse {
            status: Self::convert_to_proto_status(status),
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
}

/// Executor manager for high-level executor operations
#[derive(Clone)]
pub struct Executor {
    service: Arc<ExecutorServiceImpl>,
    driver_client: Option<DriverServiceClient<tonic::transport::Channel>>,
    heartbeat_interval: Duration,
}

impl Executor {
    /// Create a new executor
    pub fn new(executor_info: ExecutorInfo, max_concurrent_tasks: usize) -> Self {
        let service = Arc::new(ExecutorServiceImpl::new(
            executor_info,
            max_concurrent_tasks,
        ));

        Self {
            service,
            driver_client: None,
            heartbeat_interval: Duration::from_secs(10), // 10 second heartbeat interval
        }
    }

    /// Register with the driver
    pub async fn register_with_driver(
        &mut self,
        driver_addr: String,
    ) -> Result<(), Box<dyn std::error::Error>> {
        info!("Connecting to driver at: {}", driver_addr);

        let mut client = DriverServiceClient::connect(driver_addr).await?;

        let request = RegisterExecutorRequest {
            executor_id: self.service.executor_info.executor_id.clone(),
            host: self.service.executor_info.host.clone(),
            port: self.service.executor_info.port as u32,
            cores: self.service.executor_info.cores,
            memory_mb: self.service.executor_info.memory_mb,
            attributes: self.service.executor_info.attributes.clone(),
        };

        let response = client.register_executor(request).await?;
        let resp = response.into_inner();

        if resp.success {
            info!("Successfully registered with driver: {}", resp.driver_id);
            self.driver_client = Some(client);
            Ok(())
        } else {
            Err(format!("Failed to register with driver: {}", resp.message).into())
        }
    }

    /// Start the executor service
    pub async fn start(&self, addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
        let service = Arc::clone(&self.service);
        service.start(addr).await
    }

    /// Start heartbeat loop
    pub async fn start_heartbeat(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(mut client) = self.driver_client.take() {
            let executor_id = self.service.executor_info.executor_id.clone();
            let status = Arc::clone(&self.service.status);
            let metrics = Arc::clone(&self.service.metrics);
            let heartbeat_interval = self.heartbeat_interval;

            tokio::spawn(async move {
                let mut interval = interval(heartbeat_interval);

                loop {
                    interval.tick().await;

                    let current_status = *status.lock().await;
                    let current_metrics = metrics.lock().await.clone();

                    let heartbeat_request = HeartbeatRequest {
                        executor_id: executor_id.clone(),
                        timestamp: ExecutorServiceImpl::current_timestamp(),
                        status: ExecutorServiceImpl::convert_to_proto_status(current_status),
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
                        }),
                    };

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
                }
            });

            Ok(())
        } else {
            Err("No driver client available for heartbeat".into())
        }
    }
}
