//! Executor implementation for distributed computing
//!
//! The Executor receives tasks from the Driver, executes them using
//! rayon thread pools, and reports results back.

use crate::distributed::proto::driver::{
    HeartbeatRequest, RegisterExecutorRequest, TaskMetrics as ProtoTaskMetrics,
    TaskStatusRequest as DriverTaskStatusRequest, driver_service_client::DriverServiceClient,
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

        let task_id = req.task_id.clone();
        let stage_id = req.stage_id.clone();
        let partition_index = req.partition_index;
        let task_data = req.serialized_partition;
        let executor_id = self.executor_info.executor_id.clone();

        // Update status to busy
        {
            let mut status = self.status.lock().await;
            *status = ExecutorStatus::Busy;
        }

        // Add to running tasks map for tracking
        {
            let mut running_tasks = self.running_tasks.lock().await;
            let task_info = TaskInfo {
                task_id: task_id.clone(),
                stage_id: stage_id.clone(),
                partition_index,
                start_time: Self::current_timestamp(),
                status: TaskState::Running,
            };
            running_tasks.insert(task_id.clone(), task_info);
        }

        // Submit task to task runner
        let execution_result = self
            .task_runner
            .submit_task(
                task_id.clone(),
                stage_id,
                partition_index as usize,
                task_data,
            )
            .await;

        let (state, error_message, result_metrics) = match execution_result {
            Ok(result) => {
                info!("Task {} completed successfully", task_id);
                (result.state, result.error_message, result.metrics)
            }
            Err(e) => {
                error!("Task {} failed to execute: {}", task_id, e);
                (TaskState::Failed, Some(e.to_string()), Default::default())
            }
        };

        // Update internal metrics
        {
            let mut metrics = self.metrics.lock().await;
            metrics.total_tasks += 1;
            if state == TaskState::Finished {
                metrics.succeeded_tasks += 1;
            } else {
                metrics.failed_tasks += 1;
            }
            metrics.total_duration_ms += result_metrics.executor_run_time_ms;
        }

        // Remove from running tasks
        self.running_tasks.lock().await.remove(&task_id);

        // Update executor status to idle if no more tasks are running
        if self.running_tasks.lock().await.is_empty() {
            *self.status.lock().await = ExecutorStatus::Idle;
        }

        // Report task status to driver
        let mut driver_client_guard = self.driver_client.lock().await;
        if let Some(client) = driver_client_guard.as_mut() {
            let status_request = DriverTaskStatusRequest {
                executor_id,
                task_id: task_id.clone(),
                state: Self::convert_to_proto_task_status(state) as i32,
                result: vec![], // Result data would be handled separately in a real implementation
                error_message: error_message.unwrap_or_default(),
                task_metrics: Some(ProtoTaskMetrics {
                    executor_deserialize_time_ms: result_metrics.executor_deserialize_time_ms,
                    executor_deserialize_cpu_time_ms: 0, // TODO: Implement CPU time tracking
                    executor_run_time_ms: result_metrics.executor_run_time_ms,
                    executor_cpu_time_ms: 0, // TODO: Implement CPU time tracking
                    result_size_bytes: result_metrics.result_size_bytes,
                    jvm_gc_time_ms: result_metrics.jvm_gc_time_ms,
                    result_serialization_time_ms: result_metrics.result_serialization_time_ms,
                    memory_bytes_spilled: result_metrics.memory_bytes_spilled,
                    disk_bytes_spilled: result_metrics.disk_bytes_spilled,
                    peak_execution_memory_bytes: result_metrics.peak_execution_memory_bytes,
                    input_bytes_read: 0,       // TODO: Implement input tracking
                    input_records_read: 0,     // TODO: Implement input tracking
                    output_bytes_written: 0,   // TODO: Implement output tracking
                    output_records_written: 0, // TODO: Implement output tracking
                    shuffle_read_bytes: 0,     // TODO: Implement shuffle tracking
                    shuffle_read_records: 0,   // TODO: Implement shuffle tracking
                    shuffle_write_bytes: 0,    // TODO: Implement shuffle tracking
                    shuffle_write_records: 0,  // TODO: Implement shuffle tracking
                    shuffle_write_time_ms: 0,  // TODO: Implement shuffle tracking
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
    driver_client: Arc<Mutex<Option<DriverServiceClient<tonic::transport::Channel>>>>,
    heartbeat_interval: Duration,
}

impl Executor {
    /// Create a new executor
    pub fn new(executor_info: ExecutorInfo, max_concurrent_tasks: usize) -> Self {
        let service = Arc::new(ExecutorServiceImpl::new(
            executor_info.clone(),
            max_concurrent_tasks,
            Arc::new(Mutex::new(None)),
        ));

        Self {
            // Pass the service's client handle here
            driver_client: Arc::clone(&service.driver_client),
            service,
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
            *self.driver_client.lock().await = Some(client);
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
    pub async fn start_heartbeat(&self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(client) = self.driver_client.lock().await.clone() {
            let executor_id = self.service.executor_info.executor_id.clone();
            let status = Arc::clone(&self.service.status);
            let metrics = Arc::clone(&self.service.metrics);
            let heartbeat_interval = self.heartbeat_interval;

            tokio::spawn(async move {
                let mut interval = interval(heartbeat_interval);
                let mut client = client;

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
