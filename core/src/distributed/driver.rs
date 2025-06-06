//! Driver implementation for distributed computing
//!
//! The Driver is responsible for coordinating executors, scheduling tasks,
//! and managing the overall distributed computation.

use crate::distributed::proto::driver::{
    ExecutorMetrics as ProtoExecutorMetrics, ExecutorStatus as ProtoExecutorStatus, GetTaskRequest,
    GetTaskResponse, HeartbeatRequest, HeartbeatResponse, RegisterExecutorRequest,
    RegisterExecutorResponse, Task, TaskMetrics as ProtoTaskMetrics, TaskState as ProtoTaskState,
    TaskStatusRequest, TaskStatusResponse,
    driver_service_server::{DriverService, DriverServiceServer},
};
use crate::distributed::task::TaskScheduler;
use crate::distributed::types::{
    ExecutorId, ExecutorInfo, ExecutorMetrics, ExecutorStatus, StageId, TaskId, TaskMetrics,
    TaskState,
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, mpsc};
use tonic::{Request, Response, Status, transport::Server};
use tracing::{debug, error, info, warn};

/// Driver service implementation
#[derive(Clone)]
pub struct DriverServiceImpl {
    /// Unique driver identifier
    driver_id: String,
    /// Task scheduler for distributing work
    task_scheduler: Arc<TaskScheduler>,
    /// Registered executors
    executors: Arc<Mutex<HashMap<ExecutorId, RegisteredExecutor>>>,
    /// Heartbeat monitoring
    heartbeat_receiver: Arc<Mutex<mpsc::UnboundedReceiver<HeartbeatInfo>>>,
    heartbeat_sender: mpsc::UnboundedSender<HeartbeatInfo>,
}

/// Information about a registered executor
#[derive(Debug, Clone)]
struct RegisteredExecutor {
    info: ExecutorInfo,
    last_heartbeat: u64,
    status: ExecutorStatus,
    metrics: ExecutorMetrics,
}

/// Heartbeat information
#[derive(Debug, Clone)]
struct HeartbeatInfo {
    executor_id: ExecutorId,
    timestamp: u64,
    status: ExecutorStatus,
    metrics: ExecutorMetrics,
}

impl DriverServiceImpl {
    /// Create a new driver service
    pub fn new(driver_id: String) -> Self {
        let (heartbeat_sender, heartbeat_receiver) = mpsc::unbounded_channel();

        Self {
            driver_id,
            task_scheduler: Arc::new(TaskScheduler::new()),
            executors: Arc::new(Mutex::new(HashMap::new())),
            heartbeat_receiver: Arc::new(Mutex::new(heartbeat_receiver)),
            heartbeat_sender,
        }
    }

    /// Start the driver service
    pub async fn start(&self, addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
        info!("Starting Driver service on {}", addr);

        // Start heartbeat monitoring task
        let heartbeat_receiver = Arc::clone(&self.heartbeat_receiver);
        let executors = Arc::clone(&self.executors);
        tokio::spawn(async move {
            Self::heartbeat_monitor(heartbeat_receiver, executors).await;
        });

        // Start gRPC server
        Server::builder()
            .add_service(DriverServiceServer::new(self.clone()))
            .serve(addr)
            .await?;

        Ok(())
    }

    /// Monitor executor heartbeats
    async fn heartbeat_monitor(
        heartbeat_receiver: Arc<Mutex<mpsc::UnboundedReceiver<HeartbeatInfo>>>,
        executors: Arc<Mutex<HashMap<ExecutorId, RegisteredExecutor>>>,
    ) {
        let mut receiver = heartbeat_receiver.lock().await;

        while let Some(heartbeat) = receiver.recv().await {
            let mut executors = executors.lock().await;

            if let Some(executor) = executors.get_mut(&heartbeat.executor_id) {
                executor.last_heartbeat = heartbeat.timestamp;
                executor.status = heartbeat.status;
                executor.metrics = heartbeat.metrics;

                debug!("Updated heartbeat for executor: {}", heartbeat.executor_id);
            } else {
                warn!(
                    "Received heartbeat from unknown executor: {}",
                    heartbeat.executor_id
                );
            }
        }
    }

    /// Get current timestamp
    fn current_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    /// Convert protobuf ExecutorStatus to internal type
    fn convert_executor_status(status: i32) -> ExecutorStatus {
        match status {
            0 => ExecutorStatus::Starting,
            1 => ExecutorStatus::Running,
            2 => ExecutorStatus::Idle,
            3 => ExecutorStatus::Busy,
            4 => ExecutorStatus::Stopping,
            5 => ExecutorStatus::Failed,
            _ => ExecutorStatus::Failed,
        }
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

    /// Convert protobuf TaskState to internal type
    fn convert_task_state(state: i32) -> TaskState {
        match state {
            0 => TaskState::Pending,
            1 => TaskState::Running,
            2 => TaskState::Finished,
            3 => TaskState::Failed,
            4 => TaskState::Killed,
            _ => TaskState::Failed,
        }
    }
}

#[tonic::async_trait]
impl DriverService for DriverServiceImpl {
    /// Register a new executor
    async fn register_executor(
        &self,
        request: Request<RegisterExecutorRequest>,
    ) -> Result<Response<RegisterExecutorResponse>, Status> {
        let req = request.into_inner();

        info!("Registering executor: {}", req.executor_id);

        let executor_info = ExecutorInfo::new(
            req.executor_id.clone(),
            req.host,
            req.port as u16,
            req.cores,
            req.memory_mb,
        )
        .with_attributes(req.attributes);

        let registered_executor = RegisteredExecutor {
            info: executor_info.clone(),
            last_heartbeat: Self::current_timestamp(),
            status: ExecutorStatus::Starting,
            metrics: ExecutorMetrics::default(),
        };

        // Register with task scheduler
        self.task_scheduler.register_executor(executor_info).await;

        // Add to executors map
        {
            let mut executors = self.executors.lock().await;
            executors.insert(req.executor_id.clone(), registered_executor);
        }

        let response = RegisterExecutorResponse {
            success: true,
            message: "Executor registered successfully".to_string(),
            driver_id: self.driver_id.clone(),
        };

        info!("Successfully registered executor: {}", req.executor_id);
        Ok(Response::new(response))
    }

    /// Handle executor heartbeat
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();

        debug!("Received heartbeat from executor: {}", req.executor_id);

        // Convert protobuf metrics to internal type
        let metrics = if let Some(proto_metrics) = req.metrics {
            ExecutorMetrics {
                total_tasks: proto_metrics.total_tasks,
                failed_tasks: proto_metrics.failed_tasks,
                succeeded_tasks: proto_metrics.succeeded_tasks,
                total_duration_ms: proto_metrics.total_duration_ms,
                total_gc_time_ms: proto_metrics.total_gc_time_ms,
                max_memory_bytes: proto_metrics.max_memory_bytes,
                memory_used_bytes: proto_metrics.memory_used_bytes,
                active_tasks: 0, // TODO: Calculate active tasks from running tasks
            }
        } else {
            ExecutorMetrics::default()
        };

        let heartbeat_info = HeartbeatInfo {
            executor_id: req.executor_id.clone(),
            timestamp: req.timestamp,
            status: Self::convert_executor_status(req.status),
            metrics,
        };

        // Send heartbeat to monitor
        if let Err(_) = self.heartbeat_sender.send(heartbeat_info) {
            error!("Failed to send heartbeat to monitor");
            return Err(Status::internal("Failed to process heartbeat"));
        }

        let response = HeartbeatResponse {
            success: true,
            should_reregister: false,
            message: "Heartbeat received".to_string(),
        };

        Ok(Response::new(response))
    }

    /// Handle task status report
    async fn report_task_status(
        &self,
        request: Request<TaskStatusRequest>,
    ) -> Result<Response<TaskStatusResponse>, Status> {
        let req = request.into_inner();

        info!(
            "Received task status report: task_id={}, state={}, executor={}",
            req.task_id, req.state, req.executor_id
        );

        let task_state = Self::convert_task_state(req.state);

        match task_state {
            TaskState::Finished => {
                info!("Task {} completed successfully", req.task_id);
                // TODO: Process task result
            }
            TaskState::Failed => {
                error!("Task {} failed: {}", req.task_id, req.error_message);
                // TODO: Handle task failure, possibly reschedule
            }
            TaskState::Killed => {
                warn!("Task {} was killed", req.task_id);
                // TODO: Handle task cancellation
            }
            _ => {
                debug!("Task {} state updated to {:?}", req.task_id, task_state);
            }
        }

        let response = TaskStatusResponse {
            success: true,
            message: "Task status received".to_string(),
        };

        Ok(Response::new(response))
    }

    /// Get task assignment for executor
    async fn get_task(
        &self,
        request: Request<GetTaskRequest>,
    ) -> Result<Response<GetTaskResponse>, Status> {
        let req = request.into_inner();

        debug!("Executor {} requesting task", req.executor_id);

        // Try to get a task from the scheduler
        if let Some(pending_task) = self.task_scheduler.get_next_task(&req.executor_id).await {
            let task = Task {
                task_id: pending_task.task_id,
                stage_id: pending_task.stage_id,
                partition_index: pending_task.partition_index as u32,
                serialized_rdd: vec![], // TODO: Implement RDD serialization
                serialized_partition: pending_task.task_data,
                properties: HashMap::new(),
            };

            let response = GetTaskResponse {
                has_task: true,
                task: Some(task),
            };

            debug!("Assigned task to executor: {}", req.executor_id);
            Ok(Response::new(response))
        } else {
            // No tasks available
            let response = GetTaskResponse {
                has_task: false,
                task: None,
            };

            Ok(Response::new(response))
        }
    }
}

/// Driver manager for high-level driver operations
#[derive(Clone)]
pub struct Driver {
    service: Arc<DriverServiceImpl>,
    task_scheduler: Arc<TaskScheduler>,
}

impl Driver {
    /// Create a new driver
    pub fn new(driver_id: String) -> Self {
        let service = Arc::new(DriverServiceImpl::new(driver_id));
        let task_scheduler = Arc::clone(&service.task_scheduler);

        Self {
            service,
            task_scheduler,
        }
    }

    /// Start the driver
    pub async fn start(&self, addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
        let service = Arc::clone(&self.service);
        service.start(addr).await
    }

    /// Submit a task for execution
    pub async fn submit_task(
        &self,
        task_id: TaskId,
        stage_id: StageId,
        partition_index: usize,
        task_data: Vec<u8>,
        preferred_executor: Option<ExecutorId>,
    ) {
        self.task_scheduler
            .submit_task(
                task_id,
                stage_id,
                partition_index,
                task_data,
                preferred_executor,
            )
            .await;
    }

    /// Get the number of registered executors
    pub async fn executor_count(&self) -> usize {
        self.task_scheduler.executor_count().await
    }

    /// Get the number of pending tasks
    pub async fn pending_task_count(&self) -> usize {
        self.task_scheduler.pending_task_count().await
    }
}
