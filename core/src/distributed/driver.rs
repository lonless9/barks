//! Driver implementation for distributed computing
//!
//! The Driver is responsible for coordinating executors, scheduling tasks,
//! and managing the overall distributed computation.

use crate::distributed::proto::driver::{
    HeartbeatRequest, HeartbeatResponse, RegisterExecutorRequest, RegisterExecutorResponse,
    TaskStatusRequest, TaskStatusResponse,
    driver_service_server::{DriverService, DriverServiceServer},
};
use crate::distributed::proto::executor::{
    LaunchTaskRequest, executor_service_client::ExecutorServiceClient,
};
use crate::distributed::task::{PendingTask, Task, TaskScheduler};
use crate::distributed::types::{
    ExecutorId, ExecutorInfo, ExecutorMetrics, ExecutorStatus, StageId, TaskId, TaskState,
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, mpsc, oneshot};
use tonic::{Request, Response, Status, transport::Server};
use tracing::{debug, error, info, warn};

/// Result of a completed task.
#[derive(Debug, Clone)]
pub enum TaskResult {
    Success(Vec<u8>),
    Failure(String),
}

/// Driver service implementation
#[derive(Clone)]
pub struct DriverServiceImpl {
    /// Unique driver identifier
    driver_id: String,
    /// Task scheduler for distributing work
    task_scheduler: Arc<TaskScheduler>,
    /// Registered executors
    executors: Arc<Mutex<HashMap<ExecutorId, RegisteredExecutor>>>,
    /// Task statuses
    task_statuses: Arc<Mutex<HashMap<TaskId, TaskState>>>,
    /// Completed task results
    task_results: Arc<Mutex<HashMap<TaskId, TaskResult>>>,
    /// Notifiers for waiting tasks
    completion_notifiers: Arc<Mutex<HashMap<TaskId, oneshot::Sender<TaskResult>>>>,
    /// Executor clients cache
    executor_clients:
        Arc<Mutex<HashMap<ExecutorId, ExecutorServiceClient<tonic::transport::Channel>>>>,
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
            task_statuses: Arc::new(Mutex::new(HashMap::new())),
            task_results: Arc::new(Mutex::new(HashMap::new())),
            completion_notifiers: Arc::new(Mutex::new(HashMap::new())),
            executor_clients: Arc::new(Mutex::new(HashMap::new())),
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

        // Start background task scheduler
        self.start_scheduler().await;

        // Start gRPC server in background
        let service = self.clone();
        tokio::spawn(async move {
            if let Err(e) = Server::builder()
                .add_service(DriverServiceServer::new(service))
                .serve(addr)
                .await
            {
                error!("Driver gRPC server failed: {}", e);
            }
        });

        // Give the server a moment to start
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        Ok(())
    }

    /// Start the background task scheduler
    pub async fn start_scheduler(&self) {
        let service = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                if let Err(e) = service.schedule_tasks().await {
                    error!("Error during task scheduling: {}", e);
                }
            }
        });
    }

    /// Schedule pending tasks to available executors
    pub async fn schedule_tasks(&self) -> Result<(), anyhow::Error> {
        let idle_executors = self.get_idle_executors().await;

        for executor in idle_executors {
            if let Some(pending_task) = self.task_scheduler.get_next_task().await {
                info!(
                    "Attempting to schedule task {} to executor {}",
                    pending_task.task_id,
                    executor.info.executor_id.clone()
                );

                let executor_id = executor.info.executor_id.clone();
                let task_id = pending_task.task_id.clone();
                let executor_clients = self.executor_clients.clone();
                let task_scheduler = self.task_scheduler.clone();

                // Launch task on executor
                tokio::spawn(async move {
                    if let Err(e) = Self::launch_task_on_executor(
                        executor,
                        pending_task.clone(),
                        executor_clients,
                    )
                    .await
                    {
                        error!(
                            "Failed to launch task {} on executor {}: {}",
                            task_id, executor_id, e
                        );
                        // Re-queue the task if launch fails
                        warn!("Re-queueing task {}", pending_task.task_id);
                        task_scheduler.submit_pending_task(pending_task).await;
                    }
                });
            }
        }

        Ok(())
    }

    async fn get_idle_executors(&self) -> Vec<RegisteredExecutor> {
        self.executors
            .lock()
            .await
            .values()
            .filter(|e| e.status == ExecutorStatus::Idle || e.status == ExecutorStatus::Running)
            .cloned()
            .collect()
    }

    /// Gets or creates a gRPC client for an executor.
    async fn get_or_create_executor_client(
        &self,
        executor: &RegisteredExecutor,
    ) -> Result<ExecutorServiceClient<tonic::transport::Channel>, anyhow::Error> {
        let executor_id = &executor.info.executor_id;
        // Lock the client cache to prevent race conditions during client creation.
        let mut clients_guard = self.executor_clients.lock().await;

        // If client already exists, clone and return it.
        if let Some(client) = clients_guard.get(executor_id) {
            return Ok(client.clone());
        }

        // Client not found, create a new one.
        let executor_addr = format!("http://{}:{}", executor.info.host, executor.info.port);
        info!(
            "Connecting to executor {} at {}",
            executor_id, executor_addr
        );
        let client = ExecutorServiceClient::connect(executor_addr).await?;
        clients_guard.insert(executor_id.clone(), client.clone());

        Ok(client)
    }

    async fn launch_task_on_executor(
        executor: RegisteredExecutor,
        pending_task: PendingTask,
        executor_clients: Arc<
            Mutex<HashMap<ExecutorId, ExecutorServiceClient<tonic::transport::Channel>>>,
        >,
    ) -> Result<(), anyhow::Error> {
        let mut client =
            Self::get_or_create_executor_client_static(&executor, executor_clients).await?;

        let task_id = pending_task.task_id.clone();
        info!(
            "Launching task {} on executor {}",
            task_id, executor.info.executor_id
        );

        let request = LaunchTaskRequest {
            task_id: task_id.clone(),
            stage_id: pending_task.stage_id,
            partition_index: pending_task.partition_index as u32,
            // The entire task, including data and operation, is serialized into this field.
            serialized_task: pending_task.serialized_task,
            properties: HashMap::new(),
            max_result_size_bytes: 1024 * 1024 * 128, // 128MB default limit
            ..Default::default()
        };

        client
            .launch_task(request)
            .await
            .map_err(|e| anyhow::anyhow!("RPC call to LaunchTask failed: {}", e))?;

        info!(
            "Successfully requested executor {} to launch task {}",
            executor.info.executor_id, task_id
        );
        Ok(())
    }

    // Helper function to create client to avoid self parameter issues in spawn
    async fn get_or_create_executor_client_static(
        executor: &RegisteredExecutor,
        clients: Arc<Mutex<HashMap<ExecutorId, ExecutorServiceClient<tonic::transport::Channel>>>>,
    ) -> Result<ExecutorServiceClient<tonic::transport::Channel>, anyhow::Error> {
        let mut clients_guard = clients.lock().await;
        let executor_id = &executor.info.executor_id;

        if let Some(client) = clients_guard.get(executor_id) {
            return Ok(client.clone());
        }

        let executor_addr = format!("http://{}:{}", executor.info.host, executor.info.port);
        let client = ExecutorServiceClient::connect(executor_addr).await?;
        clients_guard.insert(executor_id.clone(), client.clone());
        Ok(client)
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
            0 /* TASK_PENDING */ => TaskState::Pending,
            1 /* TASK_RUNNING */ => TaskState::Running,
            2 /* TASK_FINISHED */ => TaskState::Finished,
            3 /* TASK_FAILED */ => TaskState::Failed,
            4 /* TASK_KILLED */ => TaskState::Killed,
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
                active_tasks: proto_metrics.active_tasks,
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

        let mut statuses = self.task_statuses.lock().await;
        let task_state = Self::convert_task_state(req.state);
        statuses.insert(req.task_id.clone(), task_state);

        let result = match task_state {
            TaskState::Finished => {
                info!("Task {} completed successfully", req.task_id);
                TaskResult::Success(req.result)
            }
            TaskState::Failed => {
                error!("Task {} failed: {}", req.task_id, req.error_message);
                TaskResult::Failure(req.error_message)
            }
            TaskState::Killed => {
                warn!("Task {} was killed", req.task_id);
                TaskResult::Failure("Task was killed".to_string())
            }
            _ => {
                debug!("Task {} state updated to {:?}", req.task_id, task_state);
                return Ok(Response::new(TaskStatusResponse {
                    success: true,
                    message: "Status updated".to_string(),
                }));
            }
        };

        // Store result and notify any waiters
        self.task_results
            .lock()
            .await
            .insert(req.task_id.clone(), result.clone());
        if let Some(notifier) = self.completion_notifiers.lock().await.remove(&req.task_id) {
            let _ = notifier.send(result);
        }

        let response = TaskStatusResponse {
            success: true,
            message: "Task status received".to_string(),
        };

        Ok(Response::new(response))
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
    /// Takes a serializable `Task` trait object.
    pub async fn submit_task(
        &self,
        task_id: TaskId,
        stage_id: StageId,
        partition_index: usize,
        task: Box<dyn Task>,
        preferred_executor: Option<ExecutorId>,
    ) -> Result<oneshot::Receiver<TaskResult>, anyhow::Error> {
        // Serialize the task object using `serde_json` because `typetag` works well with it.
        // The inner data (`partition_data`) is still efficiently serialized with bincode.
        let serialized_task = serde_json::to_vec(&task)
            .map_err(|e| anyhow::anyhow!("Failed to serialize task object: {}", e))?;

        let (sender, receiver) = oneshot::channel();
        self.service
            .completion_notifiers
            .lock()
            .await
            .insert(task_id.clone(), sender);

        self.task_scheduler
            .submit_pending_task(PendingTask {
                task_id,
                stage_id,
                partition_index,
                serialized_task,
                preferred_executor,
            })
            .await;

        Ok(receiver)
    }

    /// Get the number of registered executors
    pub async fn executor_count(&self) -> usize {
        self.task_scheduler.executor_count().await
    }

    /// Get the number of pending tasks
    pub async fn pending_task_count(&self) -> usize {
        self.task_scheduler.pending_task_count().await
    }

    /// Collect results from all executors for a stage
    pub async fn collect_stage_results(
        &self,
        stage_id: &StageId,
    ) -> Result<Vec<Vec<u8>>, anyhow::Error> {
        // This is a simplified implementation
        // In a real system, this would track task completion and collect results
        info!("Collecting results for stage: {}", stage_id);

        // For now, return empty results
        // TODO: Implement proper result collection
        Ok(Vec::new())
    }
}
