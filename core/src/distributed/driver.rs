//! Driver implementation for distributed computing
//!
//! The Driver is responsible for coordinating executors, scheduling tasks,
//! and managing the overall distributed computation.

use crate::context::DistributedConfig;
use crate::distributed::proto::driver::{
    driver_service_server::{DriverService, DriverServiceServer},
    ExecutorStatus, HeartbeatRequest, HeartbeatResponse, RegisterExecutorRequest,
    RegisterExecutorResponse, TaskState, TaskStatusRequest, TaskStatusResponse,
};
use crate::distributed::proto::executor::{
    executor_service_client::ExecutorServiceClient, LaunchTaskRequest,
};
use crate::distributed::task::{PendingTask, Task, TaskScheduler};
use crate::distributed::types::{ExecutorId, ExecutorInfo, ExecutorMetrics, StageId, TaskId};
use barks_utils::current_timestamp_secs;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, Mutex};
use tonic::{transport::Server, Request, Response, Status};
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
    /// Distributed configuration
    config: DistributedConfig,
    /// Task scheduler for distributing work
    task_scheduler: Arc<TaskScheduler>,
    /// Registered executors
    executors: Arc<Mutex<HashMap<ExecutorId, RegisteredExecutor>>>,
    /// Tracks which tasks are running on which executor
    executor_tasks: Arc<Mutex<HashMap<ExecutorId, HashSet<TaskId>>>>,
    /// Task statuses
    task_statuses: Arc<Mutex<HashMap<TaskId, TaskState>>>,
    /// Completed task results
    task_results: Arc<Mutex<HashMap<TaskId, TaskResult>>>,
    /// Tasks that have been scheduled but not yet completed. Key is TaskId.
    active_tasks: Arc<Mutex<HashMap<TaskId, PendingTask>>>,
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
    pub fn new(driver_id: String, config: DistributedConfig) -> Self {
        let (heartbeat_sender, heartbeat_receiver) = mpsc::unbounded_channel();

        Self {
            driver_id,
            config,
            task_scheduler: Arc::new(TaskScheduler::new()),
            executors: Arc::new(Mutex::new(HashMap::new())),
            executor_tasks: Arc::new(Mutex::new(HashMap::new())),
            task_statuses: Arc::new(Mutex::new(HashMap::new())),
            task_results: Arc::new(Mutex::new(HashMap::new())),
            active_tasks: Arc::new(Mutex::new(HashMap::new())),
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

        // Spawn liveness check task
        let executors_for_liveness = Arc::clone(&executors);
        let task_scheduler_for_liveness = Arc::clone(&self.task_scheduler);
        let executor_clients_for_liveness = Arc::clone(&self.executor_clients);
        let executor_tasks_for_liveness = Arc::clone(&self.executor_tasks);
        let completion_notifiers_for_liveness = Arc::clone(&self.completion_notifiers);
        let active_tasks_for_liveness = Arc::clone(&self.active_tasks);
        let liveness_timeout = self.config.executor_liveness_timeout_secs;
        let max_retries = self.config.task_max_retries;
        tokio::spawn(async move {
            Self::check_executor_liveness(
                executors_for_liveness,
                task_scheduler_for_liveness,
                active_tasks_for_liveness,
                completion_notifiers_for_liveness,
                executor_tasks_for_liveness,
                executor_clients_for_liveness,
                liveness_timeout,
                max_retries,
            )
            .await;
        });
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

        Ok(())
    }

    /// Start the background task scheduler
    pub async fn start_scheduler(&self) {
        let service = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(500));
            info!("Task scheduler started. Will check for tasks every 500ms.");
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
        let pending_count = self.task_scheduler.pending_task_count().await;
        if pending_count == 0 {
            return Ok(()); // Optimization: Do nothing if no tasks are pending
        }
        let available_executors = self.find_available_executors().await;

        for executor in available_executors {
            // Check available slots for this executor.
            let available_slots = executor
                .info
                .max_concurrent_tasks
                .saturating_sub(executor.metrics.active_tasks);

            let mut executor_failed = false;
            for _ in 0..available_slots {
                if executor_failed {
                    break; // Skip remaining slots for this executor
                }

                // Ask the scheduler for a task, preferring one local to this executor
                if let Some(pending_task) = self
                    .task_scheduler
                    .get_next_task_for_executor(&executor.info.executor_id)
                    .await
                {
                    info!(
                        "Scheduling task {} to executor {}",
                        pending_task.task_id,
                        executor.info.executor_id.clone()
                    );

                    // Add task to active and executor-specific maps BEFORE launching.
                    self.active_tasks
                        .lock()
                        .await
                        .insert(pending_task.task_id.clone(), pending_task.clone());
                    self.executor_tasks
                        .lock()
                        .await
                        .entry(executor.info.executor_id.clone())
                        .or_default()
                        .insert(pending_task.task_id.clone());

                    match Self::launch_task_on_executor(
                        executor.clone(),
                        pending_task.clone(),
                        self.executor_clients.clone(),
                        self.config.max_result_size,
                    )
                    .await
                    {
                        Ok(_) => {
                            // Successfully launched, continue to next slot.
                        }
                        Err(e) => {
                            error!(
                                "Failed to launch task {} on executor {}: {}",
                                pending_task.task_id, executor.info.executor_id, e
                            );
                            // IMPORTANT: If launch fails, clean up state from both active tasks
                            // and the executor's task set to prevent ghost tasks.
                            self.active_tasks.lock().await.remove(&pending_task.task_id);
                            self.executor_tasks
                                .lock()
                                .await
                                .get_mut(&executor.info.executor_id)
                                .map(|tasks| tasks.remove(&pending_task.task_id));

                            // Re-queue the task if launch fails.
                            Self::requeue_or_fail_task(
                                pending_task,
                                self.config.task_max_retries,
                                self.task_scheduler.clone(),
                                self.completion_notifiers.clone(),
                                "launch failure",
                            )
                            .await;

                            // Mark executor as failed for this scheduling round
                            executor_failed = true;
                        }
                    }
                } else {
                    // No more pending tasks, break from inner loop
                    break;
                }
            }
        }

        Ok(())
    }

    /// Helper to requeue a task or mark it as terminally failed.
    async fn requeue_or_fail_task(
        mut task: PendingTask,
        max_retries: u32,
        scheduler: Arc<TaskScheduler>,
        notifiers: Arc<Mutex<HashMap<TaskId, oneshot::Sender<TaskResult>>>>,
        failure_reason: &str,
    ) {
        if task.retries < max_retries {
            warn!(
                "Re-queueing task {} due to {}",
                task.task_id, failure_reason
            );
            task.retries += 1;
            scheduler.submit_pending_task(task).await;
        } else {
            error!(
                "Task {} failed after all retries due to {}.",
                task.task_id, failure_reason
            );
            let result = TaskResult::Failure(format!(
                "Failed to complete task after {} retries",
                max_retries
            ));
            if let Some(notifier) = notifiers.lock().await.remove(&task.task_id) {
                let _ = notifier.send(result);
            }
        }
    }

    /// Finds executors that have capacity to run more tasks.
    /// It filters executors that are not failed and have fewer active tasks than their maximum capacity.
    async fn find_available_executors(&self) -> Vec<RegisteredExecutor> {
        let executors = self.executors.lock().await;
        executors
            .values()
            .filter(|e| {
                e.status != ExecutorStatus::Failed
                    && e.metrics.active_tasks < e.info.max_concurrent_tasks
            })
            .cloned()
            .collect()
    }

    async fn launch_task_on_executor(
        executor: RegisteredExecutor,
        pending_task: PendingTask,
        executor_clients: Arc<
            Mutex<HashMap<ExecutorId, ExecutorServiceClient<tonic::transport::Channel>>>,
        >,
        max_result_size: usize,
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
            serialized_task: pending_task.serialized_task,
            properties: HashMap::new(),
            max_result_size_bytes: max_result_size as u64,
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

    /// Periodically checks for dead executors and removes them.
    async fn check_executor_liveness(
        executors: Arc<Mutex<HashMap<ExecutorId, RegisteredExecutor>>>,
        task_scheduler: Arc<TaskScheduler>,
        active_tasks: Arc<Mutex<HashMap<TaskId, PendingTask>>>,
        completion_notifiers: Arc<Mutex<HashMap<TaskId, oneshot::Sender<TaskResult>>>>,
        executor_tasks: Arc<Mutex<HashMap<ExecutorId, HashSet<TaskId>>>>,
        executor_clients: Arc<
            Mutex<HashMap<ExecutorId, ExecutorServiceClient<tonic::transport::Channel>>>,
        >,
        timeout_secs: u64,
        max_retries: u32,
    ) {
        let mut interval = tokio::time::interval(Duration::from_secs(timeout_secs / 2));
        info!(
            "Executor liveness check started. Timeout: {}s",
            timeout_secs
        );
        loop {
            interval.tick().await;
            let now = Self::current_timestamp();
            let mut dead_executors = Vec::new();

            let executors_guard = executors.lock().await;
            for (id, executor) in executors_guard.iter() {
                // Only check active executors
                if executor.status != ExecutorStatus::Failed
                    && now.saturating_sub(executor.last_heartbeat) > timeout_secs
                {
                    warn!(
                        "Executor {} timed out. Last heartbeat was {} seconds ago.",
                        id,
                        now - executor.last_heartbeat
                    );
                    dead_executors.push(id.clone());
                }
            }
            drop(executors_guard); // Release lock

            if !dead_executors.is_empty() {
                warn!("Removing dead executors: {:?}", dead_executors);
                let mut executors_guard = executors.lock().await;
                let mut clients_guard = executor_clients.lock().await;
                let mut executor_tasks_guard = executor_tasks.lock().await;

                for id in &dead_executors {
                    if let Some(executor) = executors_guard.get_mut(id) {
                        executor.status = ExecutorStatus::Failed;
                    }
                    // Unregister from scheduler to prevent future scheduling attempts
                    task_scheduler.unregister_executor(id).await;
                    clients_guard.remove(id);

                    // Re-queue tasks that were running on the dead executor
                    if let Some(tasks_to_requeue) = executor_tasks_guard.remove(id) {
                        let task_scheduler_clone = task_scheduler.clone();
                        let active_tasks_clone = active_tasks.clone();
                        let completion_notifiers_clone = completion_notifiers.clone();
                        let executor_id_clone = id.clone();

                        tokio::spawn(async move {
                            Self::handle_failed_tasks(
                                tasks_to_requeue,
                                &executor_id_clone,
                                task_scheduler_clone,
                                active_tasks_clone,
                                completion_notifiers_clone,
                                max_retries,
                            )
                            .await;
                        });
                    }
                }
            }
        }
    }

    /// Get current timestamp
    fn current_timestamp() -> u64 {
        current_timestamp_secs()
    }

    /// Convert protobuf ExecutorStatus to internal type
    fn convert_executor_status(status: i32) -> ExecutorStatus {
        ExecutorStatus::try_from(status).unwrap_or(ExecutorStatus::Failed)
    }

    /// Convert protobuf TaskState to internal type
    fn convert_task_state(state: i32) -> TaskState {
        TaskState::try_from(state).unwrap_or(TaskState::TaskFailed)
    }

    /// Helper function to handle failed tasks - used by both liveness check and re-registration
    async fn handle_failed_tasks(
        tasks_to_requeue: HashSet<TaskId>,
        executor_id: &ExecutorId,
        task_scheduler: Arc<TaskScheduler>,
        active_tasks: Arc<Mutex<HashMap<TaskId, PendingTask>>>,
        completion_notifiers: Arc<Mutex<HashMap<TaskId, oneshot::Sender<TaskResult>>>>,
        max_retries: u32,
    ) {
        warn!(
            "Re-queueing {} tasks from failed/stale executor {}",
            tasks_to_requeue.len(),
            executor_id
        );
        for task_id in tasks_to_requeue {
            if let Some(mut pending_task) = active_tasks.lock().await.remove(&task_id) {
                if pending_task.retries < max_retries {
                    pending_task.retries += 1;
                    warn!(
                        "Re-queueing task {} (attempt {}/{})",
                        task_id, pending_task.retries, max_retries
                    );
                    task_scheduler.submit_pending_task(pending_task).await;
                } else {
                    error!(
                        "Task {} on executor {} failed after all retries.",
                        task_id, executor_id
                    );
                    let result = TaskResult::Failure(format!(
                        "Task failed on executor {} after {} retries",
                        executor_id, max_retries
                    ));
                    if let Some(notifier) = completion_notifiers.lock().await.remove(&task_id) {
                        let _ = notifier.send(result);
                    }
                }
            }
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
        let executor_id = req.executor_id.clone();

        // --- Start: Handle re-registration robustly by cleaning up all old state ---
        {
            let mut executors_guard = self.executors.lock().await;
            if executors_guard.contains_key(&executor_id) {
                warn!(
                    "Executor {} is re-registering. Decommissioning old instance.",
                    executor_id
                );

                // Re-queue tasks from the old instance.
                let mut executor_tasks_guard = self.executor_tasks.lock().await;
                if let Some(tasks_to_requeue) = executor_tasks_guard.remove(&executor_id) {
                    let task_scheduler = self.task_scheduler.clone();
                    let active_tasks = self.active_tasks.clone();
                    let max_retries = self.config.task_max_retries;
                    let notifiers = self.completion_notifiers.clone();
                    let executor_id_clone = executor_id.clone();

                    tokio::spawn(async move {
                        Self::handle_failed_tasks(
                            tasks_to_requeue,
                            &executor_id_clone,
                            task_scheduler,
                            active_tasks,
                            notifiers,
                            max_retries,
                        )
                        .await;
                    });
                }
                // Remove the old executor entry completely to ensure a clean state.
                executors_guard.remove(&executor_id);
            }
        }
        // --- End: Handle re-registration ---

        // CRITICAL FIX: Invalidate any existing client for this executor ID.
        // This forces re-connection if the executor is re-registering with a new address.
        if self
            .executor_clients
            .lock()
            .await
            .remove(&executor_id)
            .is_some()
        {
            warn!(
                "Removed stale client for re-registering executor: {}",
                executor_id
            );
        }

        info!(
            "Registering new executor: id={}, addr={}:{}",
            req.executor_id, req.host, req.port
        );

        let mut executor_info = ExecutorInfo::new(
            req.executor_id.clone(),
            req.host,
            req.port as u16,
            req.cores,
            req.memory_mb,
        )
        .with_attributes(req.attributes);
        executor_info.max_concurrent_tasks = req.max_concurrent_tasks;

        let registered_executor = RegisteredExecutor {
            info: executor_info.clone(),
            last_heartbeat: Self::current_timestamp(),
            status: ExecutorStatus::Starting,
            metrics: ExecutorMetrics::default(),
        };

        // Register with task scheduler
        self.task_scheduler.register_executor(executor_info).await;

        // Add to executors map
        self.executors
            .lock()
            .await
            .insert(req.executor_id.clone(), registered_executor);
        self.executor_tasks
            .lock()
            .await
            .entry(req.executor_id.clone())
            .or_default();

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
        statuses.insert(req.task_id.clone(), task_state.clone());

        // Remove from the executor's running task set upon completion/failure
        {
            let mut executor_tasks_guard = self.executor_tasks.lock().await;
            if let Some(tasks) = executor_tasks_guard.get_mut(&req.executor_id) {
                tasks.remove(&req.task_id);
            }
        }

        match task_state {
            TaskState::TaskFinished => {
                info!("Task {} completed successfully", req.task_id);
                let result = TaskResult::Success(req.result);

                // Remove from active tasks on success.
                self.active_tasks.lock().await.remove(&req.task_id);
                self.task_results
                    .lock()
                    .await
                    .insert(req.task_id.clone(), result.clone());

                // Notify any waiters.
                if let Some(notifier) = self.completion_notifiers.lock().await.remove(&req.task_id)
                {
                    let _ = notifier.send(result);
                }
            }

            TaskState::TaskFailed => {
                error!(
                    "Task {} failed on executor {}: {}",
                    req.task_id, req.executor_id, req.error_message
                );

                let task_scheduler = self.task_scheduler.clone();
                let active_tasks = self.active_tasks.clone();
                let max_retries = self.config.task_max_retries;
                let notifiers = self.completion_notifiers.clone();
                let error_message = req.error_message.clone();

                // IMPORTANT: Remove task from active map before re-queueing to prevent duplicates.
                let pending_task_opt = {
                    let mut active_tasks_guard = active_tasks.lock().await;
                    active_tasks_guard.remove(&req.task_id)
                };

                if let Some(mut pending_task) = pending_task_opt {
                    if pending_task.retries < max_retries {
                        pending_task.retries += 1;
                        warn!(
                            "Re-queueing task {} for retry (attempt {}/{})",
                            req.task_id, pending_task.retries, max_retries
                        );
                        task_scheduler.submit_pending_task(pending_task).await;
                    } else {
                        // Task has exhausted retries, notify of final failure.
                        warn!("Task {} failed after all retries.", req.task_id);
                        // The task has already been removed from active_tasks.
                        let result = TaskResult::Failure(error_message);
                        if let Some(notifier) = notifiers.lock().await.remove(&req.task_id) {
                            let _ = notifier.send(result);
                        }
                    }
                } else {
                    // This can happen if the task was already retried due to executor timeout
                    // and this is a late failure report.
                    warn!(
                        "Received failure report for task {}, but it was not in the active task map.",
                        req.task_id
                    );
                }
            }

            TaskState::TaskKilled => {
                warn!("Task {} was killed", req.task_id);
                self.active_tasks.lock().await.remove(&req.task_id);
                let result = TaskResult::Failure("Task was killed".to_string());
                if let Some(notifier) = self.completion_notifiers.lock().await.remove(&req.task_id)
                {
                    let _ = notifier.send(result);
                }
            }

            // For Pending/Running, just update status and do nothing else.
            _ => {
                debug!("Task {} state updated to {:?}", req.task_id, task_state);
            }
        };

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
    pub fn new(driver_id: String, config: DistributedConfig) -> Self {
        let service = Arc::new(DriverServiceImpl::new(driver_id, config));
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
                retries: 0,
            })
            .await;

        Ok(receiver)
    }

    /// Get the number of registered executors
    pub async fn executor_count(&self) -> usize {
        self.task_scheduler.executor_count().await
    }

    /// Get IDs of all registered executors
    pub async fn get_executor_ids(&self) -> Vec<ExecutorId> {
        self.task_scheduler.list_executor_ids().await
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

    /// Get a copy of a pending task by its ID.
    /// This is inefficient and primarily for re-queueing logic.
    /// In a more advanced scheduler, tasks would be looked up from a HashMap.
    pub async fn get_pending_task_by_id(&self, task_id: &TaskId) -> Option<PendingTask> {
        // This now correctly checks the driver's active task map.
        let active_tasks = self.service.active_tasks.lock().await;
        active_tasks.get(task_id).cloned()
    }
}
