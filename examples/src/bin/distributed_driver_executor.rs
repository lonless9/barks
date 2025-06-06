//! Comprehensive example demonstrating the Driver-Executor distributed architecture
//!
//! This example shows how to:
//! 1. Start a Driver service that coordinates task execution
//! 2. Start multiple Executor services that register with the Driver
//! 3. Submit tasks for distributed execution
//! 4. Handle task results and monitoring
//!
//! Run this example with:
//! ```bash
//! cargo run --example distributed_driver_executor
//! ```

use barks_core::distributed::{
    Driver, Executor, ExecutorInfo, RddOperationType, RddTaskInfo, SimpleTaskInfo,
};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    info!("Starting distributed Driver-Executor example");

    // Start the driver in a separate task
    let driver_addr: SocketAddr = "127.0.0.1:50051".parse()?;
    let driver = Driver::new("driver-001".to_string());

    let _driver_handle = {
        let driver = driver.clone();
        tokio::spawn(async move {
            if let Err(e) = driver.start(driver_addr).await {
                error!("Driver failed to start: {}", e);
            }
        })
    };

    // Give the driver time to start
    sleep(Duration::from_secs(2)).await;

    // Start multiple executors
    let executor_count = 3;
    let mut executor_handles = Vec::new();

    for i in 0..executor_count {
        let executor_id = format!("executor-{:03}", i);
        let executor_addr: SocketAddr = format!("127.0.0.1:{}", 50052 + i).parse()?;
        let driver_url = format!("http://{}", driver_addr);

        let executor_info = ExecutorInfo::new(
            executor_id.clone(),
            "127.0.0.1".to_string(),
            executor_addr.port(),
            4,    // 4 cores
            8192, // 8GB memory
        );

        let mut executor = Executor::new(executor_info, 4); // max 4 concurrent tasks

        // Register with driver
        if let Err(e) = executor.register_with_driver(driver_url).await {
            error!("Failed to register executor {}: {}", executor_id, e);
            continue;
        }

        // Start executor service
        let executor_service_handle = {
            let executor = executor.clone();
            let executor_id_clone = executor_id.clone();
            tokio::spawn(async move {
                if let Err(e) = executor.start(executor_addr).await {
                    error!("Executor {} failed to start: {}", executor_id_clone, e);
                }
            })
        };

        // Start heartbeat
        let executor_id_clone2 = executor_id.clone();
        let heartbeat_handle = tokio::spawn(async move {
            if let Err(e) = executor.start_heartbeat().await {
                error!(
                    "Failed to start heartbeat for {}: {}",
                    executor_id_clone2, e
                );
            }
        });

        executor_handles.push((executor_service_handle, heartbeat_handle));
        info!("Started executor: {}", executor_id);
    }

    // Give executors time to register and start heartbeats
    sleep(Duration::from_secs(3)).await;

    // Submit some tasks for execution
    info!("Submitting tasks for distributed execution");

    for i in 0..10 {
        let task_id = format!("task-{:04}", i);
        let stage_id = format!("stage-{}", i / 3); // Group tasks into stages

        // Create a simple task
        let task_info = SimpleTaskInfo::new(
            task_id.clone(),
            stage_id.clone(),
            i,
            1000 + i * 100, // varying data sizes
        );

        // Serialize the task
        let task_data = bincode::encode_to_vec(&task_info, bincode::config::standard())
            .map_err(|e| format!("Failed to serialize task: {}", e))?;

        // Submit to driver
        driver
            .submit_task(
                task_id.clone(),
                stage_id,
                i,
                task_data,
                None, // No preferred executor
            )
            .await;

        info!("Submitted task: {}", task_id);
    }

    // Submit some RDD tasks as well
    info!("Submitting RDD tasks for distributed execution");

    for i in 0..5 {
        let task_id = format!("rdd-task-{:04}", i);
        let stage_id = format!("rdd-stage-{}", i);

        // Create sample partition data
        let partition_data: Vec<i32> = (0..100).map(|x| x * (i + 1) as i32).collect();
        let serialized_data = bincode::encode_to_vec(&partition_data, bincode::config::standard())
            .map_err(|e| format!("Failed to serialize partition data: {}", e))?;

        // Create an RDD task
        let rdd_task = RddTaskInfo {
            task_id: task_id.clone(),
            stage_id: stage_id.clone(),
            partition_index: i,
            serialized_partition_data: serialized_data,
            operation_type: match i % 3 {
                0 => RddOperationType::Map {
                    function_id: "double".to_string(),
                },
                1 => RddOperationType::Filter {
                    predicate_id: "even".to_string(),
                },
                _ => RddOperationType::Collect,
            },
        };

        // Serialize the RDD task
        let task_data = bincode::encode_to_vec(&rdd_task, bincode::config::standard())
            .map_err(|e| format!("Failed to serialize RDD task: {}", e))?;

        // Submit to driver
        driver
            .submit_task(
                task_id.clone(),
                stage_id,
                i,
                task_data,
                None, // No preferred executor
            )
            .await;

        info!(
            "Submitted RDD task: {} with operation: {:?}",
            task_id, rdd_task.operation_type
        );
    }

    // Monitor the system for a while
    info!("Monitoring distributed execution...");

    for _ in 0..30 {
        sleep(Duration::from_secs(2)).await;

        let executor_count = driver.executor_count().await;
        let pending_tasks = driver.pending_task_count().await;

        info!(
            "System status - Executors: {}, Pending tasks: {}",
            executor_count, pending_tasks
        );

        if pending_tasks == 0 {
            info!("All tasks completed!");
            break;
        }
    }

    info!("Shutting down distributed system...");

    // Cleanup: In a real implementation, we would gracefully shutdown executors
    // and wait for all tasks to complete

    Ok(())
}
