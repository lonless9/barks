//! Basic Distributed Computing Example
//!
//! This example demonstrates the core concepts of the distributed
//! Driver-Executor architecture without requiring full gRPC setup.

use barks_core::distributed::{
    context::{DistributedConfig, DistributedContext},
    driver::{RddOperation, TaskData},
    task::TaskScheduler,
    types::{ExecutorInfo, ExecutorStatus, TaskMetrics},
};
use std::collections::HashMap;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    info!("=== Barks Distributed Computing Basic Example ===");

    // Demonstrate the core distributed concepts
    demonstrate_task_scheduling().await?;
    demonstrate_executor_management().await?;
    demonstrate_distributed_context().await?;

    Ok(())
}

/// Demonstrate task scheduling functionality
async fn demonstrate_task_scheduling() -> Result<(), Box<dyn std::error::Error>> {
    info!("\n--- Task Scheduling Demo ---");

    let scheduler = TaskScheduler::new();

    // Register some executors
    let executor1 = ExecutorInfo::new(
        "executor-1".to_string(),
        "127.0.0.1".to_string(),
        8081,
        4,
        2048,
    );

    let executor2 = ExecutorInfo::new(
        "executor-2".to_string(),
        "127.0.0.1".to_string(),
        8082,
        8,
        4096,
    );

    scheduler.register_executor(executor1).await;
    scheduler.register_executor(executor2).await;

    info!("Registered {} executors", scheduler.executor_count().await);

    // Submit some tasks
    for i in 0..5 {
        // Create a task with some data and an operation
        let partition_data: Vec<i32> = vec![i as i32, i as i32 + 1];
        let task = TaskData {
            partition_data: bincode::encode_to_vec(&partition_data, bincode::config::standard())?,
            operation: RddOperation::Collect,
        };

        // The actual payload for the scheduler is the serialized `TaskData`
        let task_data = bincode::encode_to_vec(&task, bincode::config::standard())?;

        scheduler
            .submit_task(
                format!("task-{}", i),
                "stage-1".to_string(),
                i,
                task_data,
                None,
            )
            .await;
    }

    info!("Submitted {} tasks", scheduler.pending_task_count().await);

    // Simulate task assignment
    for executor_id in ["executor-1", "executor-2"] {
        while let Some(task) = scheduler.get_next_task().await {
            info!("Assigned task {} to {}", task.task_id, executor_id);
        }
    }

    info!(
        "Remaining pending tasks: {}",
        scheduler.pending_task_count().await
    );

    Ok(())
}

/// Demonstrate executor management
async fn demonstrate_executor_management() -> Result<(), Box<dyn std::error::Error>> {
    info!("\n--- Executor Management Demo ---");

    // Create executor info
    let executor_info = ExecutorInfo::new(
        "demo-executor".to_string(),
        "127.0.0.1".to_string(),
        8083,
        4,
        2048,
    )
    .with_attributes({
        let mut attrs = HashMap::new();
        attrs.insert("zone".to_string(), "us-west-1".to_string());
        attrs.insert("instance_type".to_string(), "m5.large".to_string());
        attrs
    });

    info!("Executor Info: {:?}", executor_info);

    // Demonstrate status transitions
    let mut status = ExecutorStatus::Starting;
    info!("Initial status: {:?}", status);

    status = ExecutorStatus::Running;
    info!("Status after startup: {:?}", status);

    status = ExecutorStatus::Busy;
    info!("Status during task execution: {:?}", status);

    status = ExecutorStatus::Idle;
    info!("Status after task completion: {:?}", status);

    // Demonstrate metrics
    let mut metrics = TaskMetrics::default();
    metrics.executor_run_time_ms = 1500;
    metrics.result_size_bytes = 1024;
    metrics.peak_execution_memory_bytes = 512 * 1024 * 1024; // 512MB

    info!("Task Metrics: {:?}", metrics);

    Ok(())
}

/// Demonstrate distributed context usage
async fn demonstrate_distributed_context() -> Result<(), Box<dyn std::error::Error>> {
    info!("\n--- Distributed Context Demo ---");

    // Create a local context for demonstration
    let context = DistributedContext::new_local("barks-demo".to_string());

    info!(
        "Created context: {} in mode: {:?}",
        context.app_name(),
        context.mode()
    );

    // Create some test data
    let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    info!("Original data: {:?}", data);

    // Create RDD with multiple partitions
    let rdd = context.parallelize_with_partitions(data, 3);
    info!("Created RDD with {} partitions", rdd.num_partitions());

    // Apply transformations
    let mapped_rdd = rdd.map(|x| x * 2);
    let filtered_rdd = mapped_rdd.filter(|&x| x > 10);

    // Execute and collect results
    let result = context.run(filtered_rdd).await?;
    info!("Final result: {:?}", result);

    // Demonstrate driver configuration
    let driver_config = DistributedConfig::default();
    info!("Default driver config: {:?}", driver_config);

    let driver_context =
        DistributedContext::new_driver("barks-driver-demo".to_string(), driver_config);

    info!(
        "Created driver context: {} in mode: {:?}",
        driver_context.app_name(),
        driver_context.mode()
    );

    Ok(())
}

/// Demonstrate task execution simulation
async fn demonstrate_task_execution() -> Result<(), Box<dyn std::error::Error>> {
    info!("\n--- Task Execution Demo ---");

    // Create a task with some data and an operation
    let partition_data: Vec<i32> = (0..1000).collect();
    let task_data_payload = TaskData {
        partition_data: bincode::encode_to_vec(&partition_data, bincode::config::standard())?,
        operation: RddOperation::Map {
            closure_data: vec![], // Placeholder
        },
    };
    info!(
        "Created task with operation: {:?}",
        task_data_payload.operation
    );

    // Serialize the task
    let task_data_bytes = bincode::encode_to_vec(&task_data_payload, bincode::config::standard())?;
    info!("Serialized task size: {} bytes", task_data_bytes.len());

    // Deserialize the task (as an executor would)
    let (deserialized_task, _): (TaskData, _) =
        bincode::decode_from_slice(&task_data_bytes, bincode::config::standard())?;
    info!("Deserialized task successfully.");

    // Simulate task execution
    use rayon::prelude::*;
    let start_time = std::time::Instant::now();

    // Simplified execution logic matching the Map operation
    let input_data: (Vec<i32>, _) = bincode::decode_from_slice(
        &deserialized_task.partition_data,
        bincode::config::standard(),
    )?;
    let result: Vec<i32> = input_data.0.into_par_iter().map(|i| i * 2).collect();

    let execution_time = start_time.elapsed();

    info!(
        "Task executed in {:?}, produced {} results",
        execution_time,
        result.len()
    );
    info!(
        "Sample results: {:?}",
        &result[..std::cmp::min(10, result.len())]
    );

    // Create task metrics
    let metrics = TaskMetrics {
        executor_run_time_ms: execution_time.as_millis() as u64,
        result_size_bytes: (result.len() * std::mem::size_of::<i32>()) as u64,
        peak_execution_memory_bytes: 1024 * 1024, // 1MB estimate
        ..Default::default()
    };

    info!("Task metrics: {:?}", metrics);

    Ok(())
}

/// Demonstrate serialization and communication patterns
async fn demonstrate_serialization() -> Result<(), Box<dyn std::error::Error>> {
    info!("\n--- Serialization Demo ---");

    // Test different data types
    let test_data: Vec<(&str, Vec<i32>)> = vec![
        ("integers", vec![1, 2, 3, 4, 5]),
        ("more_integers", vec![10, 20, 30, 40, 50]),
    ];

    for (name, data) in test_data {
        let serialized = bincode::encode_to_vec(&data, bincode::config::standard())?;
        info!("{} serialized to {} bytes", name, serialized.len());

        let (deserialized, _): (Vec<i32>, _) =
            bincode::decode_from_slice(&serialized, bincode::config::standard())?;

        info!("{} deserialized successfully: {:?}", name, deserialized);
    }

    Ok(())
}
