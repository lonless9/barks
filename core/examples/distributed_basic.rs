//! Basic Distributed Computing Example
//!
//! This example demonstrates a full, local run of the Driver-Executor architecture.
//! It starts a Driver and an Executor in the same process using tokio tasks,
//! has them communicate over TCP, and runs a simple distributed RDD computation.

use barks_core::distributed::context::{DistributedConfig, DistributedContext, ExecutionMode};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{Level, info};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing to see gRPC and application logs
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    info!("=== Barks Distributed Computing Basic Example ---");

    // --- 1. Define network addresses ---
    let driver_addr: SocketAddr = "127.0.0.1:50071".parse()?;
    let executor_addr: SocketAddr = "127.0.0.1:50072".parse()?;

    // --- 2. Configure and start the Driver ---
    info!("Starting Driver on {}", driver_addr);
    let driver_config = DistributedConfig {
        driver_addr: Some(driver_addr),
        ..Default::default()
    };

    // Create a single driver context and use Arc to share it
    use std::sync::Arc;
    let driver_context = Arc::new(DistributedContext::new_driver(
        "barks-driver-demo".to_string(),
        driver_config.clone(),
    ));

    // Start driver in a background task
    let driver_context_for_spawn = Arc::clone(&driver_context);
    let driver_handle = tokio::spawn({
        async move {
            if let Err(e) = driver_context_for_spawn.start(driver_addr).await {
                eprintln!("Driver failed to start: {}", e);
            }
        }
    });
    // Give the driver a moment to start up
    sleep(Duration::from_millis(100)).await;

    // --- 3. Configure and start the Executor ---
    info!("Starting Executor on {}", executor_addr);
    let executor_config = DistributedConfig {
        driver_addr: Some(driver_addr),
        ..Default::default()
    };
    let executor_context = Arc::new(DistributedContext::new_executor(
        "barks-app".to_string(),
        "executor-1".to_string(),
        executor_addr.ip().to_string(),
        executor_addr.port(),
        executor_config.clone(),
    ));

    // Start executor in a background task
    let executor_context_for_spawn = Arc::clone(&executor_context);
    let executor_handle = tokio::spawn({
        async move {
            if let Err(e) = executor_context_for_spawn.start(executor_addr).await {
                eprintln!("Executor failed to start its server: {}", e);
            }
        }
    });
    // Give the executor a moment to start up its server
    sleep(Duration::from_millis(100)).await;

    // --- 4. Register Executor with Driver ---
    info!("Registering Executor with Driver");
    executor_context
        .register_with_driver(format!("http://{}", driver_addr))
        .await?;

    // Wait to ensure registration is complete and driver is aware
    sleep(Duration::from_millis(500)).await;
    info!(
        "Driver stats: {:?}",
        driver_context.get_driver_stats().await
    );

    // --- 5. Run a Distributed RDD computation ---
    info!("--- Starting distributed RDD computation ---");
    assert_eq!(driver_context.mode(), &ExecutionMode::Driver);

    let data: Vec<i32> = (1..=20).collect();
    info!("Original data (1..20)");

    // Create RDD with 4 partitions
    let rdd = driver_context.parallelize_with_partitions(data, 4);
    info!("Created RDD with {} partitions", rdd.num_partitions());

    // For this simple example, we'll run a 'collect' task
    // The `run` method will distribute this data to the executor
    let result = driver_context.run(rdd).await?;

    info!("--- Computation finished ---");
    info!("Final collected result (should be 1..20): {:?}", result);

    assert_eq!(result.len(), 20);
    assert_eq!(result[0], 1);
    assert_eq!(result[19], 20);

    // --- 6. Shutdown ---
    info!("Example finished successfully. Shutting down.");
    driver_handle.abort();
    executor_handle.abort();

    Ok(())
}
