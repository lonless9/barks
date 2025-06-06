//! Basic Distributed Computing Example
//!
//! This example demonstrates a full, local run of the Driver-Executor architecture.
//! It starts a Driver and an Executor in the same process using tokio tasks, has
//! them communicate over TCP, and runs a true distributed RDD computation using
//! serializable operations.

use barks_core::distributed::context::{DistributedConfig, DistributedContext};
use barks_core::operations::{DoubleOperation, GreaterThanPredicate};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{Level, info};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing to see gRPC and application logs
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    info!("=== Barks Distributed Computing Basic Example ---");

    // --- 1. Define network addresses ---
    let driver_addr: SocketAddr = "127.0.0.1:50081".parse()?;
    let executor_addr: SocketAddr = "127.0.0.1:50082".parse()?;

    // --- 2. Configure and start the Driver ---
    let driver_context_for_server = Arc::new(DistributedContext::new_driver(
        "barks-driver-demo".to_string(),
        DistributedConfig::default(),
    ));
    let driver_handle = tokio::spawn({
        let context = Arc::clone(&driver_context_for_server);
        async move {
            info!("Starting Driver on {}", driver_addr);
            if let Err(e) = context.start(driver_addr).await {
                eprintln!("Driver failed to start: {}", e);
            }
        }
    });
    sleep(Duration::from_millis(100)).await;

    // --- 3. Configure and start the Executor ---
    let executor_context_for_server = Arc::new(DistributedContext::new_executor(
        "barks-app".to_string(),
        "executor-1".to_string(),
        executor_addr.ip().to_string(),
        executor_addr.port(),
        DistributedConfig::default(),
    ));
    let executor_handle = tokio::spawn({
        let executor_context_for_spawn = Arc::clone(&executor_context_for_server);
        async move {
            info!("Starting Executor on {}", executor_addr);
            // Register with driver
            if let Err(e) = executor_context_for_spawn
                .register_with_driver(format!("http://{}", driver_addr))
                .await
            {
                eprintln!("Executor failed to register: {}", e);
                return;
            }
            if let Err(e) = executor_context_for_spawn.start(executor_addr).await {
                eprintln!("Executor failed to start its server: {}", e);
            }
        }
    });
    // Give the executor a moment to start up its server
    sleep(Duration::from_millis(100)).await;

    // --- 4. Run a Distributed RDD computation from the client side ---
    info!("--- Starting distributed RDD computation ---");

    // Let's check if the driver sees the executor
    sleep(Duration::from_millis(500)).await;
    info!(
        "Driver stats: {:?}",
        driver_context_for_server.get_driver_stats().await
    );

    let data: Vec<i32> = (1..=20).collect();
    info!("Original data (1..20)");

    // Create a DistributedI32Rdd with 4 partitions using the server driver context
    let rdd = driver_context_for_server.parallelize_i32_with_partitions(data, 4);
    info!("Created RDD with {} partitions", rdd.num_partitions());

    // Chain serializable operations
    let transformed_rdd = rdd
        .map(Box::new(DoubleOperation)) // Double each number
        .filter(Box::new(GreaterThanPredicate { threshold: 20 })); // Keep results > 20

    // The `run_i32` method will analyze the RDD's lineage, serialize the operations,
    // and send them to the driver for distributed execution.
    let result = driver_context_for_server.run_i32(transformed_rdd).await?;

    info!("--- Computation finished ---");
    let expected_result = vec![22, 24, 26, 28, 30, 32, 34, 36, 38, 40];
    info!("Final collected result: {:?}", result);
    info!("Expected result:      {:?}", expected_result);

    assert_eq!(result, expected_result);

    // --- 5. Shutdown ---
    info!("Example finished successfully. Shutting down.");
    driver_handle.abort();
    executor_handle.abort();

    Ok(())
}
