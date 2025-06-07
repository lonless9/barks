//! Basic Distributed Computing Example
//!
//! This example demonstrates a full, local run of the Driver-Executor architecture.
//! It starts a Driver and an Executor in the same process using tokio tasks, has
//! them communicate over TCP, and runs a true distributed RDD computation using
//! serializable operations.

use barks_core::operations::{DoubleOperation, GreaterThanPredicate};
use barks_core::{DistributedConfig, DistributedContext};
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
    let driver_addr_str = "127.0.0.1:50081";
    let executor_addr_str = "127.0.0.1:50082";
    let driver_addr: SocketAddr = driver_addr_str.parse()?;
    let executor_addr: SocketAddr = executor_addr_str.parse()?;

    // --- 2. Configure and start the Driver ---
    // Create a context that will run the driver's gRPC server
    let driver_server_context = Arc::new(DistributedContext::new_driver(
        "barks-driver-demo".to_string(),
        DistributedConfig::default(),
    ));
    let driver_handle = tokio::spawn({
        let context = Arc::clone(&driver_server_context);
        async move {
            info!("Starting Driver on {}", driver_addr);
            if let Err(e) = context.start(driver_addr).await {
                eprintln!("Driver failed to start: {}", e);
            }
        }
    });
    sleep(Duration::from_millis(200)).await; // Give driver time to start

    // --- 3. Configure and start the Executor ---
    let executor_handle = tokio::spawn({
        async move {
            let executor_context = DistributedContext::new_executor(
                "barks-app".to_string(),
                "executor-1".to_string(),
                executor_addr.ip().to_string(),
                executor_addr.port(),
                DistributedConfig::default(),
            );
            info!("Starting Executor on {}", executor_addr);
            // Register with driver
            if let Err(e) = executor_context
                .register_with_driver(format!("http://{}", driver_addr_str))
                .await
            {
                eprintln!("Executor failed to register: {}", e);
                return;
            }
            if let Err(e) = executor_context.start(executor_addr).await {
                eprintln!("Executor failed to start its server: {}", e);
            }
        }
    });
    // Give the executor a moment to start up its server
    sleep(Duration::from_millis(200)).await;

    // --- 4. Run a Distributed RDD computation from the client side ---
    // This simulates a user's application connecting to the driver to run a job.
    // We can reuse the driver_server_context for this as it also has client capabilities.
    info!("--- Starting distributed RDD computation ---");

    // Wait for registration to complete and check driver status
    sleep(Duration::from_millis(500)).await;
    info!(
        "Driver stats: {:?}",
        driver_server_context.get_driver_stats().await
    );

    let data: Vec<i32> = (1..=20).collect();
    info!("Original data (1..20)");

    // Create a DistributedRdd with 4 partitions using the server driver context
    let rdd = driver_server_context.parallelize_distributed(data, 4);
    info!("Created RDD with {} partitions", rdd.num_partitions());

    // Chain serializable operations
    let transformed_rdd = rdd
        .map(Box::new(DoubleOperation)) // Double each number
        .filter(Box::new(GreaterThanPredicate { threshold: 20 })); // Keep results > 20

    // The `run_distributed` method will analyze the RDD's lineage, serialize the operations,
    // and send them to the driver for distributed execution.
    let result = driver_server_context
        .run_distributed(transformed_rdd)
        .await?;

    info!("--- Computation finished ---");
    let expected_result = vec![22, 24, 26, 28, 30, 32, 34, 36, 38, 40];
    info!("Final collected result: {:?}", result);
    info!("Expected result:      {:?}", expected_result);

    assert_eq!(result, expected_result);

    // --- 5. Shutdown ---
    info!("Example finished successfully. Shutting down.");
    // In a real app, you'd send shutdown signals. Here we just abort the tasks.
    driver_handle.abort();
    executor_handle.abort();

    Ok(())
}
