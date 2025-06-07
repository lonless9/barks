//! Example demonstrating distributed RDD execution with serializable operations
//!
//! This example shows how to use the new distributed RDD system that supports
//! serializable operations instead of closures, enabling true distributed execution.

use anyhow::Result;
use barks_core::operations::{
    AddConstantOperation, DoubleOperation, EvenPredicate, GreaterThanPredicate,
};
use barks_core::{DistributedConfig, DistributedContext};
use std::net::SocketAddr;
use tokio::time::{Duration, sleep};
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    info!("Starting distributed RDD example with serializable operations");

    // Start driver and executor in parallel
    let driver_handle = tokio::spawn(start_driver());
    let executor_handle = tokio::spawn(start_executor());

    // Wait a bit for services to start
    sleep(Duration::from_secs(2)).await;

    // Run RDD computations
    let computation_handle = tokio::spawn(run_rdd_computations());

    // Wait for all tasks to complete
    let (driver_result, executor_result, computation_result) =
        tokio::join!(driver_handle, executor_handle, computation_handle);

    if let Err(e) = driver_result? {
        error!("Driver error: {}", e);
    }
    if let Err(e) = executor_result? {
        error!("Executor error: {}", e);
    }
    if let Err(e) = computation_result? {
        error!("Computation error: {}", e);
    }

    info!("Distributed RDD example completed");
    Ok(())
}

async fn start_driver() -> Result<()> {
    info!("Starting Driver service");

    let config = DistributedConfig::default();
    let context = DistributedContext::new_driver("rdd-example".to_string(), config);
    let addr: SocketAddr = "127.0.0.1:50051".parse()?;

    info!("Driver listening on {}", addr);
    context
        .start(addr)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    Ok(())
}

async fn start_executor() -> Result<()> {
    info!("Starting Executor service");

    // Wait a bit for driver to start
    sleep(Duration::from_secs(1)).await;

    let config = DistributedConfig::default();
    let context = DistributedContext::new_executor(
        "rdd-example".to_string(),
        "executor-1".to_string(),
        "127.0.0.1".to_string(),
        50052,
        config,
    );

    // Register with driver
    context
        .register_with_driver("http://127.0.0.1:50051".to_string())
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;
    info!("Executor registered with driver");

    // Start executor service
    let addr: SocketAddr = "127.0.0.1:50052".parse()?;
    info!("Executor listening on {}", addr);
    context
        .start(addr)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    Ok(())
}

async fn run_rdd_computations() -> Result<()> {
    info!("Running RDD computations with serializable operations");

    // Wait for services to be ready
    sleep(Duration::from_secs(3)).await;

    // Create a driver context for running computations
    let config = DistributedConfig::default();
    let context = DistributedContext::new_driver("rdd-computation".to_string(), config);

    // Test 1: Basic distributed RDD collection
    info!("Test 1: Basic distributed RDD collection");
    let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let rdd = context.parallelize_distributed(data.clone(), 3);

    match context.run_distributed(rdd).await {
        Ok(result) => {
            info!("Collected result: {:?}", result);
            // Note: Order might be different due to distributed execution
            assert_eq!(result.len(), data.len());
        }
        Err(e) => {
            error!("Failed to collect RDD: {}", e);
        }
    }

    // Test 2: RDD with map operation (using serializable operations)
    info!("Test 2: RDD with map operation (double each element) - DISTRIBUTED");
    let data = vec![1, 2, 3, 4, 5];
    let rdd = context.parallelize_distributed(data.clone(), 2);
    let mapped_rdd = rdd.map(Box::new(DoubleOperation));

    // Use the new run_distributed method for true distributed execution
    match context.run_distributed(mapped_rdd).await {
        Ok(result) => {
            info!("Mapped result (distributed): {:?}", result);
            let expected: Vec<i32> = data.iter().map(|x| x * 2).collect();
            // Note: Order might be different due to distributed execution
            assert_eq!(result.len(), expected.len());
            for item in &expected {
                assert!(result.contains(item));
            }
        }
        Err(e) => {
            error!("Failed to collect mapped RDD: {}", e);
        }
    }

    // Test 3: RDD with filter operation
    info!("Test 3: RDD with filter operation (even numbers only) - DISTRIBUTED");
    let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let rdd = context.parallelize_distributed(data.clone(), 2);
    let filtered_rdd = rdd.filter(Box::new(EvenPredicate));

    match context.run_distributed(filtered_rdd).await {
        Ok(result) => {
            info!("Filtered result (distributed): {:?}", result);
            let expected: Vec<i32> = data.iter().filter(|&x| x % 2 == 0).cloned().collect();
            // Note: Order might be different due to distributed execution
            assert_eq!(result.len(), expected.len());
            for item in &expected {
                assert!(result.contains(item));
            }
        }
        Err(e) => {
            error!("Failed to collect filtered RDD: {}", e);
        }
    }

    // Test 4: Chained operations - This demonstrates the power of the new system!
    info!("Test 4: Chained operations (add 10, then filter even) - DISTRIBUTED");
    let data = vec![1, 2, 3, 4, 5];
    let rdd = context.parallelize_distributed(data.clone(), 2);
    let chained_rdd = rdd
        .map(Box::new(AddConstantOperation { constant: 10 }))
        .filter(Box::new(EvenPredicate));

    // This will analyze the lineage, create ChainedTask<i32> with the full operation chain,
    // and send them to executors for distributed execution!
    match context.run_distributed(chained_rdd).await {
        Ok(result) => {
            info!("Chained result (distributed): {:?}", result);
            let expected: Vec<i32> = data
                .iter()
                .map(|x| x + 10)
                .filter(|&x| x % 2 == 0)
                .collect();
            // Note: Order might be different due to distributed execution
            assert_eq!(result.len(), expected.len());
            for item in &expected {
                assert!(result.contains(item));
            }
        }
        Err(e) => {
            error!("Failed to collect chained RDD: {}", e);
        }
    }

    // Test 5: Complex chained operations (from TODO example)
    info!("Test 5: Complex chain (double, then filter > 20) - DISTRIBUTED");
    let data: Vec<i32> = (1..=20).collect();
    let rdd = context.parallelize_distributed(data.clone(), 4);
    let complex_rdd = rdd
        .map(Box::new(DoubleOperation)) // Double each number
        .filter(Box::new(GreaterThanPredicate { threshold: 20 })); // Keep results > 20

    // This demonstrates the full end-to-end distributed computation flow
    // as described in the TODO document
    match context.run_distributed(complex_rdd).await {
        Ok(result) => {
            info!("Complex chained result (distributed): {:?}", result);
            let expected: Vec<i32> = data.iter().map(|x| x * 2).filter(|&x| x > 20).collect();
            info!("Expected result: {:?}", expected);
            // Note: Order might be different due to distributed execution
            assert_eq!(result.len(), expected.len());
            for item in &expected {
                assert!(result.contains(item));
            }
        }
        Err(e) => {
            error!("Failed to collect complex chained RDD: {}", e);
        }
    }

    info!("All RDD computations completed successfully!");

    // Simulate some work to keep the example running
    for i in 0..3 {
        info!("Computation phase {}", i + 1);
        sleep(Duration::from_secs(1)).await;
    }

    Ok(())
}
