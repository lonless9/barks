//! Distributed Application Example
//!
//! This example demonstrates how to write a distributed Barks application.
//! It shows the complete workflow of setting up a driver-executor cluster
//! and running distributed RDD computations with serializable operations.

use barks_core::operations::{DoubleOperation, GreaterThanPredicate};
use barks_core::{DistributedConfig, DistributedContext};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{info, Level};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    info!("🚀 Starting Barks Distributed Application Example");

    // Step 1: Setup cluster addresses
    let driver_addr: SocketAddr = "127.0.0.1:50091".parse()?;
    let executor_addr: SocketAddr = "127.0.0.1:50092".parse()?;

    // Step 2: Start the Driver
    info!("📡 Starting Driver on {}", driver_addr);
    let driver_context = Arc::new(DistributedContext::new_driver(
        "barks-distributed-app".to_string(),
        DistributedConfig::default(),
    ));

    let driver_handle = tokio::spawn({
        let context = Arc::clone(&driver_context);
        async move {
            if let Err(e) = context.start(driver_addr).await {
                eprintln!("❌ Driver failed to start: {}", e);
            }
        }
    });

    // Give driver time to start
    sleep(Duration::from_millis(300)).await;

    // Step 3: Start an Executor
    info!("⚙️  Starting Executor on {}", executor_addr);
    let executor_handle = tokio::spawn({
        async move {
            let executor_context = DistributedContext::new_executor(
                "barks-distributed-app".to_string(),
                "executor-1".to_string(),
                executor_addr.ip().to_string(),
                executor_addr.port(),
                DistributedConfig::default(),
            );

            // Register with driver
            let driver_url = format!("http://{}", driver_addr);
            if let Err(e) = executor_context.register_with_driver(driver_url).await {
                eprintln!("❌ Executor failed to register: {}", e);
                return;
            }
            info!("✅ Executor registered with driver");

            // Start executor service
            if let Err(e) = executor_context.start(executor_addr).await {
                eprintln!("❌ Executor failed to start: {}", e);
            }
        }
    });

    // Wait for executor registration
    sleep(Duration::from_millis(500)).await;

    // Step 4: Verify cluster setup
    match driver_context.get_driver_stats().await {
        Some(stats) => {
            info!("📊 Cluster ready! Executors: {}", stats.executor_count);
            if stats.executor_count == 0 {
                eprintln!("⚠️  Warning: No executors registered");
            }
        }
        None => {
            eprintln!("❌ Failed to get driver stats");
        }
    }

    // Step 5: Run distributed computation
    info!("🔄 Running distributed computation...");

    // Create sample data
    let data: Vec<i32> = (1..=100).collect();
    info!("📊 Processing {} numbers", data.len());

    // Create distributed RDD with multiple partitions
    let rdd = driver_context.parallelize_distributed(data, 8);
    info!("📦 Created RDD with {} partitions", rdd.num_partitions());

    // Apply transformations using serializable operations
    let transformed_rdd = rdd
        .map(Box::new(DoubleOperation)) // Double each number
        .filter(Box::new(GreaterThanPredicate { threshold: 100 })); // Keep numbers > 100

    info!("🔧 Applied transformations: map(x * 2) -> filter(x > 100)");

    // Execute the computation
    let start_time = std::time::Instant::now();
    let result = driver_context.run_distributed(transformed_rdd).await?;
    let duration = start_time.elapsed();

    // Step 6: Display results
    info!("✅ Computation completed in {:?}", duration);
    info!("📈 Result count: {}", result.len());

    // Show first few results
    let preview_count = 10.min(result.len());
    if preview_count > 0 {
        let mut sorted_result = result.clone();
        sorted_result.sort();
        info!(
            "🔍 First {} results: {:?}",
            preview_count,
            &sorted_result[..preview_count]
        );
    }

    // Verify correctness
    let expected_count = (51..=100).count(); // Numbers 51-100 when doubled become 102-200
    if result.len() == expected_count {
        info!("✅ Result verification passed!");
    } else {
        eprintln!(
            "❌ Result verification failed! Expected {} items, got {}",
            expected_count,
            result.len()
        );
    }

    // Step 7: Cleanup
    info!("🧹 Shutting down cluster...");
    driver_handle.abort();
    executor_handle.abort();

    info!("🎉 Distributed application example completed successfully!");

    println!("\n=== Summary ===");
    println!("✓ Driver-Executor cluster setup");
    println!("✓ Distributed RDD creation");
    println!("✓ Serializable operations (map, filter)");
    println!("✓ Distributed task execution");
    println!("✓ Result collection and verification");
    println!("✓ Cluster cleanup");

    Ok(())
}
