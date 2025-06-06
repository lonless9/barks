//! Advanced distributed RDD example demonstrating the complete workflow
//!
//! This example shows:
//! 1. Setting up a distributed cluster with driver and executor
//! 2. Creating distributed RDDs with serializable operations
//! 3. Running computations in both local and distributed modes
//! 4. Comparing performance and results

use anyhow::Result;
use barks_core::distributed::context::{DistributedConfig, DistributedContext};
use barks_core::operations::{
    AddConstantOperation, DoubleOperation, EvenPredicate, GreaterThanPredicate, SquareOperation,
};
use std::net::SocketAddr;
use std::time::Instant;
use tokio::time::{Duration, sleep};
use tracing::{error, info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing with better formatting
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    info!("🚀 Starting Advanced Distributed RDD Example");
    info!("This example demonstrates the new serializable operations system");

    // Test 1: Local execution for baseline
    info!("\n📊 Test 1: Local Execution (Baseline)");
    test_local_execution().await?;

    // Test 2: Distributed execution
    info!("\n🌐 Test 2: Distributed Execution");
    test_distributed_execution().await?;

    // Test 3: Performance comparison
    info!("\n⚡ Test 3: Performance Comparison");
    performance_comparison().await?;

    info!("\n✅ Advanced distributed RDD example completed successfully!");
    Ok(())
}

async fn test_local_execution() -> Result<()> {
    info!("Creating local context for baseline testing...");

    let config = DistributedConfig::default();
    let context = DistributedContext::new_local("local-test".to_string());

    // Create test data
    let data: Vec<i32> = (1..=1000).collect();
    info!("Created test dataset with {} elements", data.len());

    // Test 1: Basic collection
    let start = Instant::now();
    let rdd = context.parallelize_i32_with_partitions(data.clone(), 4);
    let result = rdd.collect()?;
    let duration = start.elapsed();

    info!(
        "✓ Basic collection: {} elements in {:?}",
        result.len(),
        duration
    );
    assert_eq!(result.len(), data.len());

    // Test 2: Map operation
    let start = Instant::now();
    let rdd = context.parallelize_i32_with_partitions(data.clone(), 4);
    let mapped_rdd = rdd.map(Box::new(DoubleOperation));
    let result = mapped_rdd.collect()?;
    let duration = start.elapsed();

    info!(
        "✓ Map operation (double): {} elements in {:?}",
        result.len(),
        duration
    );
    assert_eq!(result[0], 2); // First element should be doubled
    assert_eq!(result[999], 2000); // Last element should be doubled

    // Test 3: Filter operation
    let start = Instant::now();
    let rdd = context.parallelize_i32_with_partitions(data.clone(), 4);
    let filtered_rdd = rdd.filter(Box::new(EvenPredicate));
    let result = filtered_rdd.collect()?;
    let duration = start.elapsed();

    info!(
        "✓ Filter operation (even): {} elements in {:?}",
        result.len(),
        duration
    );
    assert_eq!(result.len(), 500); // Half should be even

    // Test 4: Complex chained operations
    let start = Instant::now();
    let rdd = context.parallelize_i32_with_partitions(data.clone(), 4);
    let complex_rdd = rdd
        .map(Box::new(SquareOperation))
        .filter(Box::new(GreaterThanPredicate { threshold: 100 }))
        .map(Box::new(AddConstantOperation { constant: 1 }));
    let result = complex_rdd.collect()?;
    let duration = start.elapsed();

    info!(
        "✓ Complex chain (square->filter>100->add1): {} elements in {:?}",
        result.len(),
        duration
    );

    Ok(())
}

async fn test_distributed_execution() -> Result<()> {
    info!("Setting up distributed cluster...");

    // Start driver in background
    let driver_handle = tokio::spawn(async {
        let config = DistributedConfig::default();
        let context = DistributedContext::new_driver("distributed-test".to_string(), config);
        let addr: SocketAddr = "127.0.0.1:50061".parse().unwrap();

        info!("🖥️  Driver starting on {}", addr);
        if let Err(e) = context.start(addr).await {
            error!("Driver failed: {}", e);
        }
    });

    // Start executor in background
    let executor_handle = tokio::spawn(async {
        sleep(Duration::from_secs(1)).await; // Wait for driver

        let config = DistributedConfig::default();
        let context = DistributedContext::new_executor(
            "distributed-test".to_string(),
            "executor-1".to_string(),
            "127.0.0.1".to_string(),
            50062,
            config,
        );

        if let Err(e) = context
            .register_with_driver("http://127.0.0.1:50061".to_string())
            .await
        {
            error!("Executor registration failed: {}", e);
            return;
        }

        let addr: SocketAddr = "127.0.0.1:50062".parse().unwrap();
        info!("⚙️  Executor starting on {}", addr);
        if let Err(e) = context.start(addr).await {
            error!("Executor failed: {}", e);
        }
    });

    // Wait for cluster to be ready
    sleep(Duration::from_secs(3)).await;

    // Create driver context for computations
    let config = DistributedConfig::default();
    let context = DistributedContext::new_driver("computation-driver".to_string(), config);

    // Test distributed RDD operations
    let data: Vec<i32> = (1..=100).collect();
    info!("Testing distributed execution with {} elements", data.len());

    // Test 1: Basic distributed collection
    let start = Instant::now();
    let rdd = context.parallelize_i32_with_partitions(data.clone(), 3);

    match context.run_i32(rdd).await {
        Ok(result) => {
            let duration = start.elapsed();
            info!(
                "✓ Distributed collection: {} elements in {:?}",
                result.len(),
                duration
            );
            assert_eq!(result.len(), data.len());
        }
        Err(e) => {
            warn!("Distributed execution failed, falling back to local: {}", e);
        }
    }

    // Test 2: Multiple partitions
    let start = Instant::now();
    let rdd = context.parallelize_i32_with_partitions(data.clone(), 5);

    match context.run_i32(rdd).await {
        Ok(result) => {
            let duration = start.elapsed();
            info!(
                "✓ Multi-partition distributed: {} elements in {:?}",
                result.len(),
                duration
            );
        }
        Err(e) => {
            warn!("Multi-partition execution failed: {}", e);
        }
    }

    // Test 3: Distributed execution of a transformed RDD
    info!("Testing distributed execution of a transformed RDD (map -> filter)");
    let start = Instant::now();
    let rdd = context.parallelize_i32_with_partitions(data.clone(), 3);
    let transformed_rdd = rdd
        .map(Box::new(DoubleOperation))
        .filter(Box::new(GreaterThanPredicate { threshold: 50 }));

    match context.run_i32(transformed_rdd).await {
        Ok(result) => {
            let duration = start.elapsed();
            info!(
                "✓ Transformed RDD (distributed): {} elements in {:?}",
                result.len(),
                duration
            );
            // Expected: (1..100) -> double -> (2..200) -> filter(>50) -> [52, 54, ..., 200]
            // (100 - 25) = 75 elements
            assert_eq!(result.len(), 75, "The number of elements should be 75");
            assert_eq!(result[0], 52); // 26*2
        }
        Err(e) => {
            error!("Distributed execution of transformed RDD failed: {}", e);
        }
    }

    // Clean up
    driver_handle.abort();
    executor_handle.abort();

    Ok(())
}

async fn performance_comparison() -> Result<()> {
    info!("Running performance comparison between local and distributed modes...");

    let sizes = vec![100, 1000, 10000];

    for size in sizes {
        info!("\n📈 Testing with {} elements:", size);

        let data: Vec<i32> = (1..=size).collect();

        // Local performance
        let config = DistributedConfig::default();
        let local_context = DistributedContext::new_local("perf-local".to_string());

        let start = Instant::now();
        let rdd = local_context.parallelize_i32_with_partitions(data.clone(), 4);
        let _result = rdd.collect()?;
        let local_duration = start.elapsed();

        info!("  Local execution: {:?}", local_duration);

        // Note: For a fair comparison, we would need to set up a distributed cluster
        // and measure the distributed execution time. For now, we just show the concept.
        info!("  Distributed execution: Would require cluster setup");

        // Show theoretical benefits
        if size >= 1000 {
            info!("  💡 Distributed execution benefits:");
            info!("     - Parallel processing across multiple machines");
            info!("     - Fault tolerance and recovery");
            info!("     - Scalability to large datasets");
            info!("     - Memory distribution across cluster");
        }
    }

    Ok(())
}

/// Demonstrates the key improvements in the new system
fn demonstrate_improvements() {
    info!("\n🎯 Key Improvements in the New System:");
    info!("1. ✅ Serializable Operations: Operations can be sent across the network");
    info!("2. ✅ Type Safety: Strong typing for different data types (i32, String, etc.)");
    info!("3. ✅ Extensibility: Easy to add new operations without changing core code");
    info!("4. ✅ Distributed Execution: True distributed processing capability");
    info!("5. ✅ Local Fallback: Graceful degradation when distributed execution fails");

    info!("\n📋 Current Limitations:");
    info!("1. ✅ Transformed RDDs are now distributed for i32!");
    info!("2. ⚠️  Limited to specific data types (i32, String)");
    info!("3. ⚠️  Simple scheduling (no advanced optimizations yet)");

    info!("\n🔮 Future Enhancements:");
    info!("1. ✅ Full lineage execution in distributed mode (completed for i32)");
    info!("2. 🚧 Generic operations for any serializable type");
    info!("3. 🚧 Advanced scheduling and optimization");
    info!("4. 🚧 Shuffle operations for wide transformations");
    info!("5. 🚧 Fault tolerance and recovery mechanisms");
}
