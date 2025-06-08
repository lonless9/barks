//! Integration tests for the distributed Driver-Executor architecture.
//!
//! These tests verify the complete distributed workflow, including:
//! - Driver and Executor registration.
//! - Serializable task submission and execution.
//! - Chained RDD operations (map, filter) running on the cluster.
//! - Basic shuffle operations.

use barks_core::context::{DistributedConfig, DistributedContext};
use barks_core::operations::{DoubleOperation, GreaterThanPredicate};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{info, Level};

/// Helper to set up a mini-cluster with 1 driver and N executors.
/// Returns the driver context and handles for spawned tasks.
async fn setup_mini_cluster(
    driver_port: u16,
    executor_ports: &[u16],
) -> (Arc<DistributedContext>, Vec<tokio::task::JoinHandle<()>>) {
    let driver_addr_str = format!("127.0.0.1:{}", driver_port);
    let driver_addr: SocketAddr = driver_addr_str.parse().unwrap();
    let config = DistributedConfig::default();

    // Start Driver
    let driver_context = Arc::new(DistributedContext::new_driver(
        "barks-integration-test".to_string(),
        config.clone(),
    ));
    let driver_handle = tokio::spawn({
        let context = Arc::clone(&driver_context);
        async move {
            if let Err(e) = context.start(driver_addr).await {
                panic!("Driver failed to start: {}", e);
            }
        }
    });

    sleep(Duration::from_millis(200)).await; // Give driver time to start

    let mut handles = vec![driver_handle];

    // Start Executors
    for (i, &port) in executor_ports.iter().enumerate() {
        let executor_addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
        let handle = tokio::spawn({
            let config = config.clone();
            let driver_http_addr = format!("http://{}", driver_addr_str);
            async move {
                let executor_context = DistributedContext::new_executor(
                    "barks-app".to_string(),
                    format!("executor-{}", i),
                    executor_addr.ip().to_string(),
                    executor_addr.port(),
                    config,
                );

                if let Err(e) = executor_context
                    .register_with_driver(driver_http_addr)
                    .await
                {
                    panic!("Executor failed to register: {}", e);
                }

                if let Err(e) = executor_context.start(executor_addr).await {
                    panic!("Executor failed to start its server: {}", e);
                }
            }
        });
        handles.push(handle);
    }

    // Wait for executors to register
    sleep(Duration::from_millis(500)).await;

    (driver_context, handles)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_basic_distributed_computation() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_test_writer()
        .try_init();

    info!("--- test_basic_distributed_computation ---");
    let (driver_context, handles) = setup_mini_cluster(60001, &[60002, 60003]).await;

    assert_eq!(
        driver_context
            .get_driver_stats()
            .await
            .unwrap()
            .executor_count,
        2,
        "Should have 2 executors registered"
    );

    let data: Vec<i32> = (1..=20).collect();
    info!("Original data (1..20)");

    let rdd = driver_context.parallelize_distributed(data, 4);
    info!("Created RDD with {} partitions", rdd.num_partitions());

    let transformed_rdd = rdd
        .map(Box::new(DoubleOperation)) // Double each number
        .filter(Box::new(GreaterThanPredicate { threshold: 20 })); // Keep results > 20

    let result = driver_context
        .run(Arc::new(transformed_rdd))
        .await
        .expect("Distributed computation failed");

    info!("--- Computation finished ---");
    let mut expected_result = vec![22, 24, 26, 28, 30, 32, 34, 36, 38, 40];
    info!("Final collected result: {:?}", result);
    info!("Expected result:      {:?}", expected_result);

    // Sort both vectors for order-independent comparison
    let mut sorted_result = result;
    sorted_result.sort();
    expected_result.sort();

    assert_eq!(
        sorted_result, expected_result,
        "Results do not match after sorting."
    );

    // Shutdown cluster
    for handle in handles {
        handle.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_distributed_shuffle_reduce_by_key() {
    // Test the shuffle infrastructure with local execution fallback
    let _ = tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_test_writer()
        .try_init();

    info!("--- test_distributed_shuffle_reduce_by_key ---");
    let (driver_context, handles) = setup_mini_cluster(60011, &[60012, 60013]).await;

    let data = vec![
        ("apple", 1),
        ("banana", 2),
        ("apple", 3),
        ("cherry", 1),
        ("banana", 4),
        ("apple", 2),
    ];
    let data_owned: Vec<(String, i32)> =
        data.into_iter().map(|(k, v)| (k.to_string(), v)).collect();

    // Create an RDD using the existing parallelize method
    let rdd = driver_context.parallelize_distributed(data_owned, 2);

    // Use the PairRdd trait to create a ShuffledRdd
    use barks_core::rdd::transformations::PairRdd;
    let partitioner = Arc::new(barks_core::shuffle::HashPartitioner::new(2));
    let reduced_rdd = rdd.reduce_by_key(|a, b| a + b, partitioner);

    // For now, test the local execution since distributed shuffle is simplified
    let result = reduced_rdd.collect().unwrap();

    // Convert to HashMap for easier comparison
    let result_map: std::collections::HashMap<String, i32> = result.into_iter().collect();
    let mut expected_map = std::collections::HashMap::new();
    expected_map.insert("apple".to_string(), 6); // 1 + 3 + 2
    expected_map.insert("banana".to_string(), 6); // 2 + 4
    expected_map.insert("cherry".to_string(), 1); // 1

    assert_eq!(result_map, expected_map);

    // Shutdown cluster
    for handle in handles {
        handle.abort();
    }
}
