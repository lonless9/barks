//! Test for executor registration retry logic
//!
//! This test verifies that the executor can successfully retry
//! connection attempts when the driver is not immediately available.

use barks_core::context::DistributedConfig;
use barks_core::distributed::{Driver, Executor, ExecutorInfo};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;
use tracing::info;

#[tokio::test]
async fn test_executor_retry_logic() {
    // Initialize tracing for the test
    let _ = tracing_subscriber::fmt::try_init();

    info!("Starting executor retry logic test");

    // Define addresses
    let driver_addr: SocketAddr = "127.0.0.1:50091".parse().unwrap();
    let executor_addr: SocketAddr = "127.0.0.1:50092".parse().unwrap();

    // Create executor first (before driver is available)
    let executor_info = ExecutorInfo::new(
        "test-executor-retry".to_string(),
        executor_addr.ip().to_string(),
        executor_addr.port(),
        executor_addr.port() + 1000, // shuffle_port
        2,                           // cores
        1024,                        // memory_mb
    );

    let config = DistributedConfig::default();
    let executor = Executor::new(executor_info, 2, config.clone());

    // Start executor registration in background (this should retry)
    let executor_clone = executor.clone();
    let driver_addr_str = format!("http://{}", driver_addr);
    let registration_handle = tokio::spawn(async move {
        match executor_clone.register_with_driver(driver_addr_str).await {
            Ok(()) => Ok(()),
            Err(e) => Err(e.to_string()),
        }
    });

    // Wait a bit to let the executor start trying to connect
    sleep(Duration::from_secs(1)).await;

    // Now start the driver (simulating delayed startup)
    info!("Starting driver after executor has been trying to connect");
    let driver = Driver::new("test-driver-retry".to_string(), config);
    let driver_handle = tokio::spawn(async move {
        if let Err(e) = driver.start(driver_addr).await {
            eprintln!("Driver failed to start: {}", e);
        }
    });

    // Wait for registration to complete
    let registration_result = tokio::time::timeout(
        Duration::from_secs(15), // Give enough time for retries
        registration_handle,
    )
    .await;

    match registration_result {
        Ok(Ok(Ok(()))) => {
            info!("✅ Executor successfully registered with retry logic!");
        }
        Ok(Ok(Err(e))) => {
            panic!("❌ Executor registration failed: {}", e);
        }
        Ok(Err(e)) => {
            panic!("❌ Registration task panicked: {}", e);
        }
        Err(_) => {
            panic!("❌ Registration timed out");
        }
    }

    // Clean up
    driver_handle.abort();

    info!("Test completed successfully");
}

#[tokio::test]
async fn test_executor_immediate_connection() {
    // Initialize tracing for the test
    let _ = tracing_subscriber::fmt::try_init();

    info!("Starting immediate connection test");

    // Define addresses
    let driver_addr: SocketAddr = "127.0.0.1:50093".parse().unwrap();
    let executor_addr: SocketAddr = "127.0.0.1:50094".parse().unwrap();

    // Start driver first
    let config = DistributedConfig::default();
    let driver = Driver::new("test-driver-immediate".to_string(), config.clone());
    let driver_handle = tokio::spawn(async move {
        if let Err(e) = driver.start(driver_addr).await {
            eprintln!("Driver failed to start: {}", e);
        }
    });

    // Give driver time to start
    sleep(Duration::from_millis(500)).await;

    // Create and register executor
    let executor_info = ExecutorInfo::new(
        "test-executor-immediate".to_string(),
        executor_addr.ip().to_string(),
        executor_addr.port(),
        executor_addr.port() + 1000, // shuffle_port
        2,                           // cores
        1024,                        // memory_mb
    );

    let executor = Executor::new(executor_info, 2, config);
    let driver_addr_str = format!("http://{}", driver_addr);

    // This should succeed immediately (no retries needed)
    let registration_result = tokio::time::timeout(Duration::from_secs(5), async {
        match executor.register_with_driver(driver_addr_str).await {
            Ok(()) => Ok(()),
            Err(e) => Err(e.to_string()),
        }
    })
    .await;

    match registration_result {
        Ok(Ok(())) => {
            info!("✅ Executor successfully registered immediately!");
        }
        Ok(Err(e)) => {
            panic!("❌ Executor registration failed: {}", e);
        }
        Err(_) => {
            panic!("❌ Registration timed out");
        }
    }

    // Clean up
    driver_handle.abort();

    info!("Immediate connection test completed successfully");
}
