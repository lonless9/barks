//! Integration tests for the distributed Driver-Executor architecture
//!
//! These tests verify that the Driver and Executor services can communicate
//! properly and execute tasks in a distributed manner.

use barks_core::distributed::{
    Driver, Executor, ExecutorInfo,
    task::{ChainedI32Task, Task},
};
use barks_core::operations::{DoubleOperation, SerializableI32Operation};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;
use tracing_test::traced_test;

/// Test basic Driver-Executor registration and communication
#[tokio::test]
#[traced_test]
async fn test_driver_executor_registration() {
    // Start driver
    let driver_addr: SocketAddr = "127.0.0.1:50061".parse().unwrap();
    let driver = Driver::new("test-driver-001".to_string());

    let _driver_handle = tokio::spawn({
        let driver = driver.clone();
        async move {
            let _ = driver.start(driver_addr).await;
        }
    });

    // Give driver time to start
    sleep(Duration::from_millis(500)).await;

    // Create and register executor
    let executor_addr: SocketAddr = "127.0.0.1:50062".parse().unwrap();
    let executor_info = ExecutorInfo::new(
        "test-executor-001".to_string(),
        "127.0.0.1".to_string(),
        executor_addr.port(),
        2,    // 2 cores
        4096, // 4GB memory
    );

    let mut executor = Executor::new(executor_info, 2);

    // Register with driver
    let driver_url = format!("http://{}", driver_addr);
    let registration_result = executor.register_with_driver(driver_url).await;

    assert!(
        registration_result.is_ok(),
        "Executor should register successfully"
    );

    // Verify executor count
    sleep(Duration::from_millis(100)).await;
    let executor_count = driver.executor_count().await;
    assert_eq!(
        executor_count, 1,
        "Driver should have 1 registered executor"
    );
}

/// Test task submission and basic scheduling
#[tokio::test]
#[traced_test]
async fn test_task_submission() {
    // Start driver
    let driver_addr: SocketAddr = "127.0.0.1:50063".parse().unwrap();
    let driver = Driver::new("test-driver-002".to_string());

    let _driver_handle = tokio::spawn({
        let driver = driver.clone();
        async move {
            let _ = driver.start(driver_addr).await;
        }
    });

    sleep(Duration::from_millis(500)).await;

    // Submit some tasks
    for i in 0..5 {
        let task_id = format!("test-task-{}", i);
        let stage_id = format!("test-stage-{}", i / 2);

        // Create a task with some data and an operation
        let partition_data: Vec<i32> = vec![i as i32, i as i32 + 1];
        let serialized_partition_data =
            bincode::encode_to_vec(&partition_data, bincode::config::standard())
                .expect("Failed to serialize partition data");

        let task: Box<dyn Task> = Box::new(ChainedI32Task {
            partition_data: serialized_partition_data,
            operations: vec![], // Empty operations for a simple collect
        });

        let _result_receiver = driver.submit_task(task_id, stage_id, i, task, None).await;
    }

    // Verify pending tasks
    sleep(Duration::from_millis(100)).await;
    let pending_count = driver.pending_task_count().await;
    assert_eq!(pending_count, 5, "Driver should have 5 pending tasks");
}

/// Test RDD task serialization and submission
#[tokio::test]
#[traced_test]
async fn test_rdd_task_submission() {
    // Start driver
    let driver_addr: SocketAddr = "127.0.0.1:50064".parse().unwrap();
    let driver = Driver::new("test-driver-003".to_string());

    let _driver_handle = tokio::spawn({
        let driver = driver.clone();
        async move {
            let _ = driver.start(driver_addr).await;
        }
    });

    sleep(Duration::from_millis(500)).await;

    let operations = vec![
        vec![SerializableI32Operation::Map(Box::new(DoubleOperation))],
        vec![], // Empty operations for collect
        vec![], // Another empty operations for collect
    ];

    for (i, ops) in operations.into_iter().enumerate() {
        let task_id = format!("rdd-test-task-{}", i);
        let stage_id = "rdd-test-stage-1".to_string();
        // Create sample partition data
        let partition_data: Vec<i32> = (0..50).map(|x| x * (i + 1) as i32).collect();
        let serialized_data = bincode::encode_to_vec(&partition_data, bincode::config::standard())
            .expect("Failed to serialize partition data");

        let task: Box<dyn Task> = Box::new(ChainedI32Task {
            partition_data: serialized_data,
            operations: ops,
        });

        let _result_receiver = driver.submit_task(task_id, stage_id, i, task, None).await;
    }

    // Verify pending tasks
    sleep(Duration::from_millis(100)).await;
    let pending_count = driver.pending_task_count().await;
    assert_eq!(pending_count, 3, "Driver should have 3 pending RDD tasks");
}

/// Test multiple executors registration
#[tokio::test]
#[traced_test]
async fn test_multiple_executors() {
    // Start driver
    let driver_addr: SocketAddr = "127.0.0.1:50065".parse().unwrap();
    let driver = Driver::new("test-driver-004".to_string());

    let _driver_handle = tokio::spawn({
        let driver = driver.clone();
        async move {
            let _ = driver.start(driver_addr).await;
        }
    });

    sleep(Duration::from_millis(500)).await;

    // Register multiple executors
    let executor_count = 3;
    let mut executors = Vec::new();

    for i in 0..executor_count {
        let executor_id = format!("test-executor-{:03}", i);
        let executor_addr: SocketAddr = format!("127.0.0.1:{}", 50066 + i).parse().unwrap();

        let executor_info = ExecutorInfo::new(
            executor_id.clone(),
            "127.0.0.1".to_string(),
            executor_addr.port(),
            2,    // 2 cores
            2048, // 2GB memory
        );

        let mut executor = Executor::new(executor_info, 2);

        // Register with driver
        let driver_url = format!("http://{}", driver_addr);
        let registration_result = executor.register_with_driver(driver_url).await;

        assert!(
            registration_result.is_ok(),
            "Executor {} should register successfully",
            executor_id
        );
        executors.push(executor);
    }

    // Verify all executors are registered
    sleep(Duration::from_millis(200)).await;
    let registered_count = driver.executor_count().await;
    assert_eq!(
        registered_count, executor_count,
        "Driver should have {} registered executors",
        executor_count
    );
}

/// Test task serialization and deserialization
#[test]
fn test_task_serialization() {
    // Test ChainedI32Task serialization, which is the core of an RDD task
    let partition_data: Vec<i32> = vec![1, 2, 3, 4, 5];
    let serialized_data = bincode::encode_to_vec(&partition_data, bincode::config::standard())
        .expect("Failed to serialize partition data");

    let task: Box<dyn Task> = Box::new(ChainedI32Task {
        partition_data: serialized_data.clone(),
        operations: vec![SerializableI32Operation::Map(Box::new(DoubleOperation))],
    });

    let serialized_task = serde_json::to_vec(&task).expect("Failed to serialize ChainedI32Task");

    let deserialized_task: Box<dyn Task> =
        serde_json::from_slice(&serialized_task).expect("Failed to deserialize ChainedI32Task");

    // We can't directly compare the tasks, but we can test that they work the same way
    // by executing them and comparing results
    let rt = tokio::runtime::Runtime::new().unwrap();

    let original_result = rt.block_on(async { task.execute(0).await.unwrap() });
    let deserialized_result = rt.block_on(async { deserialized_task.execute(0).await.unwrap() });

    assert_eq!(original_result, deserialized_result);

    // Verify the result data can be deserialized
    let (result_data, _): (Vec<i32>, _) =
        bincode::decode_from_slice(&original_result, bincode::config::standard())
            .expect("Failed to deserialize result data");

    let expected: Vec<i32> = partition_data.iter().map(|x| x * 2).collect();
    assert_eq!(result_data, expected);
}
