//! Integration tests for the distributed Driver-Executor architecture
//!
//! These tests verify that the Driver and Executor services can communicate
//! properly and execute tasks in a distributed manner.

use barks_core::distributed::{
    Driver, Executor, ExecutorInfo, SimpleTaskInfo,
    driver::{RddOperation, TaskData},
};
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

        let task_info = SimpleTaskInfo::new(task_id.clone(), stage_id.clone(), i, 100 + i * 10);

        let task_data = bincode::encode_to_vec(&task_info, bincode::config::standard())
            .expect("Failed to serialize task");

        driver
            .submit_task(task_id, stage_id, i, task_data, None)
            .await;
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

    // Create RDD tasks with different operations
    let operations = vec![
        RddOperation::Map {
            closure_data: vec![],
        },
        RddOperation::Filter {
            predicate_data: vec![],
        },
        RddOperation::Collect,
        RddOperation::Reduce {
            function_data: vec![],
        },
        RddOperation::FlatMap {
            closure_data: vec![],
        },
    ];

    for (i, operation) in operations.into_iter().enumerate() {
        let task_id = format!("rdd-test-task-{}", i);
        let stage_id = format!("rdd-test-stage-{}", i);

        // Create sample partition data
        let partition_data: Vec<i32> = (0..50).map(|x| x * (i + 1) as i32).collect();
        let serialized_data = bincode::encode_to_vec(&partition_data, bincode::config::standard())
            .expect("Failed to serialize partition data");

        let rdd_task = TaskData {
            partition_data: serialized_data,
            operation: operation,
        };

        // The task data sent to the scheduler is the serialized `TaskData`
        let task_data = bincode::encode_to_vec(&rdd_task, bincode::config::standard())
            .expect("Failed to serialize RDD task");

        driver
            .submit_task(task_id, stage_id, i, task_data, None)
            .await;
    }

    // Verify pending tasks
    sleep(Duration::from_millis(100)).await;
    let pending_count = driver.pending_task_count().await;
    assert_eq!(pending_count, 5, "Driver should have 5 pending RDD tasks");
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
    // Test SimpleTaskInfo serialization
    let simple_task = SimpleTaskInfo::new(
        "test-task-001".to_string(),
        "test-stage-001".to_string(),
        0,
        1000,
    );

    let serialized = bincode::encode_to_vec(&simple_task, bincode::config::standard())
        .expect("Failed to serialize SimpleTaskInfo");

    let (deserialized, _): (SimpleTaskInfo, _) =
        bincode::decode_from_slice(&serialized, bincode::config::standard())
            .expect("Failed to deserialize SimpleTaskInfo");

    assert_eq!(simple_task.task_id, deserialized.task_id);
    assert_eq!(simple_task.stage_id, deserialized.stage_id);
    assert_eq!(simple_task.partition_index, deserialized.partition_index);
    assert_eq!(simple_task.data_size, deserialized.data_size);

    // Test TaskData serialization, which is the core of an RDD task
    let partition_data: Vec<i32> = vec![1, 2, 3, 4, 5];
    let serialized_data = bincode::encode_to_vec(&partition_data, bincode::config::standard())
        .expect("Failed to serialize partition data");

    let rdd_task = TaskData {
        partition_data: serialized_data.clone(),
        operation: RddOperation::Map {
            closure_data: vec![1, 2],
        },
    };

    let serialized_rdd = bincode::encode_to_vec(&rdd_task, bincode::config::standard())
        .expect("Failed to serialize TaskData");

    let (deserialized_rdd, _): (TaskData, _) =
        bincode::decode_from_slice(&serialized_rdd, bincode::config::standard())
            .expect("Failed to deserialize TaskData");

    assert_eq!(rdd_task.partition_data, deserialized_rdd.partition_data);

    // Verify the partition data can be deserialized
    let (recovered_data, _): (Vec<i32>, _) = bincode::decode_from_slice(
        &deserialized_rdd.partition_data,
        bincode::config::standard(),
    )
    .expect("Failed to deserialize partition data");

    assert_eq!(partition_data, recovered_data);
}
