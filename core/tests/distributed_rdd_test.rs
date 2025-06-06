//! Integration tests for distributed RDD execution
//!
//! These tests verify that the distributed computing framework
//! correctly executes RDD operations using rayon for parallel processing.

use barks_core::distributed::{
    Driver,
    driver::{RddOperation, TaskData},
    task::TaskRunner,
};
use tracing_test::traced_test;

#[tokio::test]
#[traced_test]
async fn test_task_runner_rdd_execution() {
    // Test the TaskRunner's ability to execute RDD operations with rayon
    let task_runner = TaskRunner::new(4);

    // Create test data
    let partition_data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

    // Test Map operation
    let map_operation = RddOperation::Map {
        closure_data: vec![], // Placeholder
    };

    let task_data = TaskData {
        partition_data: bincode::encode_to_vec(&partition_data, bincode::config::standard())
            .unwrap(),
        operation: map_operation,
    };

    let serialized_task = bincode::encode_to_vec(&task_data, bincode::config::standard()).unwrap();

    // Execute the task
    let result = task_runner
        .submit_task(
            "map-task-1".to_string(),
            "stage-1".to_string(),
            0,
            serialized_task,
        )
        .await;
    assert!(result.is_ok());

    let task_result = result.unwrap();
    assert_eq!(
        task_result.state,
        barks_core::distributed::types::TaskState::Finished
    );
    assert!(task_result.result.is_some());

    // Verify the result (should be doubled values: [2, 4, 6, 8, 10, 12, 14, 16, 18, 20])
    let result_bytes = task_result.result.unwrap();
    println!("Result bytes length: {}", result_bytes.len());

    let result_data: Vec<i32> =
        bincode::decode_from_slice(&result_bytes, bincode::config::standard())
            .unwrap()
            .0;

    println!("Result data: {:?}", result_data);
    let expected: Vec<i32> = partition_data.iter().map(|x| x * 2).collect();
    println!("Expected: {:?}", expected);
    assert_eq!(result_data, expected);
}

#[tokio::test]
#[traced_test]
async fn test_filter_operation() {
    let task_runner = TaskRunner::new(4);

    // Create test data with both even and odd numbers
    let partition_data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

    // Test Filter operation (should keep only even numbers)
    let filter_operation = RddOperation::Filter {
        predicate_data: vec![], // Placeholder
    };

    let task_data = TaskData {
        partition_data: bincode::encode_to_vec(&partition_data, bincode::config::standard())
            .unwrap(),
        operation: filter_operation,
    };

    let serialized_task = bincode::encode_to_vec(&task_data, bincode::config::standard()).unwrap();

    // Execute the task
    let result = task_runner
        .submit_task(
            "filter-task-1".to_string(),
            "stage-1".to_string(),
            0,
            serialized_task,
        )
        .await
        .unwrap();

    // Verify the result (should be even numbers: [2, 4, 6, 8, 10])
    let result_data: Vec<i32> =
        bincode::decode_from_slice(&result.result.unwrap(), bincode::config::standard())
            .unwrap()
            .0;

    let expected: Vec<i32> = vec![2, 4, 6, 8, 10];
    assert_eq!(result_data, expected);
}

#[tokio::test]
#[traced_test]
async fn test_reduce_operation() {
    let task_runner = TaskRunner::new(4);

    // Create test data
    let partition_data = vec![1, 2, 3, 4, 5];

    // Test Reduce operation (should sum all elements)
    let reduce_operation = RddOperation::Reduce {
        function_data: vec![], // Placeholder
    };

    let task_data = TaskData {
        partition_data: bincode::encode_to_vec(&partition_data, bincode::config::standard())
            .unwrap(),
        operation: reduce_operation,
    };

    let serialized_task = bincode::encode_to_vec(&task_data, bincode::config::standard()).unwrap();

    // Execute the task
    let result = task_runner
        .submit_task(
            "reduce-task-1".to_string(),
            "stage-1".to_string(),
            0,
            serialized_task,
        )
        .await
        .unwrap();

    // Verify the result (should be sum: [15])
    let result_data: Vec<i32> =
        bincode::decode_from_slice(&result.result.unwrap(), bincode::config::standard())
            .unwrap()
            .0;

    let expected_sum = partition_data.iter().sum::<i32>();
    assert_eq!(result_data, vec![expected_sum]);
}

#[tokio::test]
#[traced_test]
async fn test_flatmap_operation() {
    let task_runner = TaskRunner::new(4);

    // Create test data
    let partition_data = vec![1, 2, 3];

    // Test FlatMap operation (should duplicate each element)
    let flatmap_operation = RddOperation::FlatMap {
        closure_data: vec![], // Placeholder
    };

    let task_data = TaskData {
        partition_data: bincode::encode_to_vec(&partition_data, bincode::config::standard())
            .unwrap(),
        operation: flatmap_operation,
    };

    let serialized_task = bincode::encode_to_vec(&task_data, bincode::config::standard()).unwrap();

    // Execute the task
    let result = task_runner
        .submit_task(
            "flatmap-task-1".to_string(),
            "stage-1".to_string(),
            0,
            serialized_task,
        )
        .await
        .unwrap();

    // Verify the result (should be duplicated: [1, 1, 2, 2, 3, 3])
    let result_data: Vec<i32> =
        bincode::decode_from_slice(&result.result.unwrap(), bincode::config::standard())
            .unwrap()
            .0;

    let expected: Vec<i32> = vec![1, 1, 2, 2, 3, 3];
    assert_eq!(result_data, expected);
}

#[tokio::test]
#[traced_test]
async fn test_collect_operation() {
    let task_runner = TaskRunner::new(4);

    // Create test data
    let partition_data = vec![1, 2, 3, 4, 5];

    // Test Collect operation (should return data as-is)
    let collect_operation = RddOperation::Collect;

    let task_data = TaskData {
        partition_data: bincode::encode_to_vec(&partition_data, bincode::config::standard())
            .unwrap(),
        operation: collect_operation,
    };

    let serialized_task = bincode::encode_to_vec(&task_data, bincode::config::standard()).unwrap();

    // Execute the task
    let result = task_runner
        .submit_task(
            "collect-task-1".to_string(),
            "stage-1".to_string(),
            0,
            serialized_task,
        )
        .await
        .unwrap();

    // Verify the result (should be unchanged: [1, 2, 3, 4, 5])
    let result_data: Vec<i32> =
        bincode::decode_from_slice(&result.result.unwrap(), bincode::config::standard())
            .unwrap()
            .0;

    assert_eq!(result_data, partition_data);
}

#[tokio::test]
#[traced_test]
async fn test_driver_rdd_task_submission() {
    // Test the Driver's ability to submit RDD tasks
    let driver = Driver::new("test-driver".to_string());

    let partition_data = vec![1, 2, 3, 4, 5];
    let operation = RddOperation::Map {
        closure_data: vec![],
    };

    // Submit an RDD task
    let result = driver
        .submit_rdd_task(
            "test_task".to_string(),
            "test_stage".to_string(),
            0,
            partition_data,
            operation,
            None,
        )
        .await;

    assert!(result.is_ok());

    // Verify task was queued
    let pending_count = driver.pending_task_count().await;
    assert_eq!(pending_count, 1);
}

#[tokio::test]
#[traced_test]
async fn test_sequential_task_execution() {
    // Test that multiple tasks can be executed sequentially
    let task_runner = TaskRunner::new(4);

    // Create and execute multiple tasks
    for i in 0..4 {
        let partition_data = vec![i, i + 1, i + 2];
        let operation = RddOperation::Map {
            closure_data: vec![],
        };

        let task_data = TaskData {
            partition_data: bincode::encode_to_vec(&partition_data, bincode::config::standard())
                .unwrap(),
            operation: operation,
        };

        let serialized_task =
            bincode::encode_to_vec(&task_data, bincode::config::standard()).unwrap();

        // Execute task
        let result = task_runner
            .submit_task(
                format!("task-{}", i),
                "stage-seq".to_string(),
                i as usize,
                serialized_task,
            )
            .await
            .unwrap();

        // Verify task completed successfully
        assert_eq!(
            result.state,
            barks_core::distributed::types::TaskState::Finished
        );
        assert!(result.result.is_some());

        // Verify the result (should be doubled values)
        let result_data: Vec<i32> =
            bincode::decode_from_slice(&result.result.unwrap(), bincode::config::standard())
                .unwrap()
                .0;

        let expected: Vec<i32> = partition_data.iter().map(|x| x * 2).collect();
        assert_eq!(result_data, expected);
    }
}
