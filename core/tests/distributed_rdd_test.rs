//! Tests for distributed RDD task execution logic
//!
//! These tests verify that the distributed computing framework
//! correctly executes different RDD operations using the new task system.

use barks_core::distributed::task::{DataMapTask, Task};
use tracing_test::traced_test;

#[tokio::test]
#[traced_test]
async fn test_map_operation_task() {
    // Create test data
    let partition_data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let serialized_data =
        bincode::encode_to_vec(&partition_data, bincode::config::standard()).unwrap();

    // Test Map operation
    let task = DataMapTask {
        partition_data: serialized_data,
        operation_type: "map_double_i32".to_string(),
    };

    // Execute the task directly to test its logic
    let result_bytes = task.execute(0).await.unwrap();

    // Verify the result (should be doubled values: [2, 4, 6, 8, 10, 12, 14, 16, 18, 20])
    let result_data: Vec<i32> =
        bincode::decode_from_slice(&result_bytes, bincode::config::standard())
            .unwrap()
            .0;

    let expected: Vec<i32> = partition_data.iter().map(|x| x * 2).collect();
    assert_eq!(result_data, expected);
}

#[tokio::test]
#[traced_test]
async fn test_filter_operation_task() {
    // Create test data with both even and odd numbers
    let partition_data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let serialized_data =
        bincode::encode_to_vec(&partition_data, bincode::config::standard()).unwrap();

    // Test Filter operation (should keep only even numbers)
    let task = DataMapTask {
        partition_data: serialized_data,
        operation_type: "filter_even_i32".to_string(),
    };

    // Execute the task
    let result_bytes = task.execute(0).await.unwrap();

    // Verify the result (should be even numbers: [2, 4, 6, 8, 10])
    let result_data: Vec<i32> =
        bincode::decode_from_slice(&result_bytes, bincode::config::standard())
            .unwrap()
            .0;

    let expected: Vec<i32> = vec![2, 4, 6, 8, 10];
    assert_eq!(result_data, expected);
}

#[tokio::test]
#[traced_test]
async fn test_collect_operation() {
    // Create test data
    let partition_data = vec![1, 2, 3, 4, 5];
    let serialized_data =
        bincode::encode_to_vec(&partition_data, bincode::config::standard()).unwrap();

    // Test Collect operation (should return data as-is)
    let task = DataMapTask {
        partition_data: serialized_data,
        operation_type: "collect".to_string(),
    };

    // Execute the task
    let result_bytes = task.execute(0).await.unwrap();

    // Verify the result (should be unchanged: [1, 2, 3, 4, 5])
    let result_data: Vec<i32> =
        bincode::decode_from_slice(&result_bytes, bincode::config::standard())
            .unwrap()
            .0;

    assert_eq!(result_data, partition_data);
}
