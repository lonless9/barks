//! Tests for distributed RDD task execution logic
//!
//! These tests verify that the distributed computing framework
//! correctly executes different RDD operations using the new task system.

use barks_core::distributed::task::{ChainedI32Task, Task};
use barks_core::operations::{
    DoubleOperation, EvenPredicate, GreaterThanPredicate, SerializableI32Operation,
};
use tracing_test::traced_test;

#[tokio::test]
#[traced_test]
async fn test_map_operation_task() {
    // Create test data
    let partition_data = vec![1, 2, 3, 4, 5];
    let serialized_data =
        bincode::encode_to_vec(&partition_data, bincode::config::standard()).unwrap();

    // Test Map operation
    let task = ChainedI32Task {
        partition_data: serialized_data,
        operations: vec![SerializableI32Operation::Map(Box::new(DoubleOperation))],
    };

    // Execute the task directly to test its logic
    let result_bytes = task.execute(0).await.unwrap();

    // Verify the result
    let result_data: Vec<i32> =
        bincode::decode_from_slice(&result_bytes, bincode::config::standard())
            .unwrap()
            .0;

    assert_eq!(result_data, vec![2, 4, 6, 8, 10]);
}

#[tokio::test]
#[traced_test]
async fn test_filter_operation_task() {
    // Create test data with both even and odd numbers
    let partition_data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let serialized_data =
        bincode::encode_to_vec(&partition_data, bincode::config::standard()).unwrap();

    // Test Filter operation (should keep only even numbers)
    let task = ChainedI32Task {
        partition_data: serialized_data,
        operations: vec![SerializableI32Operation::Filter(Box::new(EvenPredicate))],
    };

    // Execute the task
    let result_bytes = task.execute(0).await.unwrap();

    // Verify the result (should be even numbers: [2, 4, 6, 8, 10])
    let (result_data, _): (Vec<i32>, _) =
        bincode::decode_from_slice(&result_bytes, bincode::config::standard()).unwrap();

    let expected: Vec<i32> = vec![2, 4, 6, 8, 10];
    assert_eq!(result_data, expected);
}

#[tokio::test]
#[traced_test]
async fn test_chained_operation_task() {
    // Create test data
    let partition_data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let serialized_data =
        bincode::encode_to_vec(&partition_data, bincode::config::standard()).unwrap();

    // Test a chain of operations: map(double) -> filter(>10)
    let task = ChainedI32Task {
        partition_data: serialized_data,
        operations: vec![
            SerializableI32Operation::Map(Box::new(DoubleOperation)),
            SerializableI32Operation::Filter(Box::new(GreaterThanPredicate { threshold: 10 })),
        ],
    };

    let result_bytes = task.execute(0).await.unwrap();
    let (result_data, _): (Vec<i32>, _) =
        bincode::decode_from_slice(&result_bytes, bincode::config::standard()).unwrap();
    assert_eq!(result_data, vec![12, 14, 16, 18, 20]);
}
