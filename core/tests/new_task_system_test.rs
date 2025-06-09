//! Test for the new Task trait system
//!
//! This test verifies that the `ChainedTask<i32>` can be correctly serialized,
//! deserialized, and executed by a `TaskRunner`.

use barks_core::distributed::task::{ChainedTask, Task, TaskExecutionResult, TaskRunner};
use barks_core::operations::{
    DoubleOperation, EvenPredicate, GreaterThanPredicate, SerializableI32Operation,
};
use std::sync::Arc;
use tracing_test::traced_test;

#[tokio::test]
#[traced_test]
async fn test_task_direct_execution() {
    // Test direct task execution without TaskRunner
    let test_data = vec![1, 2, 3, 4, 5];
    let serialized_data = bincode::encode_to_vec(&test_data, bincode::config::standard()).unwrap();
    let task = ChainedTask::<i32>::new(
        serialized_data,
        vec![SerializableI32Operation::Map(Box::new(DoubleOperation))],
    );
    let block_manager = Arc::new(barks_network_shuffle::shuffle::MemoryShuffleManager::new());
    let result = task.execute(0, block_manager).await.unwrap();

    let result_data: Vec<i32> = bincode::decode_from_slice(&result, bincode::config::standard())
        .unwrap()
        .0;

    assert_eq!(result_data, vec![2, 4, 6, 8, 10]);

    // Test ChainedTask<i32> with filter
    let test_data2 = vec![1, 2, 3, 4, 5, 6];
    let serialized_data2 =
        bincode::encode_to_vec(&test_data2, bincode::config::standard()).unwrap();
    let task = ChainedTask::<i32>::new(
        serialized_data2,
        vec![SerializableI32Operation::Filter(Box::new(EvenPredicate))],
    );
    let block_manager = Arc::new(barks_network_shuffle::shuffle::MemoryShuffleManager::new());
    let result = task.execute(0, block_manager).await.unwrap();

    let result_data: Vec<i32> = bincode::decode_from_slice(&result, bincode::config::standard())
        .unwrap()
        .0;

    assert_eq!(result_data, vec![2, 4, 6]);
}

#[tokio::test]
#[traced_test]
async fn test_task_runner_with_chained_i32_task() {
    let block_manager = Arc::new(barks_network_shuffle::shuffle::MemoryShuffleManager::new());
    let task_runner = TaskRunner::new(4, block_manager);

    // Create a ChainedTask<i32>
    let test_data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let serialized_data = bincode::encode_to_vec(&test_data, bincode::config::standard()).unwrap();
    let task: Box<dyn Task> = Box::new(ChainedTask::<i32>::new(
        serialized_data,
        vec![SerializableI32Operation::Map(Box::new(DoubleOperation))],
    ));

    let serialized_task = serde_json::to_vec(&task).unwrap();

    let result = task_runner
        .submit_task("test_task_1".to_string(), 0, serialized_task)
        .await;
    let result_data = extract_result(result);
    assert_eq!(result_data, vec![2, 4, 6, 8, 10, 12, 14, 16, 18, 20]);

    // Test filter operation
    let serialized_data = bincode::encode_to_vec(&test_data, bincode::config::standard()).unwrap();
    let task: Box<dyn Task> = Box::new(ChainedTask::<i32>::new(
        serialized_data,
        vec![SerializableI32Operation::Filter(Box::new(EvenPredicate))],
    ));
    let serialized_task = serde_json::to_vec(&task).unwrap();
    let result = task_runner
        .submit_task("test_task_2".to_string(), 0, serialized_task)
        .await;
    let result_data = extract_result(result);
    assert_eq!(result_data, vec![2, 4, 6, 8, 10]);

    // Test chained operations
    let serialized_data = bincode::encode_to_vec(&test_data, bincode::config::standard()).unwrap();
    let task: Box<dyn Task> = Box::new(ChainedTask::<i32>::new(
        serialized_data,
        vec![
            SerializableI32Operation::Map(Box::new(DoubleOperation)),
            SerializableI32Operation::Filter(Box::new(GreaterThanPredicate { threshold: 10 })),
        ],
    ));
    let serialized_task = serde_json::to_vec(&task).unwrap();
    let result = task_runner
        .submit_task("test_task_3".to_string(), 0, serialized_task)
        .await;
    let result_data = extract_result(result);
    assert_eq!(result_data, vec![12, 14, 16, 18, 20]);
}

/// Helper function to panic on task failure and extract result data.
fn extract_result(result: TaskExecutionResult) -> Vec<i32> {
    if let Some(error_msg) = &result.error_message {
        panic!("Task failed with error: {}", error_msg);
    }
    assert!(result.result.is_some(), "Task result should not be None");
    bincode::decode_from_slice::<Vec<i32>, _>(&result.result.unwrap(), bincode::config::standard())
        .unwrap()
        .0
}

#[tokio::test]
#[traced_test]
async fn test_task_serialization_deserialization() {
    // Test that tasks can be properly serialized and deserialized

    // Test ChainedTask<i32> serialization
    let test_data = vec![1, 2, 3, 4, 5];
    let serialized_data = bincode::encode_to_vec(&test_data, bincode::config::standard()).unwrap();
    let task1: Box<dyn Task> = Box::new(ChainedTask::<i32>::new(
        serialized_data,
        vec![SerializableI32Operation::Map(Box::new(DoubleOperation))],
    ));
    let serialized = serde_json::to_vec(&task1).unwrap();
    let deserialized: Box<dyn Task> = serde_json::from_slice(&serialized).unwrap();

    // Execute the deserialized task
    let block_manager = Arc::new(barks_network_shuffle::shuffle::MemoryShuffleManager::new());
    let result = deserialized.execute(0, block_manager).await.unwrap();
    let result_data: Vec<i32> = bincode::decode_from_slice(&result, bincode::config::standard())
        .unwrap()
        .0;
    assert_eq!(result_data, vec![2, 4, 6, 8, 10]);

    // Test another ChainedTask<i32>
    let test_data2 = vec![10, 20, 30];
    let serialized_data2 =
        bincode::encode_to_vec(&test_data2, bincode::config::standard()).unwrap();
    let task2: Box<dyn Task> = Box::new(ChainedTask::<i32>::new(
        serialized_data2,
        vec![SerializableI32Operation::Map(Box::new(DoubleOperation))],
    ));
    let serialized = serde_json::to_vec(&task2).unwrap();
    let deserialized: Box<dyn Task> = serde_json::from_slice(&serialized).unwrap();

    let block_manager = Arc::new(barks_network_shuffle::shuffle::MemoryShuffleManager::new());
    let result = deserialized.execute(0, block_manager).await.unwrap();
    let result_data: Vec<i32> = bincode::decode_from_slice(&result, bincode::config::standard())
        .unwrap()
        .0;
    assert_eq!(result_data, vec![20, 40, 60]);
}
