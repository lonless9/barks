//! Test for the new Task trait system
//!
//! This test demonstrates that the new Task trait system works correctly
//! and solves the hardcoding problem.

use barks_core::distributed::task::{DataMapTask, Task, TaskRunner};
use tracing_test::traced_test;

#[tokio::test]
#[traced_test]
async fn test_task_direct_execution() {
    // Test direct task execution without TaskRunner
    let test_data = vec![1, 2, 3, 4, 5];
    let serialized_data = bincode::encode_to_vec(&test_data, bincode::config::standard()).unwrap();
    let task = DataMapTask {
        partition_data: serialized_data,
        operation_type: "map_double_i32".to_string(),
    };
    let result = task.execute(0).await.unwrap();

    let result_data: Vec<i32> = bincode::decode_from_slice(&result, bincode::config::standard())
        .unwrap()
        .0;

    assert_eq!(result_data, vec![2, 4, 6, 8, 10]);

    // Test DataMapTask
    let test_data2 = vec![1, 2, 3];
    let serialized_data2 =
        bincode::encode_to_vec(&test_data2, bincode::config::standard()).unwrap();
    let task = DataMapTask {
        partition_data: serialized_data2,
        operation_type: "map_double_i32".to_string(),
    };
    let result = task.execute(0).await.unwrap();

    let result_data: Vec<i32> = bincode::decode_from_slice(&result, bincode::config::standard())
        .unwrap()
        .0;

    assert_eq!(result_data, vec![2, 4, 6]);
}

#[tokio::test]
#[traced_test]
async fn test_new_task_system_data_map_task_execution() {
    // Test the new task system using the TaskRunner
    let task_runner = TaskRunner::new(4);

    // Create a DataMapTask
    let test_data = vec![1, 2, 3, 4, 5];
    let serialized_data = bincode::encode_to_vec(&test_data, bincode::config::standard()).unwrap();
    let task: Box<dyn Task> = Box::new(DataMapTask {
        partition_data: serialized_data,
        operation_type: "map_double_i32".to_string(),
    });

    // Serialize the task using serde_json (for typetag compatibility)
    let serialized_task = serde_json::to_vec(&task).unwrap();
    println!(
        "Serialized task: {:?}",
        String::from_utf8_lossy(&serialized_task)
    );

    // Submit the task
    let result = task_runner.submit_task(0, serialized_task).await;

    // Check that the task completed successfully
    println!("Task result: {:?}", result);

    if let Some(error_msg) = &result.error_message {
        println!("Task error: {}", error_msg);
    }

    assert!(result.result.is_some(), "Task result should not be None");

    // Deserialize the result
    let result_data: Vec<i32> =
        bincode::decode_from_slice(&result.result.unwrap(), bincode::config::standard())
            .unwrap()
            .0;

    // The DataMapTask should double [1, 2, 3, 4, 5] to [2, 4, 6, 8, 10]
    assert_eq!(result_data, vec![2, 4, 6, 8, 10]);
}

#[tokio::test]
#[traced_test]
async fn test_new_task_system_with_different_ops() {
    // Test the new DataMapTask with different operations
    let task_runner = TaskRunner::new(4);

    // Test "map_double_i32" operation
    let test_data = vec![1, 2, 3, 4, 5];
    let serialized_data = bincode::encode_to_vec(&test_data, bincode::config::standard()).unwrap();
    let task: Box<dyn Task> = Box::new(DataMapTask {
        partition_data: serialized_data,
        operation_type: "map_double_i32".to_string(),
    });

    let serialized_task = serde_json::to_vec(&task).unwrap();

    let result = task_runner.submit_task(0, serialized_task).await;

    // Check if the task failed
    if let Some(error_msg) = &result.error_message {
        panic!("Task failed with error: {}", error_msg);
    }

    assert!(result.result.is_some(), "Task result should not be None");

    let result_data: Vec<i32> =
        bincode::decode_from_slice(&result.result.unwrap(), bincode::config::standard())
            .unwrap()
            .0;

    assert_eq!(result_data, vec![2, 4, 6, 8, 10]);

    // Test "filter_even_i32" operation
    let test_data = vec![1, 2, 3, 4, 5, 6];
    let serialized_data = bincode::encode_to_vec(&test_data, bincode::config::standard()).unwrap();
    let task: Box<dyn Task> = Box::new(DataMapTask {
        partition_data: serialized_data,
        operation_type: "filter_even_i32".to_string(),
    });

    let serialized_task = serde_json::to_vec(&task).unwrap();

    let result = task_runner.submit_task(0, serialized_task).await;

    let result_data: Vec<i32> =
        bincode::decode_from_slice(&result.result.unwrap(), bincode::config::standard())
            .unwrap()
            .0;

    assert_eq!(result_data, vec![2, 4, 6]);

    // Test "collect" operation
    let test_data = vec![10, 20, 30];
    let serialized_data = bincode::encode_to_vec(&test_data, bincode::config::standard()).unwrap();
    let task: Box<dyn Task> = Box::new(DataMapTask {
        partition_data: serialized_data,
        operation_type: "collect".to_string(),
    });

    let serialized_task = serde_json::to_vec(&task).unwrap();

    let result = task_runner.submit_task(0, serialized_task).await;

    let result_data: Vec<i32> =
        bincode::decode_from_slice(&result.result.unwrap(), bincode::config::standard())
            .unwrap()
            .0;

    assert_eq!(result_data, vec![10, 20, 30]);
}

#[tokio::test]
#[traced_test]
async fn test_task_serialization_deserialization() {
    // Test that tasks can be properly serialized and deserialized

    // Test DataMapTask serialization
    let test_data = vec![1, 2, 3, 4, 5];
    let serialized_data = bincode::encode_to_vec(&test_data, bincode::config::standard()).unwrap();
    let task1: Box<dyn Task> = Box::new(DataMapTask {
        partition_data: serialized_data,
        operation_type: "map_double_i32".to_string(),
    });
    let serialized = serde_json::to_vec(&task1).unwrap();
    let deserialized: Box<dyn Task> = serde_json::from_slice(&serialized).unwrap();

    // Execute the deserialized task
    let result = deserialized.execute(0).await.unwrap();
    let result_data: Vec<i32> = bincode::decode_from_slice(&result, bincode::config::standard())
        .unwrap()
        .0;
    assert_eq!(result_data, vec![2, 4, 6, 8, 10]);

    // Test another DataMapTask
    let test_data2 = vec![10, 20, 30];
    let serialized_data2 =
        bincode::encode_to_vec(&test_data2, bincode::config::standard()).unwrap();
    let task2: Box<dyn Task> = Box::new(DataMapTask {
        partition_data: serialized_data2,
        operation_type: "map_double_i32".to_string(),
    });
    let serialized = serde_json::to_vec(&task2).unwrap();
    let deserialized: Box<dyn Task> = serde_json::from_slice(&serialized).unwrap();

    let result = deserialized.execute(0).await.unwrap();
    let result_data: Vec<i32> = bincode::decode_from_slice(&result, bincode::config::standard())
        .unwrap()
        .0;
    assert_eq!(result_data, vec![20, 40, 60]);
}
