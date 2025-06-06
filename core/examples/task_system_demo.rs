//! Demonstration of the new Task trait system
//!
//! This example shows how the new Task trait object system with typetag
//! solves the hardcoding problem and enables extensible distributed computing.

use async_trait::async_trait;
use barks_core::distributed::task::{DataMapTask, Task, TaskRunner};
use serde::{Deserialize, Serialize};

/// A custom task that demonstrates the extensibility of the new system
#[derive(Serialize, Deserialize)]
pub struct CustomSquareTask {
    pub partition_data: Vec<u8>,
}

#[typetag::serde]
#[async_trait::async_trait]
impl Task for CustomSquareTask {
    async fn execute(&self, _partition_index: usize) -> Result<Vec<u8>, anyhow::Error> {
        // Deserialize the data as Vec<i32>
        let (data, _): (Vec<i32>, _) =
            bincode::decode_from_slice(&self.partition_data, bincode::config::standard())?;

        // Square each element
        let result: Vec<i32> = data.iter().map(|x| x * x).collect();

        // Serialize the result back
        bincode::encode_to_vec(&result, bincode::config::standard())
            .map_err(|e| anyhow::anyhow!("Failed to serialize result: {}", e))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Task System Demo ===");

    // Create a task runner
    let task_runner = TaskRunner::new(4);

    // Test 1: DataMapTask with map_double_i32 operation
    println!("\n1. Testing DataMapTask with map_double_i32:");
    let test_data = vec![1, 2, 3, 4, 5];
    let serialized_data = bincode::encode_to_vec(&test_data, bincode::config::standard())?;

    let task: Box<dyn Task> = Box::new(DataMapTask {
        partition_data: serialized_data,
        operation_type: "map_double_i32".to_string(),
    });

    let serialized_task = serde_json::to_vec(&task)?;
    let result = task_runner.submit_task(0, serialized_task).await;

    if let Some(result_bytes) = result.result {
        let (result_data, _): (Vec<i32>, _) =
            bincode::decode_from_slice(&result_bytes, bincode::config::standard())?;
        println!("Input: {:?}", test_data);
        println!("Output (doubled): {:?}", result_data);
    }

    // Test 2: DataMapTask with filter_even_i32 operation
    println!("\n2. Testing DataMapTask with filter_even_i32:");
    let test_data = vec![1, 2, 3, 4, 5, 6, 7, 8];
    let serialized_data = bincode::encode_to_vec(&test_data, bincode::config::standard())?;

    let task: Box<dyn Task> = Box::new(DataMapTask {
        partition_data: serialized_data,
        operation_type: "filter_even_i32".to_string(),
    });

    let serialized_task = serde_json::to_vec(&task)?;
    let result = task_runner.submit_task(0, serialized_task).await;

    if let Some(result_bytes) = result.result {
        let (result_data, _): (Vec<i32>, _) =
            bincode::decode_from_slice(&result_bytes, bincode::config::standard())?;
        println!("Input: {:?}", test_data);
        println!("Output (even numbers): {:?}", result_data);
    }

    // Test 3: Custom task (demonstrates extensibility)
    println!("\n3. Testing CustomSquareTask (demonstrates extensibility):");
    let test_data = vec![1, 2, 3, 4, 5];
    let serialized_data = bincode::encode_to_vec(&test_data, bincode::config::standard())?;

    let task: Box<dyn Task> = Box::new(CustomSquareTask {
        partition_data: serialized_data,
    });

    let serialized_task = serde_json::to_vec(&task)?;
    let result = task_runner.submit_task(0, serialized_task).await;

    if let Some(result_bytes) = result.result {
        let (result_data, _): (Vec<i32>, _) =
            bincode::decode_from_slice(&result_bytes, bincode::config::standard())?;
        println!("Input: {:?}", test_data);
        println!("Output (squared): {:?}", result_data);
    }

    // Test 4: Demonstrate serialization/deserialization
    println!("\n4. Testing task serialization/deserialization:");
    let test_data = vec![2, 4, 6];
    let serialized_data = bincode::encode_to_vec(&test_data, bincode::config::standard())?;

    let original_task: Box<dyn Task> = Box::new(DataMapTask {
        partition_data: serialized_data,
        operation_type: "map_double_i32".to_string(),
    });

    // Serialize the task
    let serialized_task = serde_json::to_vec(&original_task)?;
    println!("Serialized task size: {} bytes", serialized_task.len());

    // Deserialize the task
    let deserialized_task: Box<dyn Task> = serde_json::from_slice(&serialized_task)?;

    // Execute both tasks and compare results
    let original_result = original_task.execute(0).await?;
    let deserialized_result = deserialized_task.execute(0).await?;

    println!(
        "Original and deserialized tasks produce identical results: {}",
        original_result == deserialized_result
    );

    println!("\n=== Demo Complete ===");
    println!("Key benefits of the new Task system:");
    println!("1. ✅ No hardcoding - new operations can be added without changing Executor code");
    println!("2. ✅ Serializable - tasks can be sent over the network using typetag + serde_json");
    println!("3. ✅ Extensible - custom tasks can be easily added (like CustomSquareTask)");
    println!("4. ✅ Type-safe - Rust's type system ensures correctness at compile time");

    Ok(())
}
