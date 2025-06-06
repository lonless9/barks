//! Simple demonstration of the new Task system

use barks_core::distributed::task::{DataMapTask, Task};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Simple Task System Demo ===");
    
    // Test DataMapTask with map_double_i32 operation
    let test_data = vec![1, 2, 3, 4, 5];
    let serialized_data = bincode::encode_to_vec(&test_data, bincode::config::standard())?;
    
    let task = DataMapTask {
        partition_data: serialized_data,
        operation_type: "map_double_i32".to_string(),
    };
    
    println!("Input data: {:?}", test_data);
    
    // Execute the task directly
    let result_bytes = task.execute(0).await?;
    
    // Deserialize the result
    let (result_data, _): (Vec<i32>, _) = 
        bincode::decode_from_slice(&result_bytes, bincode::config::standard())?;
    
    println!("Output (doubled): {:?}", result_data);
    
    // Test serialization
    let task_box: Box<dyn Task> = Box::new(task);
    let serialized_task = serde_json::to_vec(&task_box)?;
    println!("Task serialized to {} bytes", serialized_task.len());
    
    // Deserialize and execute
    let deserialized_task: Box<dyn Task> = serde_json::from_slice(&serialized_task)?;
    let result2 = deserialized_task.execute(0).await?;
    
    println!("Serialization/deserialization works: {}", result_bytes == result2);
    
    println!("=== Demo Complete ===");
    
    Ok(())
}
