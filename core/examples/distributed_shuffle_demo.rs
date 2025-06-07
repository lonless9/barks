//! Distributed shuffle operations demonstration
//!
//! This example shows how shuffle operations like reduceByKey work in a distributed
//! environment with multiple executors. It demonstrates the complete shuffle workflow:
//! 1. Map stage: Partition data by key and write shuffle blocks
//! 2. Shuffle stage: Transfer data between executors
//! 3. Reduce stage: Aggregate values for each key

use barks_core::distributed::task::{ShuffleMapTask, Task};
use barks_core::rdd::{PairRdd, VecRdd};
use barks_core::shuffle::{HashPartitioner, Partitioner};
use barks_core::traits::RddBase;
use barks_network_shuffle::traits::MapStatus;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Barks: Distributed Shuffle Operations Demo ===\n");

    // Demo 1: Simulate a shuffle map task
    println!("1. Shuffle Map Task Simulation:");

    // Create sample key-value data that would be in one partition
    let partition_data = vec![
        ("apple".to_string(), 1),
        ("banana".to_string(), 2),
        ("apple".to_string(), 3),
        ("cherry".to_string(), 1),
        ("banana".to_string(), 4),
        ("apple".to_string(), 2),
    ];
    println!("   Partition data: {:?}", partition_data);

    // Serialize the partition data
    let serialized_data = bincode::encode_to_vec(&partition_data, bincode::config::standard())?;

    // Create a shuffle map task
    let shuffle_task = ShuffleMapTask::<String, i32>::new(
        serialized_data,
        1, // shuffle_id
        3, // num_reduce_partitions
    );

    // Execute the shuffle map task
    let result_bytes = shuffle_task.execute(0)?;

    // Deserialize the MapStatus result
    let (map_status, _): (MapStatus, _) =
        bincode::decode_from_slice(&result_bytes, bincode::config::standard())?;

    println!("   Shuffle map task completed successfully");
    println!("   Block sizes: {:?}", map_status.get_block_sizes());

    // Demo 2: Show how RDD shuffle operations create the right task structure
    println!("\n2. RDD Shuffle Operation Structure:");

    let data = vec![
        ("fruit".to_string(), "apple".to_string()),
        ("color".to_string(), "red".to_string()),
        ("fruit".to_string(), "banana".to_string()),
        ("color".to_string(), "yellow".to_string()),
        ("fruit".to_string(), "cherry".to_string()),
        ("color".to_string(), "red".to_string()),
    ];

    let rdd = VecRdd::new(1, data.clone(), 2);
    let partitioner = Arc::new(HashPartitioner::new(3));

    // Create a shuffled RDD using reduceByKey
    let original_partitions = rdd.num_partitions();
    let original_dependencies = rdd.dependencies().len();

    fn concat_strings(a: String, b: String) -> String {
        format!("{},{}", a, b)
    }
    let shuffled_rdd = rdd.reduce_by_key(concat_strings, partitioner.clone());

    println!("   Original RDD: {} partitions", original_partitions);
    println!(
        "   Shuffled RDD: {} partitions",
        shuffled_rdd.num_partitions()
    );
    println!(
        "   Dependencies: {} -> {}",
        original_dependencies,
        shuffled_rdd.dependencies().len()
    );

    // Demo 3: Show partitioner behavior
    println!("\n3. Hash Partitioner Behavior:");

    let partitioner = HashPartitioner::new(4);
    let partitioner_trait: &dyn Partitioner = &partitioner;
    println!(
        "   Partitioner: {} partitions",
        partitioner_trait.num_partitions()
    );

    let test_keys = vec!["apple", "banana", "cherry", "date", "elderberry"];
    for key in &test_keys {
        let partition = partitioner.get_partition_for(&key.to_string());
        println!("   Key '{}' -> Partition {}", key, partition);
    }

    // Demo 4: Simulate the complete shuffle workflow
    println!("\n4. Complete Shuffle Workflow Simulation:");

    println!("   Step 1: Map stage - Each executor processes its partitions");
    println!("           - Partition data by key using hash partitioner");
    println!("           - Write shuffle blocks to local storage");
    println!("           - Return MapStatus to driver");

    println!("   Step 2: Shuffle stage - Driver coordinates data transfer");
    println!("           - Driver collects all MapStatus from map tasks");
    println!("           - Driver tells reduce tasks where to fetch data");
    println!("           - Reduce tasks fetch data from multiple executors");

    println!("   Step 3: Reduce stage - Aggregate data by key");
    println!("           - Each reduce task processes one output partition");
    println!("           - Apply aggregation function (e.g., sum, concat)");
    println!("           - Return final results to driver");

    println!("\n=== Demo completed ===");
    println!("This demo shows the shuffle infrastructure components.");
    println!("In a real distributed deployment:");
    println!("- Map tasks run on different executors");
    println!("- Shuffle data is transferred over the network");
    println!("- Reduce tasks aggregate data from multiple sources");
    println!("- The driver coordinates the entire process");

    Ok(())
}
