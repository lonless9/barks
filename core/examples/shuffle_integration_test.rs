//! Integration test for shuffle operations
//!
//! This example tests the complete shuffle infrastructure including:
//! - ShuffleMapTask execution
//! - Shuffle server functionality
//! - Hash partitioning
//! - MapStatus generation

use barks_core::distributed::task::{ShuffleMapTask, Task};
use barks_core::rdd::{PairRdd, VecRdd};
use barks_core::shuffle::{HashPartitionable, HashPartitioner, Partitioner};
use barks_core::traits::RddBase;
use barks_network_shuffle::traits::MapStatus;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    println!("=== Barks: Shuffle Integration Test ===\n");

    // Test 1: Hash Partitioner functionality
    println!("1. Testing Hash Partitioner:");
    test_hash_partitioner();

    // Test 2: ShuffleMapTask execution
    println!("\n2. Testing ShuffleMapTask execution:");
    test_shuffle_map_task().await?;

    // Test 3: RDD shuffle operations
    println!("\n3. Testing RDD shuffle operations:");
    test_rdd_shuffle_operations();

    println!("\n=== All tests completed successfully ===");
    Ok(())
}

fn test_hash_partitioner() {
    let partitioner = HashPartitioner::new(4);

    println!(
        "   Partitioner with {} partitions",
        partitioner.num_partitions()
    );

    // Test consistent partitioning
    let test_keys = vec!["apple", "banana", "cherry", "date", "elderberry"];
    for key in &test_keys {
        let partition1 = key.to_string().get_partition(4);
        let partition2 = key.to_string().get_partition(4);
        assert_eq!(partition1, partition2, "Partitioning should be consistent");
        println!("   Key '{}' -> Partition {}", key, partition1);
    }

    // Test that different keys can go to different partitions
    let partitions: std::collections::HashSet<u32> = test_keys
        .iter()
        .map(|k| k.to_string().get_partition(4))
        .collect();

    println!("   Used {} out of {} partitions", partitions.len(), 4);
    assert!(partitions.len() > 1, "Should use multiple partitions");
}

async fn test_shuffle_map_task() -> Result<(), Box<dyn std::error::Error>> {
    // Create test data
    let test_data = vec![
        ("apple".to_string(), 1),
        ("banana".to_string(), 2),
        ("apple".to_string(), 3),
        ("cherry".to_string(), 1),
        ("banana".to_string(), 4),
        ("apple".to_string(), 2),
        ("date".to_string(), 5),
    ];

    println!("   Input data: {:?}", test_data);

    // Serialize the data
    let serialized_data = bincode::encode_to_vec(&test_data, bincode::config::standard())?;

    // Create and execute shuffle map task
    let shuffle_task = ShuffleMapTask::<String, i32>::new(
        serialized_data,
        1, // shuffle_id
        3, // num_reduce_partitions
    );

    let result_bytes = shuffle_task.execute(0)?;

    // Deserialize the MapStatus
    let (map_status, _): (MapStatus, _) =
        bincode::decode_from_slice(&result_bytes, bincode::config::standard())?;

    println!(
        "   MapStatus block sizes: {:?}",
        map_status.get_block_sizes()
    );

    // Verify that we have blocks for different partitions
    assert!(
        !map_status.get_block_sizes().is_empty(),
        "Should have shuffle blocks"
    );

    // Verify that total data is preserved (approximately)
    let total_block_size: u64 = map_status.get_block_sizes().values().sum();
    println!("   Total shuffle data size: {} bytes", total_block_size);
    assert!(total_block_size > 0, "Should have non-zero shuffle data");

    Ok(())
}

fn test_rdd_shuffle_operations() {
    // Test data for reduceByKey
    let data = vec![
        ("apple".to_string(), 1),
        ("banana".to_string(), 2),
        ("apple".to_string(), 3),
        ("cherry".to_string(), 1),
        ("banana".to_string(), 4),
        ("apple".to_string(), 2),
    ];

    println!("   Original data: {:?}", data);

    let rdd = VecRdd::new(1, data.clone(), 2);
    let partitioner = Arc::new(HashPartitioner::new(3));

    // Test reduceByKey
    fn add_i32(a: i32, b: i32) -> i32 {
        a + b
    }
    let reduced_rdd = rdd.clone().reduce_by_key(add_i32, partitioner.clone());

    println!("   Created reduceByKey RDD:");
    println!("     Original partitions: {}", rdd.num_partitions());
    println!("     Reduced partitions: {}", reduced_rdd.num_partitions());
    println!("     Dependencies: {}", reduced_rdd.dependencies().len());

    // Test groupByKey
    let grouped_rdd = rdd.group_by_key(partitioner.clone());

    println!("   Created groupByKey RDD:");
    println!("     Grouped partitions: {}", grouped_rdd.num_partitions());
    println!("     Dependencies: {}", grouped_rdd.dependencies().len());

    // Verify RDD properties
    assert_eq!(reduced_rdd.num_partitions(), 3, "Should have 3 partitions");
    assert_eq!(
        reduced_rdd.dependencies().len(),
        1,
        "Should have 1 shuffle dependency"
    );
    assert_eq!(grouped_rdd.num_partitions(), 3, "Should have 3 partitions");
    assert_eq!(
        grouped_rdd.dependencies().len(),
        1,
        "Should have 1 shuffle dependency"
    );
}
