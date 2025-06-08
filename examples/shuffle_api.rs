//! Shuffle API Example
//!
//! This example demonstrates shuffle operations like reduceByKey and groupByKey
//! that require wide dependencies and data redistribution.

use barks_core::rdd::transformations::PairRdd;
use barks_core::rdd::SimpleRdd;
use barks_core::shuffle::HashPartitioner;
use barks_core::traits::RddBase;
use std::sync::Arc;

fn main() {
    println!("=== Barks: Shuffle API Example ===\n");

    // Demo 1: Basic reduceByKey operation
    println!("1. ReduceByKey Operation:");

    // Create sample key-value data
    let data = vec![
        ("apple".to_string(), 1),
        ("banana".to_string(), 2),
        ("apple".to_string(), 3),
        ("cherry".to_string(), 1),
        ("banana".to_string(), 4),
        ("apple".to_string(), 2),
    ];
    println!("   Original data: {:?}", data);

    // Create an RDD with the data
    let rdd = SimpleRdd::from_vec_with_partitions(data, 2);
    println!("   Created RDD with {} partitions", rdd.num_partitions());

    // Create a hash partitioner for the shuffle
    let partitioner = Arc::new(HashPartitioner::new(2));

    // Perform reduceByKey operation (sum values for each key)
    fn add_i32(a: i32, b: i32) -> i32 {
        a + b
    }
    let reduced_rdd = rdd.clone().reduce_by_key(add_i32, partitioner);
    println!("   Created shuffled RDD for reduceByKey operation");
    println!(
        "   Shuffled RDD has {} partitions",
        reduced_rdd.num_partitions()
    );

    // Demo 2: GroupByKey operation
    println!("\n2. GroupByKey Operation:");

    let data2 = vec![
        ("fruit".to_string(), "apple".to_string()),
        ("color".to_string(), "red".to_string()),
        ("fruit".to_string(), "banana".to_string()),
        ("color".to_string(), "yellow".to_string()),
        ("fruit".to_string(), "cherry".to_string()),
        ("color".to_string(), "red".to_string()),
    ];
    println!("   Original data: {:?}", data2);

    let rdd2 = SimpleRdd::from_vec_with_partitions(data2, 2);
    let partitioner2 = Arc::new(HashPartitioner::new(3));

    let grouped_rdd = rdd2.group_by_key(partitioner2);
    println!("   Created shuffled RDD for groupByKey operation");
    println!(
        "   Grouped RDD has {} partitions",
        grouped_rdd.num_partitions()
    );

    // Demo 3: Show RDD lineage and dependencies
    println!("\n3. RDD Lineage and Dependencies:");
    println!(
        "   Original RDD dependencies: {:?}",
        rdd.dependencies().len()
    );
    println!(
        "   Shuffled RDD dependencies: {:?}",
        reduced_rdd.dependencies().len()
    );

    println!("\n=== Shuffle API Example Complete ===");
    println!("Note: This example shows the RDD creation and shuffle setup.");
    println!("In a distributed environment, the actual shuffle would involve");
    println!("network communication between executors to redistribute data.");
    println!("\nKey Features Demonstrated:");
    println!("✓ ReduceByKey operations");
    println!("✓ GroupByKey operations");
    println!("✓ Hash partitioning");
    println!("✓ Wide dependencies");
    println!("✓ RDD lineage tracking");
}
