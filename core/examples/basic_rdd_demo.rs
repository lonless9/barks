//! Basic Demo - Single-Machine Core Abstractions
//!
//! This example demonstrates the basic core RDD abstractions implemented:
//! - VecRdd for collections
//! - Map and Filter transformations
//! - Lazy evaluation
//! - Actions (collect, count, take, first)
//! - FlowContext for local execution

use barks_core::{FlowContext, SimpleRdd};

fn main() {
    println!("=== Barks: Single-Machine Core Abstractions Demo ===\n");

    // Create a FlowContext for managing RDD operations
    let context = FlowContext::new("barks-phase0-demo");
    println!("Created FlowContext: '{}'", context.app_name());

    // Demo 1: Basic RDD creation and collection
    println!("\n1. Basic RDD Creation and Collection:");
    let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    println!("   Original data: {:?}", data);

    let rdd = context.parallelize_with_partitions(data.clone(), 3);
    println!("   Created RDD with {} partitions", rdd.num_partitions());

    let collected = rdd.collect().unwrap();
    println!("   Collected result: {:?}", collected);

    // Demo 2: Map transformation
    println!("\n2. Map Transformation (multiply by 2):");
    let rdd = SimpleRdd::from_vec(vec![1, 2, 3, 4, 5]);
    let mapped_rdd = rdd.map(|x| x * 2);
    let result = mapped_rdd.collect().unwrap();
    println!("   Input: [1, 2, 3, 4, 5]");
    println!("   Map(x * 2): {:?}", result);

    // Demo 3: Filter transformation
    println!("\n3. Filter Transformation (keep even numbers):");
    let rdd = SimpleRdd::from_vec(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let filtered_rdd = rdd.filter(|&x| x % 2 == 0);
    let result = filtered_rdd.collect().unwrap();
    println!("   Input: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]");
    println!("   Filter(even): {:?}", result);

    // Demo 4: Chained transformations (map -> filter)
    println!("\n4. Chained Transformations (map -> filter):");
    let rdd = SimpleRdd::from_vec(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let result_rdd = rdd
        .map(|x| x * 3) // Multiply by 3
        .filter(|&x| x > 15); // Keep numbers > 15
    let result = result_rdd.collect().unwrap();
    println!("   Input: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]");
    println!("   Map(x * 3) -> Filter(> 15): {:?}", result);

    // Demo 5: Chained transformations (filter -> map)
    println!("\n5. Chained Transformations (filter -> map):");
    let rdd = SimpleRdd::from_vec(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let result_rdd = rdd
        .filter(|&x| x > 5) // Keep numbers > 5
        .map(|x| x * x); // Square them
    let result = result_rdd.collect().unwrap();
    println!("   Input: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]");
    println!("   Filter(> 5) -> Map(x²): {:?}", result);

    // Demo 6: Various actions
    println!("\n6. RDD Actions:");
    let rdd = SimpleRdd::from_vec(vec![10, 20, 30, 40, 50]);

    let count = rdd.count().unwrap();
    println!("   Count: {}", count);

    let first = rdd.first().unwrap();
    println!("   First: {:?}", first);

    let take_3 = rdd.take(3).unwrap();
    println!("   Take(3): {:?}", take_3);

    // Demo 7: Complex pipeline demonstrating lazy evaluation
    println!("\n7. Complex Pipeline (Lazy Evaluation Demo):");
    let large_data: Vec<i32> = (1..=100).collect();
    println!("   Input: numbers 1 to 100");

    let rdd = context.parallelize_with_partitions(large_data, 4);
    println!("   Created RDD with {} partitions", rdd.num_partitions());

    // Build a complex transformation pipeline
    let complex_rdd = rdd
        .map(|x| x * 2) // Double each number
        .filter(|&x| x % 3 == 0) // Keep multiples of 3
        .map(|x| x + 1); // Add 1

    println!("   Pipeline: Map(x * 2) -> Filter(x % 3 == 0) -> Map(x + 1)");

    // Only now does the computation happen (lazy evaluation)
    let final_result = complex_rdd.collect().unwrap();
    println!("   Result count: {}", final_result.len());
    println!(
        "   First 10 results: {:?}",
        &final_result[..10.min(final_result.len())]
    );

    // Demo 8: Demonstrate serialization bounds
    println!("\n8. Working with Different Data Types:");

    // String data (using owned strings to maintain same type)
    let string_data = vec![
        "hello".to_string(),
        "world".to_string(),
        "rust".to_string(),
        "barks".to_string(),
    ];
    let string_rdd = SimpleRdd::from_vec(string_data);
    let uppercase_result = string_rdd.map(|s| s.to_uppercase()).collect().unwrap();
    println!("   String transformation: {:?}", uppercase_result);

    // Tuple data (using owned strings for serialization)
    let tuple_data = vec![
        (1, "a".to_string()),
        (2, "b".to_string()),
        (3, "c".to_string()),
        (4, "d".to_string()),
    ];
    let tuple_rdd = SimpleRdd::from_vec(tuple_data);
    let filtered_tuples = tuple_rdd.filter(|(num, _)| num % 2 == 0).collect().unwrap();
    println!("   Tuple filtering: {:?}", filtered_tuples);

    println!("\n=== Barks Demo Complete ===");
    println!("\nKey Features Demonstrated:");
    println!("✓ RDD creation from vectors");
    println!("✓ Partitioning support");
    println!("✓ Map transformations");
    println!("✓ Filter transformations");
    println!("✓ Transformation chaining");
    println!("✓ Lazy evaluation");
    println!("✓ Actions: collect, count, take, first");
    println!("✓ FlowContext for local execution");
    println!("✓ Serialization bounds (serde)");
    println!("✓ Type safety and memory safety");
}
