//! Local RDD API Example
//!
//! This example demonstrates the basic core RDD abstractions implemented:
//! - DistributedRdd for collections with serializable operations
//! - Map and Filter transformations using serializable operations
//! - Lazy evaluation
//! - Actions (collect, count, take, first)
//! - Local execution with parallel processing

use barks_core::operations::{
    AddConstantOperation, DoubleOperation, EvenPredicate, GreaterThanPredicate, MinLengthPredicate,
    ToUpperCaseOperation,
};
use barks_core::rdd::DistributedRdd;

fn main() {
    println!("=== Barks: Local RDD API Example ===\n");

    // Demo 1: Basic RDD creation and collection
    println!("1. Basic RDD Creation and Collection:");
    let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    println!("   Original data: {:?}", data);

    let rdd: DistributedRdd<i32> = DistributedRdd::from_vec_with_partitions(data.clone(), 3);
    println!("   Created RDD with {} partitions", rdd.num_partitions());

    let collected = rdd.collect().unwrap();
    println!("   Collected result: {:?}", collected);

    // Demo 2: Map transformation
    println!("\n2. Map Transformation (multiply by 2):");
    let rdd: DistributedRdd<i32> = DistributedRdd::from_vec(vec![1, 2, 3, 4, 5]);
    let mapped_rdd = rdd.map(Box::new(DoubleOperation));
    let result = mapped_rdd.collect().unwrap();
    println!("   Input: [1, 2, 3, 4, 5]");
    println!("   Map(x * 2): {:?}", result);

    // Demo 3: Filter transformation
    println!("\n3. Filter Transformation (keep even numbers):");
    let rdd: DistributedRdd<i32> = DistributedRdd::from_vec(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let filtered_rdd = rdd.filter(Box::new(EvenPredicate));
    let result = filtered_rdd.collect().unwrap();
    println!("   Input: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]");
    println!("   Filter(even): {:?}", result);

    // Demo 4: Chained transformations (map -> filter)
    println!("\n4. Chained Transformations (map -> filter):");
    let rdd: DistributedRdd<i32> = DistributedRdd::from_vec(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let result_rdd = rdd
        .map(Box::new(AddConstantOperation { constant: 10 })) // Add 10
        .filter(Box::new(GreaterThanPredicate { threshold: 15 })); // Keep numbers > 15
    let result = result_rdd.collect().unwrap();
    println!("   Input: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]");
    println!("   Map(x + 10) -> Filter(> 15): {:?}", result);

    // Demo 5: Chained transformations (filter -> map)
    println!("\n5. Chained Transformations (filter -> map):");
    let rdd: DistributedRdd<i32> = DistributedRdd::from_vec(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let result_rdd = rdd
        .filter(Box::new(GreaterThanPredicate { threshold: 5 })) // Keep numbers > 5
        .map(Box::new(DoubleOperation)); // Double them
    let result = result_rdd.collect().unwrap();
    println!("   Input: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]");
    println!("   Filter(> 5) -> Map(x * 2): {:?}", result);

    // Demo 6: Complex pipeline demonstrating lazy evaluation
    println!("\n6. Complex Pipeline (Lazy Evaluation Demo):");
    let large_data: Vec<i32> = (1..=100).collect();
    println!("   Input: numbers 1 to 100");

    let rdd: DistributedRdd<i32> = DistributedRdd::from_vec_with_partitions(large_data, 4);
    println!("   Created RDD with {} partitions", rdd.num_partitions());

    // Build a complex transformation pipeline
    let complex_rdd = rdd
        .map(Box::new(DoubleOperation)) // Double each number
        .filter(Box::new(GreaterThanPredicate { threshold: 50 })) // Keep numbers > 50
        .map(Box::new(AddConstantOperation { constant: 1 })); // Add 1

    println!("   Pipeline: Map(x * 2) -> Filter(x > 50) -> Map(x + 1)");

    // Only now does the computation happen (lazy evaluation)
    let final_result = complex_rdd.collect().unwrap();
    println!("   Result count: {}", final_result.len());
    println!(
        "   First 10 results: {:?}",
        &final_result[..10.min(final_result.len())]
    );

    // Demo 7: Working with String data types
    println!("\n7. Working with String Data Types:");

    // String data (using owned strings to maintain same type)
    let string_data = vec![
        "hello".to_string(),
        "world".to_string(),
        "rust".to_string(),
        "barks".to_string(),
    ];
    let string_rdd: DistributedRdd<String> = DistributedRdd::from_vec(string_data);
    let uppercase_result = string_rdd
        .map(Box::new(ToUpperCaseOperation))
        .collect()
        .unwrap();
    println!("   String transformation: {:?}", uppercase_result);

    // String filtering
    let string_data2 = vec![
        "a".to_string(),
        "hello".to_string(),
        "world".to_string(),
        "rust".to_string(),
    ];
    let string_rdd2: DistributedRdd<String> = DistributedRdd::from_vec(string_data2);
    let filtered_strings = string_rdd2
        .filter(Box::new(MinLengthPredicate { min_length: 4 }))
        .collect()
        .unwrap();
    println!("   String filtering (min length 4): {:?}", filtered_strings);

    println!("\n=== Local RDD API Example Complete ===");
    println!("\nKey Features Demonstrated:");
    println!("✓ RDD creation from vectors");
    println!("✓ Partitioning support");
    println!("✓ Map transformations with serializable operations");
    println!("✓ Filter transformations with serializable predicates");
    println!("✓ Transformation chaining");
    println!("✓ Lazy evaluation");
    println!("✓ Local parallel execution with rayon");
    println!("✓ Serializable operations (no closures)");
    println!("✓ Type safety and memory safety");
}
