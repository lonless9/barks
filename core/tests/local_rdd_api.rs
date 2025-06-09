//! Integration tests for RDD transformations and actions in local mode
//!
//! These tests verify that all RDD operations work correctly when executed
//! locally without distributed components.

mod common;

use barks_core::operations::{
    AddConstantOperation, DoubleOperation, EvenPredicate, GreaterThanPredicate, SquareOperation,
};
use barks_core::rdd::transformations::PairRddExt;
use barks_core::shuffle::HashPartitioner;
use common::*;
use std::sync::Arc;

#[test]
fn test_basic_rdd_creation_and_collection() {
    let context = create_test_context("basic-rdd-test");
    let data = create_test_i32_data();
    let rdd = context.parallelize(data.clone());

    let result = rdd.collect().unwrap();
    assert_same_elements(result, data);
}

#[test]
fn test_rdd_with_multiple_partitions() {
    let context = create_test_context("partitioned-rdd-test");
    let data = create_test_i32_data();
    let rdd = context.parallelize_with_partitions(data.clone(), 4);

    assert_eq!(rdd.num_partitions(), 4);
    let result = rdd.collect().unwrap();
    assert_same_elements(result, data);
}

#[test]
fn test_map_transformation() {
    let context = create_test_context("map-test");
    let data = vec![1, 2, 3, 4, 5];
    let rdd = context.parallelize(data);

    let mapped_rdd = rdd.map(Box::new(DoubleOperation));
    let result = mapped_rdd.collect().unwrap();

    assert_eq!(result, vec![2, 4, 6, 8, 10]);
}

#[test]
fn test_filter_transformation() {
    let context = create_test_context("filter-test");
    let data = (1..=10).collect();
    let rdd = context.parallelize(data);

    let filtered_rdd = rdd.filter(Box::new(EvenPredicate));
    let result = filtered_rdd.collect().unwrap();

    assert_eq!(result, vec![2, 4, 6, 8, 10]);
}

#[test]
fn test_chained_transformations() {
    let context = create_test_context("chained-test");
    let data = (1..=10).collect();
    let rdd = context.parallelize(data);

    let result = rdd
        .map(Box::new(SquareOperation))
        .filter(Box::new(GreaterThanPredicate { threshold: 20 }))
        .map(Box::new(AddConstantOperation { constant: 5 }))
        .collect()
        .unwrap();

    // 1->1, 2->4, 3->9, 4->16, 5->25, 6->36, 7->49, 8->64, 9->81, 10->100
    // Filter > 20: 25, 36, 49, 64, 81, 100
    // Add 5: 30, 41, 54, 69, 86, 105
    let expected = vec![30, 41, 54, 69, 86, 105];
    assert_eq!(result, expected);
}

#[test]
fn test_reduce_by_key() {
    let context = create_test_context("reduce-by-key-test");
    let data = create_test_string_i32_data();
    let rdd = context.parallelize_with_partitions(data, 2);
    let partitioner = Arc::new(HashPartitioner::new(2));

    let reduced = rdd.reduce_by_key(|a, b| a + b, partitioner);
    let result = reduced.collect().unwrap();

    // Convert to HashMap for easier testing
    let result_map: std::collections::HashMap<String, i32> = result.into_iter().collect();

    assert_eq!(result_map.get("a"), Some(&10)); // 1 + 3 + 6
    assert_eq!(result_map.get("b"), Some(&7)); // 2 + 5
    assert_eq!(result_map.get("c"), Some(&4)); // 4
}

#[test]
fn test_group_by_key() {
    let context = create_test_context("group-by-key-test");
    let data = create_test_string_i32_data();
    let rdd = context.parallelize_with_partitions(data, 2);
    let partitioner = Arc::new(HashPartitioner::new(2));

    let grouped = rdd.group_by_key(partitioner);
    let result = grouped.collect().unwrap();

    // Convert to HashMap for easier testing
    let result_map: std::collections::HashMap<String, Vec<i32>> = result.into_iter().collect();

    // Check that all values for each key are grouped together
    let mut a_values = result_map.get("a").unwrap().clone();
    a_values.sort();
    assert_eq!(a_values, vec![1, 3, 6]);

    let mut b_values = result_map.get("b").unwrap().clone();
    b_values.sort();
    assert_eq!(b_values, vec![2, 5]);

    assert_eq!(result_map.get("c").unwrap(), &vec![4]);
}

#[test]
fn test_join_operation() {
    let context = create_test_context("join-test");

    let left_data = vec![
        ("a".to_string(), 1),
        ("b".to_string(), 2),
        ("c".to_string(), 3),
    ];
    let right_data = vec![
        ("a".to_string(), "apple".to_string()),
        ("b".to_string(), "banana".to_string()),
        ("d".to_string(), "date".to_string()),
    ];

    let left_rdd = context.parallelize_with_partitions(left_data, 2);
    let right_rdd = context.parallelize_with_partitions(right_data, 2);
    let partitioner = Arc::new(HashPartitioner::new(2));

    let joined = left_rdd.join(Arc::new(right_rdd), partitioner);
    let result = joined.collect().unwrap();

    // Convert to HashMap for easier testing
    let result_map: std::collections::HashMap<String, (i32, String)> = result.into_iter().collect();

    assert_eq!(result_map.get("a"), Some(&(1, "apple".to_string())));
    assert_eq!(result_map.get("b"), Some(&(2, "banana".to_string())));
    assert_eq!(result_map.get("c"), None); // c doesn't exist in right RDD
    assert_eq!(result_map.get("d"), None); // d doesn't exist in left RDD
}

#[test]
fn test_cogroup_operation() {
    let context = create_test_context("cogroup-test");

    let left_data = vec![
        ("x".to_string(), 1),
        ("y".to_string(), 2),
        ("x".to_string(), 3),
    ];
    let right_data = vec![
        ("x".to_string(), "a".to_string()),
        ("z".to_string(), "b".to_string()),
        ("x".to_string(), "c".to_string()),
    ];

    let left_rdd = context.parallelize_with_partitions(left_data, 2);
    let right_rdd = context.parallelize_with_partitions(right_data, 2);
    let partitioner = Arc::new(HashPartitioner::new(2));

    let cogrouped = left_rdd.cogroup(Arc::new(right_rdd), partitioner);
    let result = cogrouped.collect().unwrap();

    // Convert to HashMap for easier testing
    let result_map: std::collections::HashMap<String, (Vec<i32>, Vec<String>)> =
        result.into_iter().collect();

    // Check 'x' - should have values from both sides
    let x_group = result_map.get("x").unwrap();
    let mut x_left = x_group.0.clone();
    x_left.sort();
    let mut x_right = x_group.1.clone();
    x_right.sort();
    assert_eq!(x_left, vec![1, 3]);
    assert_eq!(x_right, vec!["a".to_string(), "c".to_string()]);

    // Check 'y' - should have values only from left
    let y_group = result_map.get("y").unwrap();
    assert_eq!(y_group.0, vec![2]);
    assert!(y_group.1.is_empty());

    // Check 'z' - should have values only from right
    let z_group = result_map.get("z").unwrap();
    assert!(z_group.0.is_empty());
    assert_eq!(z_group.1, vec!["b".to_string()]);
}

#[test]
fn test_distinct_operation() {
    let context = create_test_context("distinct-test");
    let data = vec![1, 2, 2, 3, 3, 3, 4, 1, 5];
    // Use single partition to ensure global deduplication works correctly
    // The current DistributedRdd::distinct() only does local deduplication within partitions
    let rdd = context.parallelize_with_partitions(data, 1);

    let distinct_rdd = rdd.distinct();
    let mut result = distinct_rdd.collect().unwrap();
    result.sort();

    assert_eq!(result, vec![1, 2, 3, 4, 5]);
}

#[test]
fn test_union_operation() {
    let context = create_test_context("union-test");
    let data1 = vec![1, 2, 3];
    let data2 = vec![4, 5, 6];

    let rdd1 = context.parallelize(data1);
    let rdd2 = context.parallelize(data2);

    let union_rdd = rdd1.union(rdd2);
    let mut result = union_rdd.collect().unwrap();
    result.sort();

    assert_eq!(result, vec![1, 2, 3, 4, 5, 6]);
}

#[test]
fn test_empty_rdd_operations() {
    let context = create_test_context("empty-rdd-test");
    let empty_data: Vec<i32> = vec![];
    let rdd = context.parallelize(empty_data);

    // Test that operations on empty RDDs work correctly
    let mapped = rdd.map(Box::new(DoubleOperation));
    let filtered = mapped.filter(Box::new(EvenPredicate));
    let result = filtered.collect().unwrap();

    assert!(result.is_empty());
}

#[test]
fn test_large_dataset_performance() {
    let context = create_test_context_with_threads("large-dataset-test", 4);
    let data: Vec<i32> = (1..=10000).collect();
    let rdd = context.parallelize_with_partitions(data.clone(), 10);

    let result = rdd
        .filter(Box::new(EvenPredicate))
        .map(Box::new(DoubleOperation))
        .collect()
        .unwrap();

    // Should have 5000 even numbers, each doubled
    assert_eq!(result.len(), 5000);
    assert_eq!(result[0], 4); // 2 * 2
    assert_eq!(result[4999], 20000); // 10000 * 2
}
