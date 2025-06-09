//! Integration tests for shuffle operations
//!
//! These tests focus specifically on shuffle-based operations like
//! reduceByKey, groupByKey, join, cogroup, and sortByKey.

mod common;

use barks_core::rdd::transformations::PairRddExt;
use barks_core::shuffle::{HashPartitioner, RangePartitioner};
use common::*;
use std::sync::Arc;

#[test]
fn test_reduce_by_key_single_partition() {
    let context = create_test_context("reduce-by-key-single");
    let data = create_test_string_i32_data();
    let rdd = context.parallelize(data);
    let partitioner = Arc::new(HashPartitioner::new(1));

    let result = rdd
        .reduce_by_key(|a, b| a + b, partitioner)
        .collect()
        .unwrap();

    let result_map: std::collections::HashMap<String, i32> = result.into_iter().collect();
    assert_eq!(result_map.get("a"), Some(&10)); // 1 + 3 + 6
    assert_eq!(result_map.get("b"), Some(&7)); // 2 + 5
    assert_eq!(result_map.get("c"), Some(&4)); // 4
}

#[test]
fn test_reduce_by_key_multiple_partitions() {
    let context = create_test_context("reduce-by-key-multi");
    let data = create_test_string_i32_data();
    let rdd = context.parallelize_with_partitions(data, 3);
    let partitioner = Arc::new(HashPartitioner::new(4));

    let result = rdd
        .reduce_by_key(|a, b| a + b, partitioner)
        .collect()
        .unwrap();

    let result_map: std::collections::HashMap<String, i32> = result.into_iter().collect();
    assert_eq!(result_map.get("a"), Some(&10));
    assert_eq!(result_map.get("b"), Some(&7));
    assert_eq!(result_map.get("c"), Some(&4));
}

#[test]
fn test_group_by_key_preserves_all_values() {
    let context = create_test_context("group-by-key-test");
    let data = vec![
        ("x".to_string(), 1),
        ("y".to_string(), 2),
        ("x".to_string(), 3),
        ("z".to_string(), 4),
        ("x".to_string(), 5),
        ("y".to_string(), 6),
    ];
    let rdd = context.parallelize_with_partitions(data, 2);
    let partitioner = Arc::new(HashPartitioner::new(3));

    let result = rdd.group_by_key(partitioner).collect().unwrap();

    let result_map: std::collections::HashMap<String, Vec<i32>> = result.into_iter().collect();

    let mut x_values = result_map.get("x").unwrap().clone();
    x_values.sort();
    assert_eq!(x_values, vec![1, 3, 5]);

    let mut y_values = result_map.get("y").unwrap().clone();
    y_values.sort();
    assert_eq!(y_values, vec![2, 6]);

    assert_eq!(result_map.get("z").unwrap(), &vec![4]);
}

#[test]
fn test_join_inner_join_behavior() {
    let context = create_test_context("join-inner-test");

    let left_data = vec![
        ("a".to_string(), 1),
        ("b".to_string(), 2),
        ("c".to_string(), 3),
        ("a".to_string(), 4), // Duplicate key
    ];
    let right_data = vec![
        ("a".to_string(), "apple".to_string()),
        ("b".to_string(), "banana".to_string()),
        ("d".to_string(), "date".to_string()),
        ("a".to_string(), "apricot".to_string()), // Duplicate key
    ];

    let left_rdd = context.parallelize_with_partitions(left_data, 2);
    let right_rdd = context.parallelize_with_partitions(right_data, 2);
    let partitioner = Arc::new(HashPartitioner::new(2));

    let result = left_rdd
        .join(Arc::new(right_rdd), partitioner)
        .collect()
        .unwrap();

    // Should produce cartesian product for matching keys
    let a_results: Vec<_> = result
        .iter()
        .filter(|(k, _)| k == "a")
        .map(|(_, v)| v.clone())
        .collect();

    // Should have 4 combinations: (1,apple), (1,apricot), (4,apple), (4,apricot)
    assert_eq!(a_results.len(), 4);

    let b_results: Vec<_> = result.iter().filter(|(k, _)| k == "b").collect();
    assert_eq!(b_results.len(), 1);

    // c and d should not appear (no matching keys)
    assert!(!result.iter().any(|(k, _)| k == "c"));
    assert!(!result.iter().any(|(k, _)| k == "d"));
}

#[test]
fn test_cogroup_comprehensive() {
    let context = create_test_context("cogroup-comprehensive");

    let left_data = vec![
        ("common".to_string(), 1),
        ("left_only".to_string(), 2),
        ("common".to_string(), 3),
    ];
    let right_data = vec![
        ("common".to_string(), "a".to_string()),
        ("right_only".to_string(), "b".to_string()),
        ("common".to_string(), "c".to_string()),
    ];

    let left_rdd = context.parallelize_with_partitions(left_data, 2);
    let right_rdd = context.parallelize_with_partitions(right_data, 2);
    let partitioner = Arc::new(HashPartitioner::new(2));

    let result = left_rdd
        .cogroup(Arc::new(right_rdd), partitioner)
        .collect()
        .unwrap();

    let result_map: std::collections::HashMap<String, (Vec<i32>, Vec<String>)> =
        result.into_iter().collect();

    // Check common key
    let common_group = result_map.get("common").unwrap();
    let mut common_left = common_group.0.clone();
    common_left.sort();
    let mut common_right = common_group.1.clone();
    common_right.sort();
    assert_eq!(common_left, vec![1, 3]);
    assert_eq!(common_right, vec!["a".to_string(), "c".to_string()]);

    // Check left-only key
    let left_only_group = result_map.get("left_only").unwrap();
    assert_eq!(left_only_group.0, vec![2]);
    assert!(left_only_group.1.is_empty());

    // Check right-only key
    let right_only_group = result_map.get("right_only").unwrap();
    assert!(right_only_group.0.is_empty());
    assert_eq!(right_only_group.1, vec!["b".to_string()]);
}

#[test]
fn test_sort_by_key_ascending() {
    let context = create_test_context("sort-by-key-asc");
    let data = vec![
        (5, "five".to_string()),
        (2, "two".to_string()),
        (8, "eight".to_string()),
        (1, "one".to_string()),
        (9, "nine".to_string()),
    ];
    let rdd = context.parallelize_with_partitions(data, 2);

    let result = rdd
        .sort_by_key(true) // ascending
        .collect()
        .unwrap();

    // Should be sorted by key in ascending order
    let keys: Vec<i32> = result.iter().map(|(k, _)| *k).collect();
    assert_eq!(keys, vec![1, 2, 5, 8, 9]);

    // Values should correspond to keys
    assert_eq!(result[0], (1, "one".to_string()));
    assert_eq!(result[1], (2, "two".to_string()));
    assert_eq!(result[4], (9, "nine".to_string()));
}

#[test]
fn test_sort_by_key_descending() {
    let context = create_test_context("sort-by-key-desc");
    let data = vec![
        (5, "five".to_string()),
        (2, "two".to_string()),
        (8, "eight".to_string()),
        (1, "one".to_string()),
        (9, "nine".to_string()),
    ];
    let rdd = context.parallelize_with_partitions(data, 2);

    let result = rdd
        .sort_by_key(false) // descending
        .collect()
        .unwrap();

    // Should be sorted by key in descending order
    let keys: Vec<i32> = result.iter().map(|(k, _)| *k).collect();
    assert_eq!(keys, vec![9, 8, 5, 2, 1]);
}

#[test]
fn test_combine_by_key_custom_aggregation() {
    let context = create_test_context("combine-by-key-test");
    let data = vec![
        ("math".to_string(), 85),
        ("science".to_string(), 90),
        ("math".to_string(), 92),
        ("english".to_string(), 78),
        ("science".to_string(), 88),
        ("math".to_string(), 79),
    ];
    let rdd = context.parallelize_with_partitions(data, 2);
    let partitioner = Arc::new(HashPartitioner::new(2));

    // Calculate average scores by subject using CombineAggregator
    let aggregator = Arc::new(barks_core::shuffle::CombineAggregator::new(
        |score| (score, 1),                             // Create combiner: (sum, count)
        |(sum, count), score| (sum + score, count + 1), // Merge value
        |(sum1, count1), (sum2, count2)| (sum1 + sum2, count1 + count2), // Merge combiners
    ));

    let result = rdd
        .combine_by_key(aggregator, partitioner)
        .collect()
        .unwrap();

    let result_map: std::collections::HashMap<String, (i32, i32)> = result.into_iter().collect();

    // Math: (85 + 92 + 79) / 3 = 256 / 3 ≈ 85.33
    let math_result = result_map.get("math").unwrap();
    assert_eq!(math_result, &(256, 3));

    // Science: (90 + 88) / 2 = 178 / 2 = 89
    let science_result = result_map.get("science").unwrap();
    assert_eq!(science_result, &(178, 2));

    // English: 78 / 1 = 78
    let english_result = result_map.get("english").unwrap();
    assert_eq!(english_result, &(78, 1));
}

#[test]
fn test_shuffle_with_different_partitioners() {
    let context = create_test_context("shuffle-partitioners");
    let data = create_test_string_i32_data();
    let rdd = context.parallelize_with_partitions(data, 2);

    // Test with HashPartitioner
    let hash_partitioner = Arc::new(HashPartitioner::new(3));
    let hash_result = rdd
        .clone()
        .reduce_by_key(|a, b| a + b, hash_partitioner)
        .collect()
        .unwrap();

    // Test with RangePartitioner (using sample keys)
    let sample_keys = vec!["a".to_string(), "b".to_string()];
    let range_partitioner = Arc::new(RangePartitioner::from_sample(3, sample_keys));
    let range_result = rdd
        .reduce_by_key(|a, b| a + b, range_partitioner)
        .collect()
        .unwrap();

    // Both should produce the same aggregated results
    let hash_map: std::collections::HashMap<String, i32> = hash_result.into_iter().collect();
    let range_map: std::collections::HashMap<String, i32> = range_result.into_iter().collect();

    assert_eq!(hash_map, range_map);
}

#[test]
fn test_empty_shuffle_operations() {
    let context = create_test_context("empty-shuffle");
    let empty_data: Vec<(String, i32)> = vec![];
    let rdd = context.parallelize(empty_data);
    let partitioner = Arc::new(HashPartitioner::new(2));

    // All shuffle operations should handle empty data gracefully
    let reduce_result = rdd
        .clone()
        .reduce_by_key(|a, b| a + b, partitioner.clone())
        .collect()
        .unwrap();
    assert!(reduce_result.is_empty());

    let group_result = rdd
        .clone()
        .group_by_key(partitioner.clone())
        .collect()
        .unwrap();
    assert!(group_result.is_empty());

    let empty_right: Vec<(String, String)> = vec![];
    let right_rdd = context.parallelize(empty_right);
    let join_result = rdd
        .join(Arc::new(right_rdd), partitioner)
        .collect()
        .unwrap();
    assert!(join_result.is_empty());
}

#[test]
fn test_large_shuffle_dataset() {
    let context = create_test_context("large-shuffle");

    // Create a larger dataset for shuffle testing
    let data: Vec<(String, i32)> = (0..1000).map(|i| (format!("key_{}", i % 10), i)).collect();

    let rdd = context.parallelize_with_partitions(data, 8);
    let partitioner = Arc::new(HashPartitioner::new(4));

    let result = rdd
        .reduce_by_key(|a, b| a + b, partitioner)
        .collect()
        .unwrap();

    // Should have 10 keys (key_0 through key_9)
    assert_eq!(result.len(), 10);

    // Verify one of the sums
    let result_map: std::collections::HashMap<String, i32> = result.into_iter().collect();

    // key_0 should have values: 0, 10, 20, ..., 990 (100 values)
    // Sum = 0 + 10 + 20 + ... + 990 = 10 * (0 + 1 + 2 + ... + 99) = 10 * 4950 = 49500
    assert_eq!(result_map.get("key_0"), Some(&49500));
}
