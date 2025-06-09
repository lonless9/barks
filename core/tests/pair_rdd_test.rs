//! Test for the generalized PairRdd implementation

use barks_core::rdd::DistributedRdd;
use barks_core::rdd::transformations::PairRddExt;
use barks_core::shuffle::{HashPartitioner, ReduceAggregator};
use barks_core::traits::IsRdd;
use std::sync::Arc;

#[test]
fn test_pair_rdd_generic_string_i32() {
    // Test with (String, i32) pairs - the original hardcoded type
    let data = vec![
        ("apple".to_string(), 1),
        ("banana".to_string(), 2),
        ("apple".to_string(), 3),
        ("cherry".to_string(), 4),
    ];

    let rdd: DistributedRdd<(String, i32)> = DistributedRdd::from_vec(data);
    let partitioner = Arc::new(HashPartitioner::new(2));

    // Test reduce_by_key - this should now work with the generic implementation
    let reduced_rdd = rdd.reduce_by_key(|a, b| a + b, partitioner.clone());

    // Verify the RDD was created successfully
    assert_eq!(reduced_rdd.num_partitions(), 2);
    assert!(reduced_rdd.id() > 0);
}

#[test]
fn test_pair_rdd_multiple_operations() {
    // Test multiple operations on the same RDD type
    let data = vec![
        ("apple".to_string(), 1),
        ("banana".to_string(), 2),
        ("apple".to_string(), 3),
        ("cherry".to_string(), 4),
    ];

    let rdd: DistributedRdd<(String, i32)> = DistributedRdd::from_vec(data);
    let partitioner = Arc::new(HashPartitioner::new(2));

    // Test reduce_by_key with addition
    let reduced_rdd = rdd.clone().reduce_by_key(|a, b| a + b, partitioner.clone());

    // Verify the RDD was created successfully
    assert_eq!(reduced_rdd.num_partitions(), 2);
    assert!(reduced_rdd.id() > 0);

    // Test reduce_by_key with multiplication
    let multiplied_rdd = rdd.reduce_by_key(|a, b| a * b, partitioner.clone());

    // Verify the RDD was created successfully
    assert_eq!(multiplied_rdd.num_partitions(), 2);
    assert!(multiplied_rdd.id() > 0);
}

#[test]
fn test_pair_rdd_join_operation() {
    // Test join operation with same types (String, i32)
    let data1 = vec![("key1".to_string(), 10), ("key2".to_string(), 20)];

    let data2 = vec![("key1".to_string(), 100), ("key3".to_string(), 300)];

    let rdd1: DistributedRdd<(String, i32)> = DistributedRdd::from_vec(data1);
    let rdd2: DistributedRdd<(String, i32)> = DistributedRdd::from_vec(data2);
    let partitioner = Arc::new(HashPartitioner::new(2));

    // Test join operation
    let joined_rdd = rdd1.join(Arc::new(rdd2), partitioner);

    // Verify the RDD was created successfully
    assert_eq!(joined_rdd.num_partitions(), 2);
    assert!(joined_rdd.id() > 0);
}

#[test]
fn test_pair_rdd_cogroup_operation() {
    // Test cogroup operation with same types (String, i32)
    let data1 = vec![("key1".to_string(), 10), ("key2".to_string(), 20)];

    let data2 = vec![("key1".to_string(), 100), ("key3".to_string(), 300)];

    let rdd1: DistributedRdd<(String, i32)> = DistributedRdd::from_vec(data1);
    let rdd2: DistributedRdd<(String, i32)> = DistributedRdd::from_vec(data2);
    let partitioner = Arc::new(HashPartitioner::new(2));

    // Test cogroup operation
    let cogrouped_rdd = rdd1.cogroup(Arc::new(rdd2), partitioner);

    // Verify the RDD was created successfully
    assert_eq!(cogrouped_rdd.num_partitions(), 2);
    assert!(cogrouped_rdd.id() > 0);
}

#[test]
fn test_pair_rdd_combine_by_key_operation() {
    // Test combine_by_key operation with custom aggregator
    let data = vec![
        ("apple".to_string(), 1),
        ("banana".to_string(), 2),
        ("apple".to_string(), 3),
    ];

    let rdd: DistributedRdd<(String, i32)> = DistributedRdd::from_vec(data);
    let partitioner = Arc::new(HashPartitioner::new(2));
    let aggregator = Arc::new(ReduceAggregator::new(|a: i32, b: i32| a + b));

    // Test combine_by_key operation
    let combined_rdd = rdd.combine_by_key(aggregator, partitioner);

    // Verify the RDD was created successfully
    assert_eq!(combined_rdd.num_partitions(), 2);
    assert!(combined_rdd.id() > 0);
}

#[test]
fn test_pair_rdd_multiple_types() {
    // Test with (i32, String) pairs - new type support
    let data = vec![
        (1, "apple".to_string()),
        (2, "banana".to_string()),
        (1, "cherry".to_string()),
        (3, "date".to_string()),
    ];

    let rdd: DistributedRdd<(i32, String)> = DistributedRdd::from_vec(data);
    let partitioner = Arc::new(HashPartitioner::new(2));

    // Test reduce_by_key with string concatenation
    let reduced_rdd = rdd.reduce_by_key(|a, b| format!("{},{}", a, b), partitioner.clone());

    // Verify the RDD was created successfully
    assert_eq!(reduced_rdd.num_partitions(), 2);
    assert!(reduced_rdd.id() > 0);

    // Test local execution to verify functionality
    let result = reduced_rdd.collect().unwrap();

    // Convert to HashMap for easier verification
    let result_map: std::collections::HashMap<i32, String> = result.into_iter().collect();

    // Verify that key 1 has concatenated values
    assert!(result_map.contains_key(&1));
    assert!(result_map.contains_key(&2));
    assert!(result_map.contains_key(&3));

    // Key 1 should have "apple,cherry" (order may vary)
    let key1_result = result_map.get(&1).unwrap();
    assert!(key1_result.contains("apple") && key1_result.contains("cherry"));
}
