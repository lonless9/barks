//! Integration tests for TODO0 improvements

use barks_core::cache::StorageLevel;
use barks_core::rdd::{
    CacheableRdd, DistinctRdd, DistributedRdd, RepartitionRdd, RoundRobinPartitioner,
};
use barks_core::shuffle::HashPartitioner;
use barks_core::traits::{IsRdd, RddBase};
use std::collections::HashSet;
use std::sync::Arc;

#[tokio::test]
async fn test_improved_distinct_operation() {
    // Test global distinct operation with duplicates across partitions
    let data = vec![1, 2, 3, 2, 4, 3, 5, 1, 6, 4]; // Contains duplicates
    let rdd = Arc::new(DistributedRdd::from_vec(data));

    // Create distinct RDD with hash partitioner
    let partitioner = Arc::new(HashPartitioner::new(3));
    let distinct_rdd = DistinctRdd::new(1001, rdd, partitioner);

    // Collect results
    let result = distinct_rdd.collect().unwrap();
    let result_set: HashSet<i32> = result.into_iter().collect();

    // Should have only unique elements
    let expected: HashSet<i32> = vec![1, 2, 3, 4, 5, 6].into_iter().collect();
    assert_eq!(result_set, expected);
}

#[tokio::test]
async fn test_repartition_operation() {
    // Test repartitioning with different number of partitions
    let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let rdd = Arc::new(DistributedRdd::from_vec(data.clone()));

    // Original RDD has default partitions
    let _original_partitions = rdd.num_partitions();

    // Repartition to 4 partitions
    let partitioner = Arc::new(RoundRobinPartitioner::new(4));
    let repartitioned_rdd = RepartitionRdd::new(1002, rdd, partitioner);

    // Check new partition count
    assert_eq!(repartitioned_rdd.num_partitions(), 4);

    // Collect results - should have same data, different distribution
    let result = repartitioned_rdd.collect().unwrap();
    let mut result_sorted = result;
    result_sorted.sort();

    let mut expected = data;
    expected.sort();

    assert_eq!(result_sorted, expected);
}

#[tokio::test]
async fn test_rdd_caching_functionality() {
    // Test RDD caching with memory storage
    let data = vec![1, 2, 3, 4, 5];
    let rdd = Arc::new(DistributedRdd::from_vec(data.clone()));

    // Cache the RDD
    let cached_rdd = rdd.cache();

    // First access should compute and cache
    let result1 = cached_rdd.collect().unwrap();
    assert_eq!(result1, data);

    // Second access should hit cache
    let result2 = cached_rdd.collect().unwrap();
    assert_eq!(result2, data);

    // Check that it's marked as cached
    assert!(cached_rdd.is_cached());
    assert_eq!(cached_rdd.storage_level(), StorageLevel::MemoryOnly);
}

#[tokio::test]
async fn test_rdd_persist_with_different_storage_levels() {
    let data = vec![1, 2, 3, 4, 5];
    let rdd = Arc::new(DistributedRdd::from_vec(data.clone()));

    // Test different storage levels
    let memory_only = rdd.clone().persist(StorageLevel::MemoryOnly);
    assert_eq!(memory_only.storage_level(), StorageLevel::MemoryOnly);

    let memory_and_disk = rdd.clone().persist(StorageLevel::MemoryAndDisk);
    assert_eq!(memory_and_disk.storage_level(), StorageLevel::MemoryAndDisk);

    let disk_only = rdd.clone().persist(StorageLevel::DiskOnly);
    assert_eq!(disk_only.storage_level(), StorageLevel::DiskOnly);

    // All should work correctly
    assert_eq!(memory_only.collect().unwrap(), data);
    assert_eq!(memory_and_disk.collect().unwrap(), data);
    assert_eq!(disk_only.collect().unwrap(), data);
}

#[tokio::test]
async fn test_cache_unpersist() {
    let data = vec![1, 2, 3, 4, 5];
    let rdd = Arc::new(DistributedRdd::from_vec(data.clone()));

    // Cache the RDD
    let cached_rdd = rdd.cache();

    // Access to populate cache
    let _result = cached_rdd.collect().unwrap();

    // Unpersist
    cached_rdd.unpersist().await.unwrap();

    // Should still work but will recompute
    let result = cached_rdd.collect().unwrap();
    assert_eq!(result, data);
}

#[tokio::test]
async fn test_cache_statistics() {
    let data = vec![1, 2, 3, 4, 5];
    let rdd = Arc::new(DistributedRdd::from_vec(data.clone()));

    // Cache the RDD
    let cached_rdd = rdd.cache();

    // Access multiple times
    let _result1 = cached_rdd.collect().unwrap();
    let _result2 = cached_rdd.collect().unwrap();
    let _result3 = cached_rdd.collect().unwrap();

    // Check cache stats
    let stats = cached_rdd.cache_stats().await;

    // Should have some hits or misses
    assert!(stats.hits > 0 || stats.misses > 0);
}

#[tokio::test]
async fn test_distinct_with_strings() {
    // Test distinct operation with string data
    let data = vec![
        "apple".to_string(),
        "banana".to_string(),
        "apple".to_string(),
        "cherry".to_string(),
        "banana".to_string(),
        "date".to_string(),
    ];
    let rdd = Arc::new(DistributedRdd::from_vec(data));

    // Create distinct RDD
    let partitioner = Arc::new(HashPartitioner::new(2));
    let distinct_rdd = DistinctRdd::new(1003, rdd, partitioner);

    // Collect results
    let result = distinct_rdd.collect().unwrap();
    let result_set: HashSet<String> = result.into_iter().collect();

    // Should have only unique strings
    let expected: HashSet<String> = vec![
        "apple".to_string(),
        "banana".to_string(),
        "cherry".to_string(),
        "date".to_string(),
    ]
    .into_iter()
    .collect();

    assert_eq!(result_set, expected);
}

#[tokio::test]
async fn test_repartition_with_strings() {
    // Test repartitioning with string data
    let data = vec![
        "a".to_string(),
        "b".to_string(),
        "c".to_string(),
        "d".to_string(),
        "e".to_string(),
    ];
    let rdd = Arc::new(DistributedRdd::from_vec(data.clone()));

    // Repartition to 3 partitions
    let partitioner = Arc::new(RoundRobinPartitioner::new(3));
    let repartitioned_rdd = RepartitionRdd::new(1004, rdd, partitioner);

    // Check partition count
    assert_eq!(repartitioned_rdd.num_partitions(), 3);

    // Collect and verify data integrity
    let result = repartitioned_rdd.collect().unwrap();
    let mut result_sorted = result;
    result_sorted.sort();

    let mut expected = data;
    expected.sort();

    assert_eq!(result_sorted, expected);
}

#[tokio::test]
async fn test_complex_pipeline_with_caching() {
    // Test a complex pipeline: data -> distinct -> repartition -> cache
    let data = vec![1, 2, 3, 2, 4, 3, 5, 1, 6, 4, 7, 8, 9, 10];
    let rdd = Arc::new(DistributedRdd::from_vec(data));

    // Apply distinct
    let partitioner1 = Arc::new(HashPartitioner::new(3));
    let distinct_rdd = Arc::new(DistinctRdd::new(2001, rdd, partitioner1));

    // Repartition the distinct result
    let partitioner2 = Arc::new(RoundRobinPartitioner::new(4));
    let repartitioned_rdd = Arc::new(RepartitionRdd::new(2002, distinct_rdd, partitioner2));

    // Cache the final result
    let cached_rdd = repartitioned_rdd.cache();

    // Collect results multiple times
    let mut result1 = cached_rdd.collect().unwrap();
    let mut result2 = cached_rdd.collect().unwrap();

    // Sort results for consistent comparison
    result1.sort();
    result2.sort();

    // Results should be consistent
    assert_eq!(result1, result2);

    // Should have unique elements from 1 to 10
    let result_set: HashSet<i32> = result1.into_iter().collect();
    let expected: HashSet<i32> = (1..=10).collect();
    assert_eq!(result_set, expected);

    // Should be cached
    assert!(cached_rdd.is_cached());
}

#[test]
fn test_storage_level_properties() {
    // Test storage level utility methods
    assert!(StorageLevel::MemoryOnly.use_memory());
    assert!(!StorageLevel::MemoryOnly.use_disk());
    assert!(!StorageLevel::MemoryOnly.use_serialization());
    assert_eq!(StorageLevel::MemoryOnly.replication(), 1);

    assert!(!StorageLevel::DiskOnly.use_memory());
    assert!(StorageLevel::DiskOnly.use_disk());
    assert!(!StorageLevel::DiskOnly.use_serialization());

    assert!(StorageLevel::MemoryAndDisk.use_memory());
    assert!(StorageLevel::MemoryAndDisk.use_disk());
    assert!(!StorageLevel::MemoryAndDisk.use_serialization());

    assert!(StorageLevel::MemoryOnlySer.use_serialization());
    assert_eq!(StorageLevel::MemoryOnly2.replication(), 2);

    assert!(!StorageLevel::None.is_cached());
    assert!(StorageLevel::MemoryOnly.is_cached());
}
