//! Parallel execution tests
//!
//! These tests verify the parallel execution functionality using Rayon

use barks_core::{FlowContext, LocalScheduler, SimpleRdd};

#[test]
fn test_parallel_collect() {
    let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let rdd = SimpleRdd::from_vec_with_partitions(data.clone(), 4);

    // Test automatic parallel collect (multiple partitions)
    let result = rdd.collect().unwrap();
    assert_eq!(result, data);
}

#[test]
fn test_parallel_count() {
    let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let rdd = SimpleRdd::from_vec_with_partitions(data.clone(), 3);

    // Test automatic parallel count (multiple partitions)
    let count = rdd.count().unwrap();
    assert_eq!(count, data.len());
}

#[test]
fn test_parallel_reduce() {
    let data = vec![1, 2, 3, 4, 5];
    let rdd = SimpleRdd::from_vec_with_partitions(data, 3);

    // Test automatic parallel reduce (multiple partitions)
    let sum = rdd.reduce(|a, b| a + b).unwrap();
    assert_eq!(sum, Some(15)); // 1+2+3+4+5 = 15
}

#[test]
fn test_parallel_reduce_empty() {
    let data: Vec<i32> = vec![];
    let rdd = SimpleRdd::from_vec(data);

    // Test reduce on empty RDD (single partition, sequential)
    let result = rdd.reduce(|a, b| a + b).unwrap();
    assert_eq!(result, None);
}

#[test]
fn test_parallel_foreach() {
    use std::sync::{Arc, Mutex};

    let data = vec![1, 2, 3, 4, 5];
    let rdd = SimpleRdd::from_vec_with_partitions(data.clone(), 2);

    // Test parallel foreach with side effects
    let counter = Arc::new(Mutex::new(0));
    let counter_clone = Arc::clone(&counter);

    // Test automatic parallel foreach (multiple partitions)
    rdd.foreach(move |_| {
        let mut count = counter_clone.lock().unwrap();
        *count += 1;
    })
    .unwrap();

    let final_count = *counter.lock().unwrap();
    assert_eq!(final_count, data.len());
}

#[test]
fn test_flow_context_parallel_run() {
    let context = FlowContext::new_with_threads("parallel-test", 4);
    assert_eq!(context.num_threads(), 4);

    let data = vec![1, 2, 3, 4, 5, 6, 7, 8];
    let rdd = context.parallelize_with_partitions(data.clone(), 3);

    // Test parallel run
    let result = context.run(rdd).unwrap();
    assert_eq!(result, data);
}

#[test]
fn test_flow_context_with_transformations() {
    let context = FlowContext::new("parallel-transform-test");

    let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let rdd = context
        .parallelize_with_partitions(data, 4)
        .map(|x| x * 2)
        .filter(|&x| x > 10);

    // Test parallel execution with transformations
    let result = context.run(rdd).unwrap();
    let expected: Vec<i32> = vec![12, 14, 16, 18, 20]; // [6,7,8,9,10] * 2 = [12,14,16,18,20]
    assert_eq!(result, expected);
}

#[test]
fn test_local_scheduler() {
    let scheduler = LocalScheduler::new(2);
    assert_eq!(scheduler.num_threads(), 2);

    let default_scheduler = LocalScheduler::default();
    assert!(default_scheduler.num_threads() > 0);
}

#[test]
fn test_partition_driven_parallelism() {
    let data: Vec<i32> = (1..=100).collect();

    // Test single partition (sequential execution)
    let single_partition_rdd = SimpleRdd::from_vec_with_partitions(data.clone(), 1);
    let single_result = single_partition_rdd.collect().unwrap();
    let single_count = single_partition_rdd.count().unwrap();
    let single_reduce = single_partition_rdd.reduce(|a, b| a + b).unwrap();

    // Test multiple partitions (automatic parallel execution)
    let multi_partition_rdd = SimpleRdd::from_vec_with_partitions(data.clone(), 8);
    let multi_result = multi_partition_rdd.collect().unwrap();
    let multi_count = multi_partition_rdd.count().unwrap();
    let multi_reduce = multi_partition_rdd.reduce(|a, b| a + b).unwrap();

    // Results should be identical regardless of partition count
    assert_eq!(single_result, multi_result);
    assert_eq!(single_count, multi_count);
    assert_eq!(single_reduce, multi_reduce);
}

#[test]
fn test_complex_parallel_pipeline() {
    let context = FlowContext::new_with_threads("complex-pipeline", 4);

    // Create a larger dataset for meaningful parallel processing
    let data: Vec<i32> = (1..=1000).collect();
    let rdd = context
        .parallelize_with_partitions(data, 10)
        .map(|x| x * x) // Square each number
        .filter(|&x| x % 2 == 0) // Keep only even squares
        .map(|x| x / 2); // Divide by 2

    let result = context.run(rdd.clone()).unwrap();

    // Verify the result using automatic count (parallel due to multiple partitions)
    let count = rdd.count().unwrap();
    assert_eq!(result.len(), count);

    // Verify some properties of the result
    assert!(result.iter().all(|&x| x % 2 == 0)); // All should be even
    assert!(!result.is_empty());
}

#[test]
fn test_parallel_reduce_with_strings() {
    let data = ["hello", "world", "from", "rust"];
    let rdd = SimpleRdd::from_vec_with_partitions(data.iter().map(|s| s.to_string()).collect(), 2);

    // Test automatic parallel reduce with string concatenation (multiple partitions)
    let result = rdd.reduce(|a, b| format!("{} {}", a, b)).unwrap();
    assert!(result.is_some());

    let concatenated = result.unwrap();
    assert!(concatenated.contains("hello"));
    assert!(concatenated.contains("world"));
    assert!(concatenated.contains("from"));
    assert!(concatenated.contains("rust"));
}

#[test]
fn test_default_parallelism() {
    let context = FlowContext::new("default-parallelism-test");

    // Test that default parallelism is based on CPU cores
    assert!(context.default_parallelism() > 0);

    // Test parallelize without specifying partitions uses default parallelism
    let data = vec![1, 2, 3, 4, 5, 6, 7, 8];
    let rdd = context.parallelize(data.clone());
    assert_eq!(rdd.num_partitions(), context.default_parallelism());

    let result = rdd.collect().unwrap();
    assert_eq!(result, data);
}

#[test]
fn test_parallelize_with_slices() {
    let context = FlowContext::new("slices-test");
    let data = vec![1, 2, 3, 4, 5, 6, 7, 8];

    // Test with explicit num_slices = 1 (no parallel advantages)
    let single_slice_rdd = context.parallelize_with_slices(data.clone(), Some(1));
    assert_eq!(single_slice_rdd.num_partitions(), 1);

    // Test with explicit num_slices = 4 (parallel processing)
    let multi_slice_rdd = context.parallelize_with_slices(data.clone(), Some(4));
    assert_eq!(multi_slice_rdd.num_partitions(), 4);

    // Test with None (uses defaultParallelism, minimum 2 for parallel processing)
    let default_rdd = context.parallelize_with_slices(data.clone(), None);
    assert!(default_rdd.num_partitions() >= 2);

    // All should produce same results
    let single_result = single_slice_rdd.collect().unwrap();
    let multi_result = multi_slice_rdd.collect().unwrap();
    let default_result = default_rdd.collect().unwrap();

    assert_eq!(single_result, data);
    assert_eq!(multi_result, data);
    assert_eq!(default_result, data);
}

#[test]
fn test_repartition_and_coalesce() {
    let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let rdd = SimpleRdd::from_vec_with_partitions(data.clone(), 2);

    // Test repartition (increase partitions)
    let repartitioned = rdd.repartition(5);
    assert_eq!(repartitioned.num_partitions(), 5);

    // Test coalesce (decrease partitions)
    let coalesced = repartitioned.coalesce(3);
    assert_eq!(coalesced.num_partitions(), 3);

    // Test coalesce with more partitions than current (should not change)
    let no_change = coalesced.coalesce(10);
    assert_eq!(no_change.num_partitions(), 3);

    // Test coalesce to 0 (should become 1)
    let min_partitions = no_change.coalesce(0);
    assert_eq!(min_partitions.num_partitions(), 1);
}
