//! Common test utilities and helpers for integration tests

use barks_core::context::FlowContext;
use barks_core::operations::{DoubleOperation, GreaterThanPredicate};
use barks_core::rdd::DistributedRdd;

/// Create a test context with a given name
pub fn create_test_context(name: &str) -> FlowContext {
    FlowContext::new(name)
}

/// Create a test context with specific thread count
#[allow(dead_code)] // Used in local_rdd_api.rs but clippy doesn't detect cross-module usage
pub fn create_test_context_with_threads(name: &str, threads: usize) -> FlowContext {
    FlowContext::new_with_threads(name, threads)
}

/// Create test data for integer operations
pub fn create_test_i32_data() -> Vec<i32> {
    (1..=20).collect()
}

/// Create test data for string operations
pub fn create_test_string_data() -> Vec<String> {
    vec![
        "apple".to_string(),
        "banana".to_string(),
        "cherry".to_string(),
        "date".to_string(),
        "elderberry".to_string(),
    ]
}

/// Create test data for key-value pairs (String, i32)
pub fn create_test_string_i32_data() -> Vec<(String, i32)> {
    vec![
        ("a".to_string(), 1),
        ("b".to_string(), 2),
        ("a".to_string(), 3),
        ("c".to_string(), 4),
        ("b".to_string(), 5),
        ("a".to_string(), 6),
    ]
}

/// Create an RDD with specific number of partitions
pub fn create_partitioned_rdd<T: barks_core::operations::RddDataType>(
    data: Vec<T>,
    partitions: usize,
) -> DistributedRdd<T> {
    DistributedRdd::from_vec_with_partitions(data, partitions)
}

/// Common operations for testing
pub fn double_operation() -> Box<dyn barks_core::operations::I32Operation> {
    Box::new(DoubleOperation)
}

pub fn greater_than_predicate(threshold: i32) -> Box<dyn barks_core::operations::I32Predicate> {
    Box::new(GreaterThanPredicate { threshold })
}

/// Assert that two vectors contain the same elements (order-independent)
pub fn assert_same_elements<T: Ord + Clone + std::fmt::Debug>(
    mut actual: Vec<T>,
    mut expected: Vec<T>,
) {
    actual.sort();
    expected.sort();
    assert_eq!(actual, expected);
}

/// Assert that a result contains all expected elements
pub fn assert_contains_all<T: PartialEq + std::fmt::Debug>(actual: &[T], expected: &[T]) {
    for item in expected {
        assert!(
            actual.contains(item),
            "Expected item {:?} not found in actual result {:?}",
            item,
            actual
        );
    }
}

/// Test helper for verifying RDD transformations
pub fn verify_transformation_pipeline() {
    let context = create_test_context("transformation-test");
    let data = create_test_i32_data();
    let rdd = context.parallelize_with_partitions(data, 4);

    // Apply transformations: map (x * 2) -> filter (x > 15) -> collect
    let result = rdd
        .map(double_operation())
        .filter(greater_than_predicate(15))
        .collect()
        .unwrap();

    // Expected: [2, 4, 6, 8, ..., 40] -> [16, 18, 20, ..., 40]
    let expected: Vec<i32> = (8..=20).map(|x| x * 2).collect();
    assert_same_elements(result, expected);
}

/// Test helper for verifying parallel execution consistency
pub fn verify_parallel_consistency() {
    let data = create_test_i32_data();

    // Test with different partition counts
    let single_partition = create_partitioned_rdd(data.clone(), 1);
    let multi_partition = create_partitioned_rdd(data.clone(), 8);

    let single_result = single_partition.collect().unwrap();
    let multi_result = multi_partition.collect().unwrap();

    assert_same_elements(single_result, multi_result);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_common_helpers() {
        let context = create_test_context("test");
        assert_eq!(context.app_name(), "test");

        let data = create_test_i32_data();
        assert_eq!(data.len(), 20);
        assert_eq!(data[0], 1);
        assert_eq!(data[19], 20);

        let string_data = create_test_string_data();
        assert_eq!(string_data.len(), 5);
        assert_eq!(string_data[0], "apple");
    }

    #[test]
    fn test_assert_helpers() {
        assert_same_elements(vec![3, 1, 2], vec![1, 2, 3]);
        assert_contains_all(&[1, 2, 3, 4], &[2, 4]);
    }

    #[test]
    fn test_transformation_pipeline() {
        verify_transformation_pipeline();
    }

    #[test]
    fn test_parallel_consistency() {
        verify_parallel_consistency();
    }
}
