//! Integration tests for basic implementation
//!
//! These tests verify the complete pipeline: map -> filter -> collect

use barks_core::operations::{DoubleOperation, EvenPredicate};
use barks_core::rdd::DistributedRdd;

#[test]
fn test_distributed_rdd_creation() {
    let data = vec![1, 2, 3, 4, 5];
    let rdd: DistributedRdd<i32> = DistributedRdd::from_vec(data.clone());

    let result = rdd.collect().unwrap();
    assert_eq!(result, data);
}

#[test]
fn test_distributed_rdd_with_partitions() {
    let data = vec![1, 2, 3, 4, 5, 6, 7, 8];
    let rdd: DistributedRdd<i32> = DistributedRdd::from_vec_with_partitions(data.clone(), 3);

    assert_eq!(rdd.num_partitions(), 3);
    let result = rdd.collect().unwrap();
    assert_eq!(result, data);
}

#[test]
fn test_map_transformation() {
    let data = vec![1, 2, 3, 4, 5];
    let rdd: DistributedRdd<i32> = DistributedRdd::from_vec(data);

    let mapped_rdd = rdd.map(Box::new(DoubleOperation));
    let result = mapped_rdd.collect().unwrap();

    assert_eq!(result, vec![2, 4, 6, 8, 10]);
}

#[test]
fn test_filter_transformation() {
    let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let rdd: DistributedRdd<i32> = DistributedRdd::from_vec(data);

    let filtered_rdd = rdd.filter(Box::new(EvenPredicate));
    let result = filtered_rdd.collect().unwrap();

    assert_eq!(result, vec![2, 4, 6, 8, 10]);
}

#[test]
fn test_map_filter_chain() {
    use barks_core::operations::GreaterThanPredicate;

    let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let rdd: DistributedRdd<i32> = DistributedRdd::from_vec(data);

    // Map: multiply by 2, then filter: keep only numbers > 10
    let result_rdd = rdd
        .map(Box::new(DoubleOperation))
        .filter(Box::new(GreaterThanPredicate { threshold: 10 }));

    let result = result_rdd.collect().unwrap();
    assert_eq!(result, vec![12, 14, 16, 18, 20]);
}

#[test]
fn test_filter_map_chain() {
    use barks_core::operations::AddConstantOperation;

    let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let rdd: DistributedRdd<i32> = DistributedRdd::from_vec(data);

    // Filter: keep only even numbers, then map: add 1
    let result_rdd = rdd
        .filter(Box::new(EvenPredicate))
        .map(Box::new(AddConstantOperation { constant: 1 }));

    let result = result_rdd.collect().unwrap();
    assert_eq!(result, vec![3, 5, 7, 9, 11]);
}

#[test]
fn test_count_action() {
    let data = vec![1, 2, 3, 4, 5];
    let rdd: DistributedRdd<i32> = DistributedRdd::from_vec(data);

    let count = rdd.count().unwrap();
    assert_eq!(count, 5);
}

#[test]
fn test_take_action() {
    let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let rdd: DistributedRdd<i32> = DistributedRdd::from_vec(data);

    let result = rdd.take(3).unwrap();
    assert_eq!(result, vec![1, 2, 3]);
}

#[test]
fn test_first_action() {
    let data = vec![10, 20, 30];
    let rdd: DistributedRdd<i32> = DistributedRdd::from_vec(data);

    let first = rdd.first().unwrap();
    assert_eq!(first, Some(10));
}

#[test]
fn test_first_action_empty() {
    let data: Vec<i32> = vec![];
    let rdd: DistributedRdd<i32> = DistributedRdd::from_vec(data);

    let first = rdd.first().unwrap();
    assert_eq!(first, None);
}

#[test]
fn test_flow_context() {
    use barks_core::FlowContext;

    let context = FlowContext::new("test-app");
    assert_eq!(context.app_name(), "test-app");

    let data = vec![1, 2, 3, 4, 5];
    let rdd = context.parallelize(data.clone());

    let result = rdd.collect().unwrap();
    assert_eq!(result, data);
}

#[test]
fn test_flow_context_with_partitions() {
    use barks_core::FlowContext;

    let context = FlowContext::new("test-app");

    let data = vec![1, 2, 3, 4, 5, 6, 7, 8];
    let rdd = context.parallelize_with_partitions(data.clone(), 4);

    assert_eq!(rdd.num_partitions(), 4);
    let result = rdd.collect().unwrap();
    assert_eq!(result, data);
}

#[test]
fn test_complete_pipeline() {
    use barks_core::FlowContext;
    use barks_core::operations::GreaterThanPredicate;

    // This is the main test for the complete map->filter->collect pipeline
    let context = FlowContext::new("barks-phase0-test");

    // Create RDD with numbers 1-20
    let data: Vec<i32> = (1..=20).collect();
    let rdd = context.parallelize_with_partitions(data, 4);

    // Apply transformations: map (x * 2) -> filter (x > 15) -> collect
    let result = rdd
        .map(Box::new(DoubleOperation)) // [2, 4, 6, 8, ..., 40]
        .filter(Box::new(GreaterThanPredicate { threshold: 15 })) // [16, 18, 20, ..., 40]
        .collect()
        .unwrap();

    let expected: Vec<i32> = (8..=20).map(|x| x * 2).collect(); // [16, 18, 20, ..., 40]
    assert_eq!(result, expected);

    // Verify count
    let count_rdd = context
        .parallelize((1..=20).collect())
        .map(Box::new(DoubleOperation))
        .filter(Box::new(GreaterThanPredicate { threshold: 15 }));

    assert_eq!(count_rdd.count().unwrap(), 13);
}

#[test]
fn test_lazy_evaluation() {
    use barks_core::operations::{AddConstantOperation, GreaterThanPredicate};

    // Test that transformations are lazy and only executed on actions
    let data = vec![1, 2, 3, 4, 5];
    let rdd: DistributedRdd<i32> = DistributedRdd::from_vec(data);

    // These transformations should not execute until an action is called
    let transformed_rdd = rdd
        .map(Box::new(AddConstantOperation { constant: 20 }))
        .filter(Box::new(GreaterThanPredicate { threshold: 22 }));

    // Only now should the computation happen
    let result = transformed_rdd.collect().unwrap();
    assert_eq!(result, vec![23, 24, 25]);
}
