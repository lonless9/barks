//! Tests for serializable operations
//!
//! This module contains comprehensive tests for the serializable operations
//! that enable distributed RDD execution.

#[cfg(test)]
mod operation_tests {
    use super::super::*;
    use crate::rdd::DistributedRdd;

    #[test]
    fn test_distributed_rdd_basic_operations() {
        // Test basic RDD creation and collection
        let data = vec![1, 2, 3, 4, 5];
        let rdd: DistributedRdd<i32> = DistributedRdd::from_vec_with_partitions(data.clone(), 2);

        let result = rdd.collect().unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn test_distributed_rdd_map_operation() {
        // Test map operation with DoubleOperation
        let data = vec![1, 2, 3, 4, 5];
        let rdd: DistributedRdd<i32> = DistributedRdd::from_vec_with_partitions(data.clone(), 2);
        let mapped_rdd = rdd.map(Box::new(DoubleOperation));

        let result = mapped_rdd.collect().unwrap();
        let expected: Vec<i32> = data.iter().map(|x| x * 2).collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_distributed_rdd_filter_operation() {
        // Test filter operation with EvenPredicate
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let rdd: DistributedRdd<i32> = DistributedRdd::from_vec_with_partitions(data.clone(), 3);
        let filtered_rdd = rdd.filter(Box::new(EvenPredicate));

        let result = filtered_rdd.collect().unwrap();
        let expected: Vec<i32> = data.iter().filter(|&x| x % 2 == 0).cloned().collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_distributed_rdd_chained_operations() {
        // Test chained operations: add constant then filter even
        let data = vec![1, 2, 3, 4, 5];
        let rdd: DistributedRdd<i32> = DistributedRdd::from_vec_with_partitions(data.clone(), 2);
        let chained_rdd = rdd
            .map(Box::new(AddConstantOperation { constant: 10 }))
            .filter(Box::new(EvenPredicate));

        let result = chained_rdd.collect().unwrap();
        let expected: Vec<i32> = data
            .iter()
            .map(|x| x + 10)
            .filter(|&x| x % 2 == 0)
            .collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_operation_serialization() {
        // Test that operations can be serialized and deserialized as trait objects
        let op: Box<dyn I32Operation> = Box::new(DoubleOperation);
        let serialized = serde_json::to_string(&op).unwrap();
        let deserialized: Box<dyn I32Operation> = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.execute(5), 10);
        assert_eq!(deserialized.execute(-3), -6);
    }

    #[test]
    fn test_predicate_serialization() {
        // Test that predicates can be serialized and deserialized as trait objects
        let pred: Box<dyn I32Predicate> = Box::new(EvenPredicate);
        let serialized = serde_json::to_string(&pred).unwrap();
        let deserialized: Box<dyn I32Predicate> = serde_json::from_str(&serialized).unwrap();

        assert!(deserialized.test(&4));
        assert!(!deserialized.test(&5));
    }

    #[test]
    fn test_add_constant_operation() {
        let op = AddConstantOperation { constant: 100 };
        assert_eq!(op.execute(5), 105);
        assert_eq!(op.execute(-10), 90);

        // Test serialization as trait object
        let op_boxed: Box<dyn I32Operation> = Box::new(AddConstantOperation { constant: 100 });
        let serialized = serde_json::to_string(&op_boxed).unwrap();
        let deserialized: Box<dyn I32Operation> = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.execute(5), 105);
    }

    #[test]
    fn test_greater_than_predicate() {
        let pred = GreaterThanPredicate { threshold: 10 };
        assert!(pred.test(&15));
        assert!(!pred.test(&5));
        assert!(!pred.test(&10));

        // Test serialization as trait object
        let pred_boxed: Box<dyn I32Predicate> = Box::new(GreaterThanPredicate { threshold: 10 });
        let serialized = serde_json::to_string(&pred_boxed).unwrap();
        let deserialized: Box<dyn I32Predicate> = serde_json::from_str(&serialized).unwrap();
        assert!(deserialized.test(&15));
        assert!(!deserialized.test(&5));
    }

    #[test]
    fn test_rdd_partitioning() {
        // Test that partitioning works correctly
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let rdd: DistributedRdd<i32> = DistributedRdd::from_vec_with_partitions(data.clone(), 3);

        assert_eq!(rdd.num_partitions(), 3);

        let partitions = rdd.partitions();
        assert_eq!(partitions.len(), 3);

        // Test that all partitions together contain all data
        let mut all_data = Vec::new();
        for partition in partitions {
            let partition_data = rdd.compute(partition.as_ref()).unwrap();
            all_data.extend(partition_data);
        }

        // Sort both vectors since partition order might differ
        let mut expected = data.clone();
        expected.sort();
        all_data.sort();
        assert_eq!(all_data, expected);
    }

    #[test]
    fn test_rdd_coalesce() {
        // Test coalescing partitions
        let data = vec![1, 2, 3, 4, 5, 6];
        let rdd: DistributedRdd<i32> = DistributedRdd::from_vec_with_partitions(data.clone(), 4);
        assert_eq!(rdd.num_partitions(), 4);

        let coalesced_rdd = rdd.coalesce(2);
        assert_eq!(coalesced_rdd.num_partitions(), 2);

        let result = coalesced_rdd.collect().unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn test_rdd_repartition() {
        // Test repartitioning
        let data = vec![1, 2, 3, 4];
        let rdd: DistributedRdd<i32> = DistributedRdd::from_vec_with_partitions(data.clone(), 2);
        assert_eq!(rdd.num_partitions(), 2);

        let repartitioned_rdd = rdd.repartition(4);
        assert_eq!(repartitioned_rdd.num_partitions(), 4);
    }

    #[test]
    fn test_empty_rdd() {
        // Test empty RDD
        let data: Vec<i32> = vec![];
        let rdd: DistributedRdd<i32> = DistributedRdd::from_vec(data.clone());

        let result = rdd.collect().unwrap();
        assert_eq!(result, data);
        assert!(result.is_empty());
    }

    #[test]
    fn test_single_element_rdd() {
        // Test RDD with single element
        let data = vec![42];
        let rdd: DistributedRdd<i32> = DistributedRdd::from_vec_with_partitions(data.clone(), 3);

        let result = rdd.collect().unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn test_complex_chained_operations() {
        // Test more complex chained operations
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let rdd: DistributedRdd<i32> = DistributedRdd::from_vec_with_partitions(data.clone(), 3);

        let result_rdd = rdd
            .map(Box::new(SquareOperation)) // Square each number
            .filter(Box::new(GreaterThanPredicate { threshold: 20 })) // Keep only > 20
            .map(Box::new(AddConstantOperation { constant: 5 })); // Add 5

        let result = result_rdd.collect().unwrap();

        // Expected: square each, filter > 20, add 5
        // 1->1, 2->4, 3->9, 4->16, 5->25, 6->36, 7->49, 8->64, 9->81, 10->100
        // Filter > 20: 25, 36, 49, 64, 81, 100
        // Add 5: 30, 41, 54, 69, 86, 105
        let expected = vec![30, 41, 54, 69, 86, 105];
        assert_eq!(result, expected);
    }
}

#[cfg(test)]
mod string_string_tuple_tests {
    use super::super::*;
    use crate::rdd::DistributedRdd;

    #[test]
    fn test_string_string_tuple_swap_operation() {
        // Test SwapStringTupleOperation
        let data = vec![
            ("hello".to_string(), "world".to_string()),
            ("foo".to_string(), "bar".to_string()),
        ];
        let rdd: DistributedRdd<(String, String)> =
            DistributedRdd::from_vec_with_partitions(data.clone(), 2);
        let mapped_rdd = rdd.map(Box::new(SwapStringTupleOperation));

        let result = mapped_rdd.collect().unwrap();
        let expected = vec![
            ("world".to_string(), "hello".to_string()),
            ("bar".to_string(), "foo".to_string()),
        ];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_string_string_tuple_concat_operation() {
        // Test ConcatStringTupleOperation
        let data = vec![
            ("hello".to_string(), "world".to_string()),
            ("foo".to_string(), "bar".to_string()),
        ];
        let rdd: DistributedRdd<(String, String)> =
            DistributedRdd::from_vec_with_partitions(data.clone(), 2);
        let mapped_rdd = rdd.map(Box::new(ConcatStringTupleOperation {
            separator: "-".to_string(),
        }));

        let result = mapped_rdd.collect().unwrap();
        let expected = vec![
            ("hello-world".to_string(), "world".to_string()),
            ("foo-bar".to_string(), "bar".to_string()),
        ];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_string_string_tuple_filter_operation() {
        // Test FirstContainsSecondPredicate
        let data = vec![
            ("hello world".to_string(), "world".to_string()),
            ("foo bar".to_string(), "baz".to_string()),
            ("test string".to_string(), "test".to_string()),
        ];
        let rdd: DistributedRdd<(String, String)> =
            DistributedRdd::from_vec_with_partitions(data.clone(), 2);
        let filtered_rdd = rdd.filter(Box::new(FirstContainsSecondPredicate));

        let result = filtered_rdd.collect().unwrap();
        let expected = vec![
            ("hello world".to_string(), "world".to_string()),
            ("test string".to_string(), "test".to_string()),
        ];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_string_string_tuple_flatmap_operation() {
        // Test SplitAndCombineOperation
        let data = vec![("hello world".to_string(), "foo bar".to_string())];
        let rdd: DistributedRdd<(String, String)> =
            DistributedRdd::from_vec_with_partitions(data.clone(), 1);
        let flatmapped_rdd = rdd.flat_map(Box::new(SplitAndCombineOperation));

        let result = flatmapped_rdd.collect().unwrap();
        let expected = vec![
            ("hello".to_string(), "foo".to_string()),
            ("hello".to_string(), "bar".to_string()),
            ("world".to_string(), "foo".to_string()),
            ("world".to_string(), "bar".to_string()),
        ];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_string_string_tuple_chained_operations() {
        // Test chained operations on (String, String) tuples
        let data = vec![
            ("hello world".to_string(), "world".to_string()),
            ("foo bar".to_string(), "baz".to_string()),
            ("test string".to_string(), "test".to_string()),
        ];
        let rdd: DistributedRdd<(String, String)> =
            DistributedRdd::from_vec_with_partitions(data.clone(), 2);

        let result_rdd = rdd
            .filter(Box::new(FirstContainsSecondPredicate)) // Filter where first contains second
            .map(Box::new(SwapStringTupleOperation)); // Swap the strings

        let result = result_rdd.collect().unwrap();
        let expected = vec![
            ("world".to_string(), "hello world".to_string()),
            ("test".to_string(), "test string".to_string()),
        ];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_string_string_tuple_serialization() {
        // Test that (String, String) operations can be serialized and deserialized
        let op: Box<dyn StringStringTupleOperation> = Box::new(SwapStringTupleOperation);
        let serialized = serde_json::to_string(&op).unwrap();
        let deserialized: Box<dyn StringStringTupleOperation> =
            serde_json::from_str(&serialized).unwrap();

        let test_tuple = ("hello".to_string(), "world".to_string());
        let result = deserialized.execute(test_tuple);
        assert_eq!(result, ("world".to_string(), "hello".to_string()));
    }
}
