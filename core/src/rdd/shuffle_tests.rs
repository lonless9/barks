//! Comprehensive tests for shuffle operations

#[cfg(test)]
mod tests {
    use crate::context::FlowContext;
    use crate::rdd::transformations::PairRddExt;
    use crate::shuffle::HashPartitioner;
    use std::sync::Arc;

    #[test]
    fn test_reduce_by_key() {
        let context = FlowContext::new("test_reduce_by_key");

        // Create test data: (key, value) pairs
        let data = vec![
            ("a".to_string(), 1),
            ("b".to_string(), 2),
            ("a".to_string(), 3),
            ("c".to_string(), 4),
            ("b".to_string(), 5),
            ("a".to_string(), 6),
        ];

        let rdd = context.parallelize_with_partitions(data, 2);
        let partitioner = Arc::new(HashPartitioner::new(2));

        // Test reduce_by_key
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
        let context = FlowContext::new("test_group_by_key");

        // Create test data: (key, value) pairs
        let data = vec![
            ("x".to_string(), 1),
            ("y".to_string(), 2),
            ("x".to_string(), 3),
            ("z".to_string(), 4),
            ("y".to_string(), 5),
            ("x".to_string(), 6),
        ];

        let rdd = context.parallelize_with_partitions(data, 2);
        let partitioner = Arc::new(HashPartitioner::new(2));

        // Test group_by_key
        let grouped = rdd.group_by_key(partitioner);
        let result = grouped.collect().unwrap();

        // Convert to HashMap for easier testing
        let result_map: std::collections::HashMap<String, Vec<i32>> = result.into_iter().collect();

        // Check that all values for each key are grouped together
        let mut x_values = result_map.get("x").unwrap().clone();
        x_values.sort();
        assert_eq!(x_values, vec![1, 3, 6]);

        let mut y_values = result_map.get("y").unwrap().clone();
        y_values.sort();
        assert_eq!(y_values, vec![2, 5]);

        assert_eq!(result_map.get("z").unwrap(), &vec![4]);
    }

    #[test]
    fn test_sort_by_key() {
        let context = FlowContext::new("test_sort_by_key");

        // Create test data: (key, value) pairs
        let data = vec![
            (3, "three".to_string()),
            (1, "one".to_string()),
            (4, "four".to_string()),
            (2, "two".to_string()),
            (5, "five".to_string()),
        ];

        let rdd = context.parallelize_with_partitions(data, 2);

        // Test sort_by_key ascending
        let sorted_asc = rdd.clone().sort_by_key(true);
        let result_asc = sorted_asc.collect().unwrap();

        // Check that results are sorted by key in ascending order
        let keys_asc: Vec<i32> = result_asc.iter().map(|(k, _)| *k).collect();
        assert_eq!(keys_asc, vec![1, 2, 3, 4, 5]);

        // Test sort_by_key descending
        let sorted_desc = rdd.sort_by_key(false);
        let result_desc = sorted_desc.collect().unwrap();

        // Check that results are sorted by key in descending order
        let keys_desc: Vec<i32> = result_desc.iter().map(|(k, _)| *k).collect();
        assert_eq!(keys_desc, vec![5, 4, 3, 2, 1]);
    }

    #[test]
    fn test_join() {
        let context = FlowContext::new("test_join");

        // Create test data for left RDD
        let left_data = vec![
            ("a".to_string(), 1),
            ("b".to_string(), 2),
            ("c".to_string(), 3),
        ];

        // Create test data for right RDD - use String instead of &str for RddDataType compatibility
        let right_data = vec![
            ("a".to_string(), "alpha".to_string()),
            ("b".to_string(), "beta".to_string()),
            ("d".to_string(), "delta".to_string()),
        ];

        let left_rdd = context.parallelize_with_partitions(left_data, 2);
        let right_rdd = context.parallelize_with_partitions(right_data, 2);
        let partitioner = Arc::new(HashPartitioner::new(2));

        // Test join
        let joined = left_rdd.join(Arc::new(right_rdd), partitioner);
        let result = joined.collect().unwrap();

        // Convert to HashMap for easier testing
        let result_map: std::collections::HashMap<String, (i32, String)> =
            result.into_iter().collect();

        // Only keys present in both RDDs should appear in the result
        assert_eq!(result_map.get("a"), Some(&(1, "alpha".to_string())));
        assert_eq!(result_map.get("b"), Some(&(2, "beta".to_string())));
        assert_eq!(result_map.get("c"), None); // Not in right RDD
        assert_eq!(result_map.get("d"), None); // Not in left RDD
        assert_eq!(result_map.len(), 2);
    }

    #[test]
    fn test_cogroup() {
        let context = FlowContext::new("test_cogroup");

        // Create test data for left RDD
        let left_data = vec![
            ("a".to_string(), 1),
            ("b".to_string(), 2),
            ("a".to_string(), 3),
        ];

        // Create test data for right RDD - use String instead of &str
        let right_data = vec![
            ("a".to_string(), "alpha".to_string()),
            ("c".to_string(), "gamma".to_string()),
            ("a".to_string(), "alpha2".to_string()),
        ];

        let left_rdd = context.parallelize_with_partitions(left_data, 2);
        let right_rdd = context.parallelize_with_partitions(right_data, 2);
        let partitioner = Arc::new(HashPartitioner::new(2));

        // Test cogroup
        let cogrouped = left_rdd.cogroup(Arc::new(right_rdd), partitioner);
        let result = cogrouped.collect().unwrap();

        // Convert to HashMap for easier testing
        let result_map: std::collections::HashMap<String, (Vec<i32>, Vec<String>)> =
            result.into_iter().collect();

        // Check cogrouped results
        let (a_left, a_right) = result_map.get("a").unwrap();
        let mut a_left_sorted = a_left.clone();
        a_left_sorted.sort();
        let mut a_right_sorted = a_right.clone();
        a_right_sorted.sort();
        assert_eq!(a_left_sorted, vec![1, 3]);
        assert_eq!(
            a_right_sorted,
            vec!["alpha".to_string(), "alpha2".to_string()]
        );

        let (b_left, b_right) = result_map.get("b").unwrap();
        assert_eq!(b_left, &vec![2]);
        assert_eq!(b_right, &Vec::<String>::new()); // Empty for right side

        let (c_left, c_right) = result_map.get("c").unwrap();
        assert_eq!(c_left, &Vec::<i32>::new()); // Empty for left side
        assert_eq!(c_right, &vec!["gamma".to_string()]);
    }

    #[test]
    fn test_combine_by_key() {
        let context = FlowContext::new("test_combine_by_key");

        // Create test data: (key, value) pairs
        let data = vec![
            ("a".to_string(), 1),
            ("b".to_string(), 2),
            ("a".to_string(), 3),
            ("c".to_string(), 4),
            ("b".to_string(), 5),
        ];

        let rdd = context.parallelize_with_partitions(data, 2);
        let partitioner = Arc::new(HashPartitioner::new(2));

        // Create a custom aggregator that sums values
        let aggregator = Arc::new(crate::shuffle::CombineAggregator::new(
            |v: i32| v,                 // create_combiner: just return the value
            |c: i32, v: i32| c + v,     // merge_value: add value to combiner
            |c1: i32, c2: i32| c1 + c2, // merge_combiners: add combiners
        ));

        // Test combine_by_key
        let combined = rdd.combine_by_key(aggregator, partitioner);
        let result = combined.collect().unwrap();

        // Convert to HashMap for easier testing
        let result_map: std::collections::HashMap<String, i32> = result.into_iter().collect();

        assert_eq!(result_map.get("a"), Some(&4)); // 1 + 3
        assert_eq!(result_map.get("b"), Some(&7)); // 2 + 5
        assert_eq!(result_map.get("c"), Some(&4)); // 4
    }
}
