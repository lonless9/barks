//! Tests for distributed context improvements from TODO0
//!
//! These tests verify the fixes implemented for the distributed computing framework

use barks_core::{DistributedConfig, DistributedContext, ExecutionMode, SimpleRdd};

#[tokio::test]
async fn test_distributed_context_rejects_transformed_rdds() {
    // This test verifies that transformed RDDs are properly rejected in distributed mode
    // instead of silently falling back to local execution.

    // Create a distributed context in driver mode
    let config = DistributedConfig::default();
    let context = DistributedContext::new_driver("test-app".to_string(), config);

    // Create a base RDD and apply a transformation
    let data = vec![1, 2, 3, 4, 5];
    let base_rdd = SimpleRdd::from_vec(data);
    let transformed_rdd = base_rdd.map(|x| x * 2);

    // Try to run the transformed RDD in distributed mode
    // This should now return an error instead of falling back to local execution
    let result = context.run(transformed_rdd).await;

    // The result should be an error because transformed SimpleRdds cannot be distributed
    assert!(result.is_err());
    if let Err(error) = result {
        // Verify the error message mentions non-serializable closures
        let error_msg = error.to_string();
        assert!(error_msg.contains("non-serializable closures"));
        assert!(error_msg.contains("DistributedRdd"));
    }
}

#[tokio::test]
async fn test_distributed_context_accepts_base_rdds_without_executors() {
    // Create a distributed context in driver mode
    let config = DistributedConfig::default();
    let context = DistributedContext::new_driver("test-app".to_string(), config);

    // Create a base RDD
    let data = vec![1, 2, 3, 4, 5];
    let base_rdd = SimpleRdd::from_vec(data.clone());

    // Try to run the base RDD in distributed mode
    // Since there are no executors, it should fall back to local execution
    let result = context.run(base_rdd).await;

    assert!(result.is_ok());
    let collected = result.unwrap();
    assert_eq!(collected, data);
}

#[test]
fn test_execution_mode_variants() {
    // Test that all execution modes are properly defined
    let driver_mode = ExecutionMode::Driver;
    let executor_mode = ExecutionMode::Executor;
    let local_mode = ExecutionMode::Local;

    assert_eq!(driver_mode, ExecutionMode::Driver);
    assert_eq!(executor_mode, ExecutionMode::Executor);
    assert_eq!(local_mode, ExecutionMode::Local);
}

#[test]
fn test_distributed_config_default() {
    // Test that the default configuration is reasonable
    let config = DistributedConfig::default();

    // Should have reasonable defaults
    assert!(config.default_parallelism > 0);
    assert!(config.executor_config.cores > 0);
    assert!(config.executor_config.memory_mb > 0);
}
