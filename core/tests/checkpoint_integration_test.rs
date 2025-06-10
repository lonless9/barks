//! Integration tests for RDD checkpointing functionality
//!
//! These tests verify the complete checkpointing workflow, including:
//! - Creating checkpointed RDDs
//! - Materializing checkpoints
//! - Lineage truncation
//! - Recovery from checkpoints

use barks_core::context::DistributedContext;
use barks_core::rdd::{CheckpointedRdd, DistributedRdd};
use barks_core::traits::{IsRdd, RddBase};
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn test_basic_checkpointing() {
    let temp_dir = tempdir().unwrap();
    let mut context = DistributedContext::new_local("checkpoint-test".to_string());
    context.set_checkpoint_dir(temp_dir.path());

    // Create an RDD with some data
    let data = vec![1, 2, 3, 4, 5, 6, 7, 8];
    let rdd: Arc<dyn RddBase<Item = i32>> =
        Arc::new(DistributedRdd::from_vec_with_partitions(data.clone(), 2));

    // Checkpoint the RDD
    let checkpointed_rdd = context.checkpoint_rdd(rdd.clone()).await.unwrap();

    // Verify the checkpointed RDD properties
    assert_eq!(checkpointed_rdd.num_partitions(), 2);
    assert!(checkpointed_rdd.is_materialized());
    assert_eq!(checkpointed_rdd.dependencies().len(), 0); // No dependencies - lineage truncated

    // Verify we can compute from the checkpoint
    let partition = barks_core::traits::BasicPartition::new(0);
    let result = checkpointed_rdd.compute(&partition).unwrap();
    let partition_data: Vec<i32> = result.collect();
    assert!(!partition_data.is_empty());
}

#[tokio::test]
async fn test_checkpoint_lineage_truncation() {
    let temp_dir = tempdir().unwrap();
    let mut context = DistributedContext::new_local("lineage-test".to_string());
    context.set_checkpoint_dir(temp_dir.path());

    // Create a chain of RDD transformations
    let data = vec![1, 2, 3, 4, 5, 6, 7, 8];
    let base_rdd = DistributedRdd::from_vec_with_partitions(data, 2);

    // Apply some transformations to create a lineage
    let mapped_rdd = base_rdd.map(Box::new(barks_core::operations::DoubleOperation));
    let filtered_rdd = mapped_rdd.filter(Box::new(barks_core::operations::GreaterThanPredicate {
        threshold: 5,
    }));

    let final_rdd: Arc<dyn RddBase<Item = i32>> = Arc::new(filtered_rdd);

    // Before checkpointing, the RDD should have dependencies
    assert!(!final_rdd.dependencies().is_empty());

    // Checkpoint the RDD
    let checkpointed_rdd = context.checkpoint_rdd(final_rdd).await.unwrap();

    // After checkpointing, the checkpointed RDD should have no dependencies
    assert_eq!(checkpointed_rdd.dependencies().len(), 0);
    assert!(checkpointed_rdd.is_materialized());

    // Verify the data is correct
    let partition = barks_core::traits::BasicPartition::new(0);
    let result = checkpointed_rdd.compute(&partition).unwrap();
    let partition_data: Vec<i32> = result.collect();

    // The data should be the result of the transformations: double then filter > 5
    // Original: [1, 2, 3, 4, 5, 6, 7, 8]
    // After double: [2, 4, 6, 8, 10, 12, 14, 16]
    // After filter > 5: [6, 8, 10, 12, 14, 16]
    // But we only get partition 0, so we'll just verify it's not empty and contains expected values
    assert!(!partition_data.is_empty());
    for value in &partition_data {
        assert!(*value > 5); // All values should be > 5 due to the filter
        assert!(*value % 2 == 0); // All values should be even due to the doubling
    }
}

#[tokio::test]
async fn test_checkpoint_without_directory_fails() {
    let context = DistributedContext::new_local("no-dir-test".to_string());
    // Don't set checkpoint directory

    let data = vec![1, 2, 3, 4];
    let rdd: Arc<dyn RddBase<Item = i32>> =
        Arc::new(DistributedRdd::from_vec_with_partitions(data, 2));

    // Checkpointing should fail without a directory set
    let result = context.checkpoint_rdd(rdd).await;
    assert!(result.is_err());

    if let Err(error) = result {
        match error {
            barks_core::traits::RddError::CheckpointError(msg) => {
                assert!(msg.contains("Checkpoint directory not set"));
            }
            _ => panic!("Expected CheckpointError"),
        }
    }
}

#[tokio::test]
async fn test_multiple_checkpoints() {
    let temp_dir = tempdir().unwrap();
    let mut context = DistributedContext::new_local("multi-checkpoint-test".to_string());
    context.set_checkpoint_dir(temp_dir.path());

    // Create multiple RDDs and checkpoint them
    let data1 = vec![1, 2, 3, 4];
    let data2 = vec![5, 6, 7, 8];

    let rdd1: Arc<dyn RddBase<Item = i32>> =
        Arc::new(DistributedRdd::from_vec_with_partitions(data1, 2));
    let rdd2: Arc<dyn RddBase<Item = i32>> =
        Arc::new(DistributedRdd::from_vec_with_partitions(data2, 2));

    // Checkpoint both RDDs
    let checkpoint1 = context.checkpoint_rdd(rdd1).await.unwrap();
    let checkpoint2 = context.checkpoint_rdd(rdd2).await.unwrap();

    // Both should be materialized and have different IDs
    assert!(checkpoint1.is_materialized());
    assert!(checkpoint2.is_materialized());
    assert_ne!(checkpoint1.id(), checkpoint2.id());

    // Both should have no dependencies
    assert_eq!(checkpoint1.dependencies().len(), 0);
    assert_eq!(checkpoint2.dependencies().len(), 0);

    // Verify we can compute from both checkpoints
    let partition = barks_core::traits::BasicPartition::new(0);

    let result1 = checkpoint1.compute(&partition).unwrap();
    let data1: Vec<i32> = result1.collect();
    assert!(!data1.is_empty());

    let result2 = checkpoint2.compute(&partition).unwrap();
    let data2: Vec<i32> = result2.collect();
    assert!(!data2.is_empty());
}

#[test]
fn test_checkpointed_rdd_properties() {
    let temp_dir = tempdir().unwrap();
    let storage_path = temp_dir.path().to_path_buf();

    let original_rdd: Arc<dyn RddBase<Item = i32>> = Arc::new(
        DistributedRdd::from_vec_with_partitions(vec![1, 2, 3, 4], 2),
    );

    let checkpointed = CheckpointedRdd::new(
        42,
        "test_checkpoint".to_string(),
        2,
        storage_path,
        Some(original_rdd),
    )
    .unwrap();

    // Test basic properties
    assert_eq!(checkpointed.id(), 42);
    assert_eq!(checkpointed.num_partitions(), 2);
    assert!(!checkpointed.is_materialized());

    // Test partitions
    let partitions = checkpointed.partitions();
    assert_eq!(partitions.len(), 2);
    assert_eq!(partitions[0].index(), 0);
    assert_eq!(partitions[1].index(), 1);
    assert!(partitions[0].id().contains("test_checkpoint"));
    assert!(partitions[1].id().contains("test_checkpoint"));

    // Test dependencies (should be empty)
    assert_eq!(checkpointed.dependencies().len(), 0);
}

#[tokio::test]
async fn test_checkpoint_compute_before_materialization_fails() {
    let temp_dir = tempdir().unwrap();
    let storage_path = temp_dir.path().to_path_buf();

    let original_rdd: Arc<dyn RddBase<Item = i32>> = Arc::new(
        DistributedRdd::from_vec_with_partitions(vec![1, 2, 3, 4], 2),
    );

    let checkpointed = CheckpointedRdd::new(
        1,
        "test_checkpoint".to_string(),
        2,
        storage_path,
        Some(original_rdd),
    )
    .unwrap();

    // Try to compute before materialization - should fail
    let partition = barks_core::traits::BasicPartition::new(0);
    let result = checkpointed.compute(&partition);

    assert!(result.is_err());
    if let Err(error) = result {
        match error {
            barks_core::traits::RddError::CheckpointError(msg) => {
                assert!(msg.contains("not materialized"));
            }
            _ => panic!("Expected CheckpointError"),
        }
    }
}
