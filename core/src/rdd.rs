//! Provides an RDD-like abstraction, `DistributedDataset`, on top of DataFusion.

use crate::context::BarksContext;
use crate::storage::StorageLevel;
use async_trait::async_trait;
use barks_common::error::{CommonError, Result};
use datafusion::arrow::array::UInt64Array;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::functions_aggregate::count::count;
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, col, lit};
use std::sync::Arc;

/// The `DistributedDataset` trait provides an RDD-like abstraction over a distributed
/// collection of data. It encapsulates the data's lineage through a DataFusion `LogicalPlan`.
#[async_trait]
pub trait DistributedDataset: Send + Sync {
    /// Returns the context associated with this dataset.
    fn context(&self) -> Arc<BarksContext>;

    /// Returns the logical plan that represents the computation to produce this dataset.
    fn logical_plan(&self) -> Arc<LogicalPlan>;

    /// Persist this dataset with the default storage level (`MEMORY_ONLY`).
    ///
    /// This is a lazy operation; the dataset will be cached only when an action is triggered.
    async fn cache(&self) -> Arc<dyn DistributedDataset>;

    /// Persist this dataset with a specific `StorageLevel`.
    ///
    /// This is a lazy operation; the dataset will be cached only when an action is triggered.
    async fn persist(&self, level: StorageLevel) -> Arc<dyn DistributedDataset>;

    // --- Actions ---

    /// Returns all elements of the dataset as a collection of `RecordBatch`es to the driver.
    async fn collect(&self) -> Result<Vec<RecordBatch>>;

    /// Returns the number of elements in the dataset.
    async fn count(&self) -> Result<u64>;

    // --- Transformations ---

    /// Returns a new dataset by selecting a set of columns (projection).
    async fn select(&self, cols: Vec<&str>) -> Result<Arc<dyn DistributedDataset>>;

    /// Returns a new dataset containing only the elements that satisfy a predicate.
    async fn filter(&self, predicate: Expr) -> Result<Arc<dyn DistributedDataset>>;
}

/// The default implementation of a `DistributedDataset`.
#[derive(Clone)]
pub struct DefaultDistributedDataset {
    /// The context for the Barks application, providing access to shared resources.
    context: Arc<BarksContext>,
    /// The logical plan representing the dataset's lineage and transformations.
    plan: Arc<LogicalPlan>,
    /// Indicates if this dataset is marked for caching.
    is_cached: bool,
    /// The storage level to use for caching.
    storage_level: StorageLevel,
}

impl DefaultDistributedDataset {
    /// Creates a new `DefaultDistributedDataset` from a context and a logical plan.
    pub fn new(context: Arc<BarksContext>, plan: Arc<LogicalPlan>) -> Self {
        Self {
            context,
            plan,
            is_cached: false,
            storage_level: StorageLevel::default(),
        }
    }
}

#[async_trait]
impl DistributedDataset for DefaultDistributedDataset {
    fn context(&self) -> Arc<BarksContext> {
        self.context.clone()
    }

    fn logical_plan(&self) -> Arc<LogicalPlan> {
        self.plan.clone()
    }

    async fn cache(&self) -> Arc<dyn DistributedDataset> {
        self.persist(StorageLevel::MEMORY_ONLY).await
    }

    async fn persist(&self, level: StorageLevel) -> Arc<dyn DistributedDataset> {
        // In a real implementation, this would register the dataset with a CacheManager.
        // For now, we just clone the dataset and update its caching metadata.
        // The lineage (plan) remains the same.
        let mut new_dataset = self.clone();
        new_dataset.is_cached = true;
        new_dataset.storage_level = level;
        Arc::new(new_dataset)
    }

    async fn collect(&self) -> Result<Vec<RecordBatch>> {
        let df = self
            .context
            .df_session_ctx()
            .execute_logical_plan(self.plan.as_ref().clone())
            .await?;
        let results = df.collect().await?;
        Ok(results)
    }

    async fn count(&self) -> Result<u64> {
        // Create an aggregate plan to count all rows
        let count_plan = LogicalPlanBuilder::from(self.plan.as_ref().clone())
            .aggregate(Vec::<Expr>::new(), vec![count(lit(1)).alias("count")])?
            .build()?;

        let df = self
            .context
            .df_session_ctx()
            .execute_logical_plan(count_plan)
            .await?;
        let results = df.collect().await?;

        if results.is_empty() {
            return Ok(0);
        }

        let batch = &results[0];
        if batch.num_rows() == 0 {
            return Ok(0);
        }

        let count_array = batch
            .column_by_name("count")
            .ok_or_else(|| CommonError::internal_error("Count column not found"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| CommonError::internal_error("Count column is not UInt64"))?;

        Ok(count_array.value(0))
    }

    async fn select(&self, cols: Vec<&str>) -> Result<Arc<dyn DistributedDataset>> {
        let projections: Vec<Expr> = cols.into_iter().map(col).collect();
        let new_plan = LogicalPlanBuilder::from(self.plan.as_ref().clone())
            .project(projections)?
            .build()?;
        Ok(Arc::new(DefaultDistributedDataset::new(
            self.context(),
            Arc::new(new_plan),
        )))
    }

    async fn filter(&self, predicate: Expr) -> Result<Arc<dyn DistributedDataset>> {
        let new_plan = LogicalPlanBuilder::from(self.plan.as_ref().clone())
            .filter(predicate)?
            .build()?;
        Ok(Arc::new(DefaultDistributedDataset::new(
            self.context(),
            Arc::new(new_plan),
        )))
    }
}
