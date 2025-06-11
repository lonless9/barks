//! Provides an RDD-like abstraction, `DistributedDataset`.

use crate::context::BarksContext;
use crate::expression::{Expr, ExprKind, ObjectName};
use crate::logical_plan::{QueryNode, QueryPlan};
use crate::planner::to_df_logical_plan;
use crate::storage::StorageLevel;
use crate::types::NoMetadata;
use async_trait::async_trait;
use barks_common::error::{CommonError, Result};
use datafusion::arrow::array::UInt64Array;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::functions_aggregate::count::count;
use datafusion::logical_expr::{LogicalPlanBuilder, lit};
use std::sync::Arc;

/// The `DistributedDataset` trait provides an RDD-like abstraction over a distributed
/// collection of data. It encapsulates the data's lineage through a Barks `QueryPlan`.
#[async_trait]
pub trait DistributedDataset: Send + Sync {
    /// Returns the context associated with this dataset.
    fn context(&self) -> Arc<BarksContext>;

    /// Returns the logical plan that represents the computation to produce this dataset.
    fn logical_plan(&self) -> Arc<QueryPlan<NoMetadata>>;

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
    async fn filter(&self, predicate: Expr<NoMetadata>) -> Result<Arc<dyn DistributedDataset>>;
}

/// The default implementation of a `DistributedDataset`.
#[derive(Clone)]
pub struct DefaultDistributedDataset {
    /// The context for the Barks application, providing access to shared resources.
    context: Arc<BarksContext>,
    /// The logical plan representing the dataset's lineage and transformations.
    plan: Arc<QueryPlan<NoMetadata>>,
    /// Indicates if this dataset is marked for caching.
    is_cached: bool,
    /// The storage level to use for caching.
    storage_level: StorageLevel,
}

impl DefaultDistributedDataset {
    /// Creates a new `DefaultDistributedDataset` from a context and a logical plan.
    pub fn new(context: Arc<BarksContext>, plan: Arc<QueryPlan<NoMetadata>>) -> Self {
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

    fn logical_plan(&self) -> Arc<QueryPlan<NoMetadata>> {
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
        let df_plan = to_df_logical_plan(self.plan.as_ref())?;
        let df = self
            .context
            .df_session_ctx()
            .execute_logical_plan(df_plan)
            .await
            .map_err(CommonError::from)?;
        let results = df.collect().await.map_err(CommonError::from)?;
        Ok(results)
    }

    async fn count(&self) -> Result<u64> {
        let df_plan = to_df_logical_plan(self.plan.as_ref())?;

        let count_plan = LogicalPlanBuilder::from(df_plan)
            .aggregate(
                Vec::<datafusion::logical_expr::Expr>::new(),
                vec![count(lit(1)).alias("count")],
            )?
            .build()?;

        let df = self
            .context
            .df_session_ctx()
            .execute_logical_plan(count_plan)
            .await
            .map_err(CommonError::from)?;
        let results = df.collect().await.map_err(CommonError::from)?;

        if results.is_empty() {
            return Ok(0);
        }

        let batch = &results[0];
        if batch.num_rows() == 0 {
            return Ok(0);
        }

        let count_column = batch
            .column_by_name("count")
            .ok_or_else(|| CommonError::internal_error("Count column not found"))?;

        // Try different numeric types that count() might return
        if let Some(array) = count_column.as_any().downcast_ref::<UInt64Array>() {
            Ok(array.value(0))
        } else if let Some(array) = count_column
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
        {
            Ok(array.value(0) as u64)
        } else {
            Err(CommonError::internal_error(format!(
                "Count column has unexpected type: {:?}",
                count_column.data_type()
            )))
        }
    }

    async fn select(&self, cols: Vec<&str>) -> Result<Arc<dyn DistributedDataset>> {
        let projections: Vec<Expr<NoMetadata>> = cols
            .into_iter()
            .map(|c| Expr {
                kind: ExprKind::UnresolvedAttribute {
                    name: ObjectName::bare(c),
                    plan_id: None,
                    is_metadata_column: false,
                },
                metadata: NoMetadata,
            })
            .collect();

        let new_plan = QueryPlan {
            node: QueryNode::Project {
                input: Some(Box::new(self.plan.as_ref().clone())),
                expressions: projections,
            },
            plan_id: None,
            metadata: NoMetadata,
        };

        Ok(Arc::new(DefaultDistributedDataset::new(
            self.context(),
            Arc::new(new_plan),
        )))
    }

    async fn filter(&self, predicate: Expr<NoMetadata>) -> Result<Arc<dyn DistributedDataset>> {
        let new_plan = QueryPlan {
            node: QueryNode::Filter {
                input: Box::new(self.plan.as_ref().clone()),
                condition: predicate,
            },
            plan_id: None,
            metadata: NoMetadata,
        };
        Ok(Arc::new(DefaultDistributedDataset::new(
            self.context(),
            Arc::new(new_plan),
        )))
    }
}
