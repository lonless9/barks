//! DataFrame API for Barks SQL
//!
//! This module provides a high-level DataFrame API that integrates
//! DataFusion's DataFrame capabilities with Barks' distributed execution model.

use async_trait::async_trait;
use barks_core::traits::{Data, RddBase};
use barks_sql_core::traits::{SqlError, SqlResult};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{Expr, LogicalPlan, col};
use datafusion::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// A distributed DataFrame that can execute SQL operations
/// across a Barks cluster using DataFusion
#[derive(Clone)]
pub struct DistributedDataFrame {
    /// The underlying DataFusion DataFrame
    dataframe: DataFrame,
    /// Reference to the session context
    context: Arc<SessionContext>,
    /// Optional reference to the distributed context for cluster execution
    distributed_context: Option<Arc<barks_core::context::DistributedContext>>,
}

impl DistributedDataFrame {
    /// Create a new DistributedDataFrame from a DataFusion DataFrame
    pub fn new(
        dataframe: DataFrame,
        context: Arc<SessionContext>,
        distributed_context: Option<Arc<barks_core::context::DistributedContext>>,
    ) -> Self {
        Self {
            dataframe,
            context,
            distributed_context,
        }
    }

    /// Create a DataFrame from a SQL query
    pub async fn from_sql(
        sql: &str,
        context: Arc<SessionContext>,
        distributed_context: Option<Arc<barks_core::context::DistributedContext>>,
    ) -> SqlResult<Self> {
        let dataframe = context.sql(sql).await.map_err(SqlError::from)?;
        Ok(Self::new(dataframe, context, distributed_context))
    }

    /// Create a DataFrame from RecordBatches
    pub async fn from_record_batches(
        batches: Vec<RecordBatch>,
        context: Arc<SessionContext>,
        distributed_context: Option<Arc<barks_core::context::DistributedContext>>,
    ) -> SqlResult<Self> {
        if batches.is_empty() {
            return Err(SqlError::DataSource(
                "Cannot create DataFrame from empty batches".to_string(),
            ));
        }

        // Register the batches as a temporary table
        let table_name = format!("temp_table_{}", uuid::Uuid::new_v4().simple());
        let schema = batches[0].schema();
        let combined_batch = datafusion::arrow::compute::concat_batches(&schema, &batches)
            .map_err(|e| SqlError::DataSource(format!("Failed to combine batches: {}", e)))?;

        context
            .register_batch(&table_name, combined_batch)
            .map_err(SqlError::from)?;

        let dataframe = context.table(&table_name).await.map_err(SqlError::from)?;

        Ok(Self::new(dataframe, context, distributed_context))
    }

    /// Select specific columns
    pub fn select(self, exprs: Vec<Expr>) -> SqlResult<Self> {
        let new_df = self.dataframe.select(exprs).map_err(SqlError::from)?;
        Ok(Self::new(new_df, self.context, self.distributed_context))
    }

    /// Filter rows based on a predicate
    pub fn filter(self, predicate: Expr) -> SqlResult<Self> {
        let new_df = self.dataframe.filter(predicate).map_err(SqlError::from)?;
        Ok(Self::new(new_df, self.context, self.distributed_context))
    }

    /// Group by columns and apply aggregations
    pub fn group_by(self, group_expr: Vec<Expr>) -> SqlResult<DataFrameGroupBy> {
        Ok(DataFrameGroupBy {
            dataframe: self.dataframe,
            group_expr,
            context: self.context,
            distributed_context: self.distributed_context,
        })
    }

    /// Sort the DataFrame
    pub fn sort(self, exprs: Vec<Expr>) -> SqlResult<Self> {
        // Convert Expr to SortExpr
        let sort_exprs: Vec<_> = exprs
            .into_iter()
            .map(|expr| expr.sort(true, true)) // ascending, nulls first
            .collect();
        let new_df = self.dataframe.sort(sort_exprs).map_err(SqlError::from)?;
        Ok(Self::new(new_df, self.context, self.distributed_context))
    }

    /// Limit the number of rows
    pub fn limit(self, skip: usize, fetch: Option<usize>) -> SqlResult<Self> {
        let new_df = self.dataframe.limit(skip, fetch).map_err(SqlError::from)?;
        Ok(Self::new(new_df, self.context, self.distributed_context))
    }

    /// Join with another DataFrame
    pub fn join(
        self,
        right: DistributedDataFrame,
        join_type: JoinType,
        left_cols: &[&str],
        right_cols: &[&str],
        filter: Option<Expr>,
    ) -> SqlResult<Self> {
        let new_df = self
            .dataframe
            .join(right.dataframe, join_type, left_cols, right_cols, filter)
            .map_err(SqlError::from)?;
        Ok(Self::new(new_df, self.context, self.distributed_context))
    }

    /// Execute the DataFrame and collect results
    pub async fn collect(self) -> SqlResult<Vec<RecordBatch>> {
        if let Some(dist_ctx) = self.distributed_context.clone() {
            // Execute in distributed mode
            self.collect_distributed(dist_ctx).await
        } else {
            // Execute locally
            self.dataframe.collect().await.map_err(SqlError::from)
        }
    }

    /// Show the DataFrame contents (for debugging)
    pub async fn show(self) -> SqlResult<()> {
        self.dataframe.show().await.map_err(SqlError::from)
    }

    /// Get the logical plan
    pub fn logical_plan(&self) -> &LogicalPlan {
        self.dataframe.logical_plan()
    }

    /// Get the schema
    pub fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        self.dataframe.schema().inner().clone()
    }

    /// Execute in distributed mode
    async fn collect_distributed(
        self,
        _dist_ctx: Arc<barks_core::context::DistributedContext>,
    ) -> SqlResult<Vec<RecordBatch>> {
        // For now, fall back to local execution
        // TODO: Implement distributed execution by breaking down the logical plan
        // into stages and executing them across the cluster
        self.dataframe.collect().await.map_err(SqlError::from)
    }
}

/// Helper for group by operations
pub struct DataFrameGroupBy {
    dataframe: DataFrame,
    group_expr: Vec<Expr>,
    context: Arc<SessionContext>,
    distributed_context: Option<Arc<barks_core::context::DistributedContext>>,
}

impl DataFrameGroupBy {
    /// Apply aggregation functions
    pub fn agg(self, aggr_expr: Vec<Expr>) -> SqlResult<DistributedDataFrame> {
        let new_df = self
            .dataframe
            .aggregate(self.group_expr, aggr_expr)
            .map_err(SqlError::from)?;
        Ok(DistributedDataFrame::new(
            new_df,
            self.context,
            self.distributed_context,
        ))
    }

    /// Count rows in each group
    pub fn count(self) -> SqlResult<DistributedDataFrame> {
        self.agg(vec![datafusion::functions_aggregate::expr_fn::count(lit(
            1,
        ))])
    }

    /// Sum a column in each group
    pub fn sum(self, column: &str) -> SqlResult<DistributedDataFrame> {
        self.agg(vec![datafusion::functions_aggregate::expr_fn::sum(col(
            column,
        ))])
    }

    /// Average a column in each group
    pub fn avg(self, column: &str) -> SqlResult<DistributedDataFrame> {
        self.agg(vec![datafusion::functions_aggregate::expr_fn::avg(col(
            column,
        ))])
    }

    /// Find minimum value in each group
    pub fn min(self, column: &str) -> SqlResult<DistributedDataFrame> {
        self.agg(vec![datafusion::functions_aggregate::expr_fn::min(col(
            column,
        ))])
    }

    /// Find maximum value in each group
    pub fn max(self, column: &str) -> SqlResult<DistributedDataFrame> {
        self.agg(vec![datafusion::functions_aggregate::expr_fn::max(col(
            column,
        ))])
    }
}

/// Utility functions for DataFrame operations
pub mod functions {
    use super::*;

    /// Create a column reference
    pub fn col(name: &str) -> Expr {
        datafusion::logical_expr::col(name)
    }

    /// Create a literal value
    pub fn lit<T: Into<datafusion::scalar::ScalarValue> + datafusion::logical_expr::Literal>(
        value: T,
    ) -> Expr {
        datafusion::logical_expr::lit(value)
    }

    /// Create a sum aggregation
    pub fn sum(expr: Expr) -> Expr {
        datafusion::functions_aggregate::expr_fn::sum(expr)
    }

    /// Create a count aggregation
    pub fn count(expr: Expr) -> Expr {
        datafusion::functions_aggregate::expr_fn::count(expr)
    }

    /// Create an average aggregation
    pub fn avg(expr: Expr) -> Expr {
        datafusion::functions_aggregate::expr_fn::avg(expr)
    }

    /// Create a min aggregation
    pub fn min(expr: Expr) -> Expr {
        datafusion::functions_aggregate::expr_fn::min(expr)
    }

    /// Create a max aggregation
    pub fn max(expr: Expr) -> Expr {
        datafusion::functions_aggregate::expr_fn::max(expr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    #[tokio::test]
    async fn test_dataframe_from_sql() -> SqlResult<()> {
        let ctx = Arc::new(SessionContext::new());

        // Create a simple table
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            ],
        )
        .unwrap();

        ctx.register_batch("test_table", batch).unwrap();

        let df = DistributedDataFrame::from_sql("SELECT * FROM test_table WHERE id > 1", ctx, None)
            .await?;

        let results = df.collect().await?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_dataframe_operations() -> SqlResult<()> {
        let ctx = Arc::new(SessionContext::new());

        // Create test data
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])),
            ],
        )
        .unwrap();

        let df = DistributedDataFrame::from_record_batches(vec![batch], ctx, None).await?;

        // Test filter and select
        let filtered_df = df
            .filter(col("id").gt(lit(2)))
            .and_then(|df| df.select(vec![col("id"), col("value")]))?;

        let results = filtered_df.collect().await?;
        assert_eq!(results[0].num_rows(), 3);

        Ok(())
    }
}
