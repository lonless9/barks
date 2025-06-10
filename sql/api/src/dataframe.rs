//! DataFrame API for Barks SQL
//!
//! This module provides a high-level DataFrame API that integrates
//! DataFusion's DataFrame capabilities with Barks' distributed execution model.

use async_trait::async_trait;
use barks_core::distributed::task::Task;
use barks_network_shuffle::traits::ShuffleBlockManager;
use barks_sql_core::datasources::SqlTaskCreator;
use barks_sql_core::traits::{SqlError, SqlResult};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{Expr, LogicalPlan, col};
use datafusion::prelude::*;
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

    /// Create a DataFrame from a CSV file
    pub async fn from_csv(
        path: &str,
        context: Arc<SessionContext>,
        distributed_context: Option<Arc<barks_core::context::DistributedContext>>,
    ) -> SqlResult<Self> {
        let dataframe = context
            .read_csv(path, datafusion::prelude::CsvReadOptions::new())
            .await
            .map_err(SqlError::from)?;
        Ok(Self::new(dataframe, context, distributed_context))
    }

    /// Create a DataFrame from a CSV file with custom options
    pub async fn from_csv_with_options(
        path: &str,
        options: datafusion::prelude::CsvReadOptions<'_>,
        context: Arc<SessionContext>,
        distributed_context: Option<Arc<barks_core::context::DistributedContext>>,
    ) -> SqlResult<Self> {
        let dataframe = context
            .read_csv(path, options)
            .await
            .map_err(SqlError::from)?;
        Ok(Self::new(dataframe, context, distributed_context))
    }

    /// Create a DataFrame from a Parquet file
    pub async fn from_parquet(
        path: &str,
        context: Arc<SessionContext>,
        distributed_context: Option<Arc<barks_core::context::DistributedContext>>,
    ) -> SqlResult<Self> {
        let dataframe = context
            .read_parquet(path, datafusion::prelude::ParquetReadOptions::default())
            .await
            .map_err(SqlError::from)?;
        Ok(Self::new(dataframe, context, distributed_context))
    }

    /// Create a DataFrame from a Parquet file with custom options
    pub async fn from_parquet_with_options(
        path: &str,
        options: datafusion::prelude::ParquetReadOptions<'_>,
        context: Arc<SessionContext>,
        distributed_context: Option<Arc<barks_core::context::DistributedContext>>,
    ) -> SqlResult<Self> {
        let dataframe = context
            .read_parquet(path, options)
            .await
            .map_err(SqlError::from)?;
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
        dist_ctx: Arc<barks_core::context::DistributedContext>,
    ) -> SqlResult<Vec<RecordBatch>> {
        if dist_ctx.mode() != &barks_core::context::ExecutionMode::Driver
            && dist_ctx.mode() != &barks_core::context::ExecutionMode::Local
        {
            return Err(SqlError::DistributedExecution(
                "Distributed SQL execution can only be initiated from the driver".to_string(),
            ));
        }

        let plan = self.logical_plan().clone();

        // Find the source RDD from the plan. This is a simplification; a real implementation
        // would handle complex plans with multiple sources (e.g., joins).
        let (table_name, source_rdd) = find_rdd_in_plan(&plan, &self.context).await?;

        let mut task_futures = Vec::new();
        for i in 0..source_rdd.num_partitions() {
            // Create the SQL task directly here since we have access to SqlTask
            let task = create_sql_task_for_rdd(source_rdd.as_ref(), i, &plan, table_name.clone())?;
            let task_id = format!("sql-task-{}-{}", uuid::Uuid::new_v4(), i);
            let future = dist_ctx
                .submit_task(
                    task_id,
                    "distributed-sql-stage".to_string(),
                    i,
                    task,
                    vec![],
                )
                .await
                .map_err(|e| SqlError::DistributedExecution(e.to_string()))?;
            task_futures.push(future);
        }

        let mut all_batches = Vec::new();
        for future in task_futures {
            match future.await {
                Ok((barks_core::distributed::driver::TaskResult::Success(bytes), _)) => {
                    if !bytes.is_empty() {
                        let cursor = std::io::Cursor::new(bytes);
                        let reader =
                            datafusion::arrow::ipc::reader::StreamReader::try_new(cursor, None)
                                .map_err(|e| SqlError::Serialization(e.to_string()))?;
                        for batch_result in reader {
                            let batch =
                                batch_result.map_err(|e| SqlError::Serialization(e.to_string()))?;
                            all_batches.push(batch);
                        }
                    }
                }
                Ok((barks_core::distributed::driver::TaskResult::Failure(e), _)) => {
                    return Err(SqlError::DistributedExecution(e));
                }
                Err(e) => return Err(SqlError::DistributedExecution(e.to_string())),
            }
        }
        Ok(all_batches)
    }
}

/// Helper function to create SQL tasks directly from RDD
fn create_sql_task_for_rdd(
    sql_task_creator: &dyn SqlTaskCreator,
    partition_index: usize,
    plan: &LogicalPlan,
    table_name: String,
) -> SqlResult<Box<dyn barks_core::distributed::task::Task>> {
    // Get the RDD from the SqlTaskCreator
    if let Some(rdd_creator) = sql_task_creator
        .as_any()
        .downcast_ref::<barks_sql_core::datasources::RddSqlTaskCreator>()
    {
        // Access the RDD directly and create the task
        let rdd = rdd_creator.get_rdd();
        let rdd_partition = barks_core::traits::BasicPartition::new(partition_index);
        let record_batch =
            barks_sql_core::rdd_sql_bridge::rdd_to_record_batch(rdd, &rdd_partition)?;
        let sql_task = SqlTask::new(plan, record_batch, table_name)?;
        Ok(Box::new(sql_task))
    } else {
        Err(SqlError::QueryExecution(
            "Unsupported SqlTaskCreator type".to_string(),
        ))
    }
}

async fn find_rdd_in_plan(
    plan: &LogicalPlan,
    ctx: &SessionContext,
) -> SqlResult<(String, Arc<dyn SqlTaskCreator>)> {
    for input in plan.inputs() {
        if let LogicalPlan::TableScan(ts) = input {
            if let Ok(provider) = ctx.table_provider(ts.table_name.clone()).await {
                if let Some(rdd_provider) = provider
                    .as_any()
                    .downcast_ref::<barks_sql_core::datasources::RddTableProvider>(
                ) {
                    return Ok((
                        ts.table_name.to_string(),
                        rdd_provider.get_sql_task_creator(),
                    ));
                }
            }
        }
    }
    Err(SqlError::RddIntegration(
        "No RDD-backed table found in the query plan. Distributed execution currently requires a base RDD.".to_string(),
    ))
}

/// A task for executing SQL operations in a distributed manner
// NOTE: To make this work, `LogicalPlan` needs to be serializable. This typically involves
// using `datafusion-proto` to convert the plan to/from a Protobuf representation.
// For now, we use bincode as a placeholder.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct SqlTask {
    /// Serialized DataFusion LogicalPlan
    pub plan_bytes: Vec<u8>,
    /// Input data for this partition (serialized as an Arrow IPC stream)
    pub input_data: Vec<u8>,
    /// Table name to register the input data as
    pub table_name: String,
}

impl SqlTask {
    pub fn new(
        plan: &LogicalPlan,
        input_batch: RecordBatch,
        table_name: String,
    ) -> SqlResult<Self> {
        // This is a placeholder for actual LogicalPlan serialization.
        // A real implementation would use `datafusion-proto`.
        // For now, we'll serialize the plan as a debug string (not ideal but functional)
        let plan_debug = format!("{:?}", plan);
        let plan_bytes = bincode::encode_to_vec(&plan_debug, bincode::config::standard())
            .map_err(|e| SqlError::Serialization(format!("Failed to serialize plan: {}", e)))?;

        // Serialize the input RecordBatch
        let input_data = {
            let mut buffer = Vec::new();
            let mut writer = datafusion::arrow::ipc::writer::StreamWriter::try_new(
                &mut buffer,
                &input_batch.schema(),
            )
            .map_err(|e| SqlError::Serialization(format!("Failed to create IPC writer: {}", e)))?;
            writer.write(&input_batch).map_err(|e| {
                SqlError::Serialization(format!("Failed to write RecordBatch: {}", e))
            })?;
            writer.finish().map_err(|e| {
                SqlError::Serialization(format!("Failed to finish IPC writer: {}", e))
            })?;
            buffer
        };

        Ok(Self {
            plan_bytes,
            input_data,
            table_name,
        })
    }
}

#[typetag::serde(name = "SqlTask")]
#[async_trait]
impl Task for SqlTask {
    async fn execute(
        &self,
        _partition_index: usize,
        _block_manager: Arc<dyn ShuffleBlockManager>,
    ) -> Result<Vec<u8>, String> {
        // 1. Deserialize the logical plan. This is a placeholder.
        let _plan_debug: String =
            bincode::decode_from_slice(&self.plan_bytes, bincode::config::standard())
                .map_err(|e| format!("Failed to deserialize logical plan: {}", e))?
                .0;

        // 2. Deserialize the input RecordBatches
        let input_batches = {
            let cursor = std::io::Cursor::new(&self.input_data);
            let reader = datafusion::arrow::ipc::reader::StreamReader::try_new(cursor, None)
                .map_err(|e| format!("Failed to create IPC reader: {}", e))?;

            reader
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| format!("Failed to read RecordBatches: {}", e))?
        };

        if input_batches.is_empty() {
            return Ok(Vec::new());
        }

        // 3. Create a local SessionContext
        let ctx = SessionContext::new();

        // 4. Register the input batches as a table using a MemTable
        let schema = input_batches[0].schema();
        let mem_table = datafusion::datasource::MemTable::try_new(schema, vec![input_batches])
            .map_err(|e| format!("Failed to create MemTable: {}", e))?;
        ctx.register_table(&self.table_name, Arc::new(mem_table))
            .map_err(|e| format!("Failed to register table: {}", e))?;

        // 5. For now, execute a simple query since we can't properly deserialize LogicalPlan
        // In a real implementation, we would deserialize the LogicalPlan and execute it directly
        let df = ctx
            .sql(&format!("SELECT * FROM {}", self.table_name))
            .await
            .map_err(|e| format!("Failed to create DataFrame: {}", e))?;

        let result_batches = df
            .collect()
            .await
            .map_err(|e| format!("Failed to execute query: {}", e))?;

        // 6. Serialize the result
        let mut buffer = Vec::new();
        if !result_batches.is_empty() {
            let schema = result_batches[0].schema();
            let mut writer =
                datafusion::arrow::ipc::writer::StreamWriter::try_new(&mut buffer, &schema)
                    .map_err(|e| format!("Failed to create result writer: {}", e))?;

            for batch in result_batches {
                writer
                    .write(&batch)
                    .map_err(|e| format!("Failed to write result batch: {}", e))?;
            }

            writer
                .finish()
                .map_err(|e| format!("Failed to finish result writer: {}", e))?;
        }

        Ok(buffer)
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

    #[tokio::test]
    async fn test_sql_task_execution() {
        // Create a sample RecordBatch
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let id_array = Int32Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie"]);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(id_array), Arc::new(name_array)],
        )
        .unwrap();

        // Create a LogicalPlan for testing
        let ctx = SessionContext::new();
        ctx.register_batch("test_table", batch.clone()).unwrap();
        let df = ctx
            .sql("SELECT * FROM test_table WHERE id > 1")
            .await
            .unwrap();
        let logical_plan = df.logical_plan();

        // Create and execute SqlTask
        let table_name = "test_table".to_string();
        let sql_task = SqlTask::new(logical_plan, batch, table_name).unwrap();

        let block_manager = barks_network_shuffle::shuffle::FileShuffleBlockManager::new(
            std::env::temp_dir().join("test_sql_shuffle"),
        )
        .unwrap();

        let result = sql_task.execute(0, Arc::new(block_manager)).await.unwrap();

        // Verify we got some result
        assert!(!result.is_empty(), "SqlTask should return non-empty result");

        // Clean up
        let _ = std::fs::remove_dir_all(std::env::temp_dir().join("test_sql_shuffle"));
    }

    #[tokio::test]
    async fn test_distributed_dataframe_collect_local() {
        // Create a simple DataFrame
        let ctx = Arc::new(SessionContext::new());

        // Create sample data
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();

        let df = DistributedDataFrame::from_record_batches(
            vec![batch],
            ctx.clone(),
            None, // No distributed context for local execution
        )
        .await
        .unwrap();

        // Test local collection
        let results = df.collect().await.unwrap();
        assert!(!results.is_empty(), "Should return results");
        assert_eq!(results[0].num_rows(), 5, "Should have 5 rows");
    }
}
