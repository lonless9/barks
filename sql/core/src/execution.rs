//! SQL execution engine implementation using DataFusion
//!
//! This module provides the core SQL execution capabilities by integrating
//! DataFusion's query engine with Barks' distributed execution model.

use crate::traits::{SqlConfig, SqlError, SqlQueryEngine, SqlResult, SqlSession};
use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::*;
use std::sync::Arc;

/// DataFusion-based SQL query engine
pub struct DataFusionQueryEngine {
    context: SessionContext,
    config: SqlConfig,
}

impl DataFusionQueryEngine {
    /// Create a new DataFusion query engine
    pub fn new(config: SqlConfig) -> Self {
        let ctx_config = datafusion::execution::config::SessionConfig::new()
            .with_batch_size(config.batch_size)
            .with_target_partitions(config.max_concurrent_tasks);

        // Note: Memory limit configuration may vary by DataFusion version
        // For now, we'll skip this configuration

        let context = SessionContext::new_with_config(ctx_config);

        Self { context, config }
    }

    /// Get the underlying DataFusion session context
    pub fn session_context(&self) -> &SessionContext {
        &self.context
    }

    /// Register a CSV file as a table
    pub async fn register_csv(&self, name: &str, path: &str) -> SqlResult<()> {
        self.context
            .register_csv(name, path, CsvReadOptions::new())
            .await
            .map_err(SqlError::from)
    }

    /// Register a Parquet file as a table
    pub async fn register_parquet(&self, name: &str, path: &str) -> SqlResult<()> {
        self.context
            .register_parquet(name, path, ParquetReadOptions::default())
            .await
            .map_err(SqlError::from)
    }

    /// Register record batches as a table
    pub async fn register_record_batches(
        &self,
        name: &str,
        batches: Vec<RecordBatch>,
    ) -> SqlResult<()> {
        if batches.is_empty() {
            return Err(SqlError::DataSource(
                "Cannot register empty record batches".to_string(),
            ));
        }

        // Create a single record batch from all batches
        let schema = batches[0].schema();
        let combined_batch = datafusion::arrow::compute::concat_batches(&schema, &batches)
            .map_err(|e| SqlError::DataSource(format!("Failed to combine batches: {}", e)))?;

        self.context
            .register_batch(name, combined_batch)
            .map_err(SqlError::from)?;
        Ok(())
    }

    /// Register an RDD as a table. This function infers the schema and creates
    /// a `TableProvider` that can scan the RDD.
    pub async fn register_rdd<T: crate::columnar::ToRecordBatch + barks_core::traits::Data>(
        &self,
        name: &str,
        rdd: Arc<dyn barks_core::traits::RddBase<Item = T>>,
    ) -> SqlResult<()> {
        let schema = T::to_schema()?;
        let provider = Arc::new(crate::datasources::RddTableProvider::new(
            rdd.as_is_rdd(),
            schema,
        ));
        self.context
            .register_table(name, provider)
            .map_err(SqlError::from)?;
        Ok(())
    }

    /// A generic method to register any RDD by downcasting.
    /// This is less type-safe but more flexible.
    pub async fn register_rdd_any(
        &self,
        name: &str,
        rdd: Arc<dyn barks_core::traits::IsRdd>,
    ) -> SqlResult<()> {
        // Try to downcast and register. Support common types.
        if rdd.as_any().is::<barks_core::rdd::DistributedRdd<i32>>() {
            let schema = <i32 as crate::columnar::ToRecordBatch>::to_schema()?;
            let provider = Arc::new(crate::datasources::RddTableProvider::new(rdd, schema));
            self.context
                .register_table(name, provider)
                .map_err(SqlError::from)?;
            return Ok(());
        }
        if rdd.as_any().is::<barks_core::rdd::DistributedRdd<String>>() {
            let schema = <String as crate::columnar::ToRecordBatch>::to_schema()?;
            let provider = Arc::new(crate::datasources::RddTableProvider::new(rdd, schema));
            self.context
                .register_table(name, provider)
                .map_err(SqlError::from)?;
            return Ok(());
        }
        if rdd
            .as_any()
            .is::<barks_core::rdd::DistributedRdd<(String, i32)>>()
        {
            let schema = <(String, i32) as crate::columnar::ToRecordBatch>::to_schema()?;
            let provider = Arc::new(crate::datasources::RddTableProvider::new(rdd, schema));
            self.context
                .register_table(name, provider)
                .map_err(SqlError::from)?;
            return Ok(());
        }
        Err(SqlError::RddIntegration("Unsupported RDD type".to_string()))
    }
}

#[async_trait]
impl SqlQueryEngine for DataFusionQueryEngine {
    async fn execute_sql(&self, sql: &str) -> SqlResult<Vec<RecordBatch>> {
        let df = self.context.sql(sql).await.map_err(SqlError::from)?;
        df.collect().await.map_err(SqlError::from)
    }

    async fn parse_sql(&self, sql: &str) -> SqlResult<LogicalPlan> {
        let df = self.context.sql(sql).await.map_err(SqlError::from)?;
        Ok(df.logical_plan().clone())
    }

    async fn optimize_plan(&self, plan: LogicalPlan) -> SqlResult<LogicalPlan> {
        if !self.config.enable_optimization {
            return Ok(plan);
        }

        // DataFusion automatically optimizes plans during execution
        // For now, we return the plan as-is since optimization happens internally
        Ok(plan)
    }

    async fn create_physical_plan(&self, plan: LogicalPlan) -> SqlResult<Arc<dyn ExecutionPlan>> {
        self.context
            .state()
            .create_physical_plan(&plan)
            .await
            .map_err(SqlError::from)
    }

    async fn execute_physical_plan(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> SqlResult<Vec<RecordBatch>> {
        datafusion::physical_plan::collect(plan, self.context.task_ctx())
            .await
            .map_err(SqlError::from)
    }
}

/// SQL session implementation
pub struct DataFusionSession {
    engine: Arc<DataFusionQueryEngine>,
    config: SqlConfig,
}

impl DataFusionSession {
    /// Create a new SQL session
    pub fn new(config: SqlConfig) -> Self {
        let engine = Arc::new(DataFusionQueryEngine::new(config.clone()));
        Self { engine, config }
    }

    /// Get the underlying query engine
    pub fn engine(&self) -> Arc<DataFusionQueryEngine> {
        self.engine.clone()
    }
}

#[async_trait]
impl SqlSession for DataFusionSession {
    async fn register_table(
        &self,
        _name: &str,
        _data_source: Arc<dyn crate::traits::SqlDataSource>,
    ) -> SqlResult<()> {
        // For now, we'll implement a basic table provider wrapper
        // This will be expanded to support custom data sources
        Err(SqlError::DataSource(
            "Custom data source registration not yet implemented".to_string(),
        ))
    }

    async fn execute_sql(&self, sql: &str) -> SqlResult<Vec<RecordBatch>> {
        self.engine.execute_sql(sql).await
    }

    fn config(&self) -> &SqlConfig {
        &self.config
    }

    fn set_config(&mut self, config: SqlConfig) {
        self.config = config;
    }
}

/// Utility functions for SQL execution
pub mod utils {
    use super::*;
    use datafusion::arrow::array::ArrayRef;

    /// Convert a vector of values to a RecordBatch
    pub fn values_to_record_batch<T>(values: Vec<T>, _field_name: &str) -> SqlResult<RecordBatch>
    where
        T: Into<ArrayRef> + Clone,
    {
        if values.is_empty() {
            return Err(SqlError::DataSource(
                "Cannot create RecordBatch from empty values".to_string(),
            ));
        }

        // This is a simplified implementation
        // In practice, we'd need proper type conversion based on T
        Err(SqlError::DataSource(
            "Generic value conversion not yet implemented".to_string(),
        ))
    }

    /// Get the number of rows in a set of record batches
    pub fn count_rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|batch| batch.num_rows()).sum()
    }

    /// Combine multiple record batches into one
    pub fn combine_batches(batches: Vec<RecordBatch>) -> SqlResult<RecordBatch> {
        if batches.is_empty() {
            return Err(SqlError::DataSource(
                "Cannot combine empty batches".to_string(),
            ));
        }

        let schema = batches[0].schema();
        datafusion::arrow::compute::concat_batches(&schema, &batches)
            .map_err(|e| SqlError::DataSource(format!("Failed to combine batches: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_datafusion_engine_creation() {
        let config = SqlConfig::default();
        let engine = DataFusionQueryEngine::new(config);

        // Basic smoke test
        assert!(engine.session_context().catalog("datafusion").is_some());
    }

    #[tokio::test]
    async fn test_csv_registration_and_query() -> SqlResult<()> {
        // Create a temporary CSV file
        let mut temp_file = NamedTempFile::new()
            .map_err(|e| SqlError::DataSource(format!("Failed to create temp file: {}", e)))?;

        writeln!(temp_file, "id,name,value")
            .map_err(|e| SqlError::DataSource(format!("Failed to write to temp file: {}", e)))?;
        writeln!(temp_file, "1,Alice,100")
            .map_err(|e| SqlError::DataSource(format!("Failed to write to temp file: {}", e)))?;
        writeln!(temp_file, "2,Bob,200")
            .map_err(|e| SqlError::DataSource(format!("Failed to write to temp file: {}", e)))?;

        let config = SqlConfig::default();
        let engine = DataFusionQueryEngine::new(config);

        // Register the CSV file
        engine
            .register_csv("test_table", temp_file.path().to_str().unwrap())
            .await?;

        // Execute a simple query
        let results = engine
            .execute_sql("SELECT COUNT(*) FROM test_table")
            .await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_session_creation() {
        let config = SqlConfig::default();
        let session = DataFusionSession::new(config);

        // Basic smoke test
        assert!(
            session
                .engine()
                .session_context()
                .catalog("datafusion")
                .is_some()
        );
    }

    #[tokio::test]
    async fn test_rdd_registration() {
        use barks_core::rdd::DistributedRdd;
        use barks_core::traits::RddBase;

        let config = SqlConfig::default();
        let engine = DataFusionQueryEngine::new(config);

        // Create an RDD with some test data
        let data: Vec<i32> = (1..=10).collect();
        let rdd: Arc<dyn RddBase<Item = i32>> = Arc::new(DistributedRdd::from_vec(data));

        // Register the RDD as a table
        let result = engine.register_rdd_any("test_rdd", rdd.as_is_rdd()).await;

        // For now, we expect this to work (the table provider is created successfully)
        // The actual scanning is not implemented yet
        assert!(result.is_ok(), "RDD registration should succeed");
    }

    #[tokio::test]
    async fn test_rdd_sql_execution() {
        use barks_core::rdd::DistributedRdd;
        use barks_core::traits::RddBase;

        let config = SqlConfig::default();
        let engine = DataFusionQueryEngine::new(config);

        // Create an RDD with some test data
        let data: Vec<i32> = (1..=5).collect();
        let rdd: Arc<dyn RddBase<Item = i32>> = Arc::new(DistributedRdd::from_vec(data));

        // Register the RDD as a table
        engine
            .register_rdd_any("numbers", rdd.as_is_rdd())
            .await
            .unwrap();

        // Execute a simple SQL query
        let result = engine.execute_sql("SELECT * FROM numbers").await;

        assert!(result.is_ok(), "SQL execution should succeed");
        let batches = result.unwrap();
        assert!(!batches.is_empty(), "Should return at least one batch");

        // Check that we got the expected number of rows
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 5, "Should have 5 rows total");
    }

    #[tokio::test]
    async fn test_rdd_sql_with_tuples() {
        use barks_core::rdd::DistributedRdd;
        use barks_core::traits::RddBase;

        let config = SqlConfig::default();
        let engine = DataFusionQueryEngine::new(config);

        // Create an RDD with tuple data
        let data: Vec<(String, i32)> = vec![
            ("Alice".to_string(), 25),
            ("Bob".to_string(), 30),
            ("Charlie".to_string(), 35),
        ];
        let rdd: Arc<dyn RddBase<Item = (String, i32)>> = Arc::new(DistributedRdd::from_vec(data));

        // Register the RDD as a table
        engine
            .register_rdd_any("people", rdd.as_is_rdd())
            .await
            .unwrap();

        // Execute a SQL query with filtering
        let result = engine
            .execute_sql("SELECT * FROM people WHERE c1 > 25")
            .await;

        assert!(result.is_ok(), "SQL execution should succeed");
        let batches = result.unwrap();
        assert!(!batches.is_empty(), "Should return at least one batch");

        // Check that we got the expected number of rows (Bob and Charlie)
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2, "Should have 2 rows after filtering");
    }
}
