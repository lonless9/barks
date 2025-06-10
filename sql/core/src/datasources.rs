//! Data source adapters for bridging RDDs with DataFusion
//!
//! This module provides adapters that allow RDDs to be used as data sources
//! in DataFusion SQL queries, enabling seamless integration between the
//! distributed RDD model and SQL operations.

use crate::columnar::ToRecordBatch;
use crate::rdd_exec::RddExec;
use crate::traits::{SqlDataSource, SqlError, SqlResult};
use async_trait::async_trait;
use barks_core::traits::IsRdd;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::{ExecutionPlan, Statistics};
use std::any::Any;
use std::sync::Arc;

/// Trait for creating SQL tasks from RDDs
pub trait SqlTaskCreator: Send + Sync {
    fn num_partitions(&self) -> usize;
    fn create_sql_task(
        &self,
        partition_index: usize,
        sql_query: String,
        table_name: String,
    ) -> SqlResult<Box<dyn barks_core::distributed::task::Task>>;
}

/// Wrapper that implements SqlTaskCreator for any RDD
pub struct RddSqlTaskCreator {
    rdd: Arc<dyn IsRdd>,
}

impl SqlTaskCreator for RddSqlTaskCreator {
    fn num_partitions(&self) -> usize {
        self.rdd.num_partitions()
    }

    fn create_sql_task(
        &self,
        _partition_index: usize,
        _sql_query: String,
        _table_name: String,
    ) -> SqlResult<Box<dyn barks_core::distributed::task::Task>> {
        // This is a simplified implementation that creates a basic SQL task
        // In a real implementation, we would need to handle different RDD types
        // and convert their data to RecordBatch format

        // For now, we'll create a placeholder task that returns an error
        // This needs to be implemented properly based on the RDD's data type
        Err(SqlError::QueryExecution(
            "SQL task creation from RDD not yet fully implemented".to_string(),
        ))
    }
}

/// A table provider that wraps any RDD for use in DataFusion.
/// It uses downcasting to handle different RDD item types.
#[derive(Debug)]
pub struct RddTableProvider {
    rdd: Arc<dyn IsRdd>,
    schema: SchemaRef,
}

impl RddTableProvider {
    /// Create a new RDD table provider
    pub fn new(rdd: Arc<dyn IsRdd>, schema: SchemaRef) -> Self {
        Self { rdd, schema }
    }

    /// Get the underlying RDD
    pub fn get_rdd(&self) -> Arc<dyn IsRdd> {
        self.rdd.clone()
    }

    /// Get a SQL task creator for this RDD
    pub fn get_sql_task_creator(&self) -> Arc<dyn SqlTaskCreator> {
        // Create a wrapper that implements SqlTaskCreator
        Arc::new(RddSqlTaskCreator {
            rdd: self.rdd.clone(),
        })
    }
}

#[async_trait]
impl TableProvider for RddTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let exec_plan = RddExec::new(self.rdd.clone(), self.schema.clone(), projection.cloned());
        Ok(Arc::new(exec_plan))
    }

    fn supports_filters_pushdown(
        &self,
        _filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            _filters.len()
        ])
    }
}

/// Data source adapter for external files
pub struct FileDataSource {
    #[allow(dead_code)]
    path: String,
    format: FileFormat,
    schema: Option<SchemaRef>,
}

#[derive(Debug, Clone)]
pub enum FileFormat {
    Csv,
    Parquet,
    Json,
}

impl FileDataSource {
    pub fn new(path: String, format: FileFormat) -> Self {
        Self {
            path,
            format,
            schema: None,
        }
    }

    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }
}

#[async_trait]
impl SqlDataSource for FileDataSource {
    async fn schema(&self) -> SqlResult<SchemaRef> {
        if let Some(schema) = &self.schema {
            return Ok(schema.clone());
        }

        // Infer schema from file
        match self.format {
            FileFormat::Csv => {
                // For CSV, we'd need to read the first few rows to infer schema
                // This is a placeholder implementation
                let fields = vec![
                    Field::new("column1", DataType::Utf8, true),
                    Field::new("column2", DataType::Int32, true),
                ];
                Ok(Arc::new(Schema::new(fields)))
            }
            FileFormat::Parquet => {
                // For Parquet, we can read the schema from the file metadata
                Err(SqlError::DataSource(
                    "Parquet schema inference not implemented".to_string(),
                ))
            }
            FileFormat::Json => {
                // For JSON, we'd need to sample the file to infer schema
                Err(SqlError::DataSource(
                    "JSON schema inference not implemented".to_string(),
                ))
            }
        }
    }

    async fn scan(&self, _projection: Option<&[usize]>) -> SqlResult<Vec<RecordBatch>> {
        // This would implement actual file reading
        // For now, return an error
        Err(SqlError::DataSource(format!(
            "File scanning not implemented for format: {:?}",
            self.format
        )))
    }

    async fn statistics(&self) -> SqlResult<Statistics> {
        // Return default statistics for now
        let schema = self.schema().await?;
        Ok(Statistics::new_unknown(&schema))
    }
}

/// Utility functions for data source operations
pub mod utils {
    use super::*;
    use datafusion::prelude::{CsvReadOptions, ParquetReadOptions};

    /// Register an i32 RDD as a table in DataFusion
    pub async fn register_i32_rdd(
        ctx: &datafusion::execution::context::SessionContext,
        name: &str,
        rdd: Arc<dyn IsRdd>,
    ) -> SqlResult<()> {
        let schema = <i32 as ToRecordBatch>::to_schema()?;
        let provider = Arc::new(RddTableProvider::new(rdd, schema));
        ctx.register_table(name, provider)
            .map_err(|e| SqlError::DataSource(format!("Failed to register table: {}", e)))?;
        Ok(())
    }

    /// Register a String RDD as a table in DataFusion
    pub async fn register_string_rdd(
        ctx: &datafusion::execution::context::SessionContext,
        name: &str,
        rdd: Arc<dyn IsRdd>,
    ) -> SqlResult<()> {
        let schema = <String as ToRecordBatch>::to_schema()?;
        let provider = Arc::new(RddTableProvider::new(rdd, schema));
        ctx.register_table(name, provider)
            .map_err(|e| SqlError::DataSource(format!("Failed to register table: {}", e)))?;
        Ok(())
    }

    /// Register a CSV file as a table in DataFusion
    pub async fn register_csv_table(
        ctx: &datafusion::execution::context::SessionContext,
        name: &str,
        path: &str,
    ) -> SqlResult<()> {
        ctx.register_csv(name, path, CsvReadOptions::new())
            .await
            .map_err(|e| {
                SqlError::DataSource(format!("Failed to register CSV table '{}': {}", name, e))
            })
    }

    /// Register a CSV file as a table with custom options
    pub async fn register_csv_table_with_options(
        ctx: &datafusion::execution::context::SessionContext,
        name: &str,
        path: &str,
        options: CsvReadOptions<'_>,
    ) -> SqlResult<()> {
        ctx.register_csv(name, path, options).await.map_err(|e| {
            SqlError::DataSource(format!("Failed to register CSV table '{}': {}", name, e))
        })
    }

    /// Register a Parquet file as a table in DataFusion
    pub async fn register_parquet_table(
        ctx: &datafusion::execution::context::SessionContext,
        name: &str,
        path: &str,
    ) -> SqlResult<()> {
        ctx.register_parquet(name, path, ParquetReadOptions::default())
            .await
            .map_err(|e| {
                SqlError::DataSource(format!(
                    "Failed to register Parquet table '{}': {}",
                    name, e
                ))
            })
    }

    /// Register a Parquet file as a table with custom options
    pub async fn register_parquet_table_with_options(
        ctx: &datafusion::execution::context::SessionContext,
        name: &str,
        path: &str,
        options: ParquetReadOptions<'_>,
    ) -> SqlResult<()> {
        ctx.register_parquet(name, path, options)
            .await
            .map_err(|e| {
                SqlError::DataSource(format!(
                    "Failed to register Parquet table '{}': {}",
                    name, e
                ))
            })
    }

    /// Create a simple schema for testing
    pub fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]))
    }

    /// Create CSV read options with common defaults
    pub fn csv_options() -> CsvReadOptions<'static> {
        CsvReadOptions::new().has_header(true).delimiter(b',')
    }

    /// Create CSV read options without headers
    pub fn csv_options_no_header() -> CsvReadOptions<'static> {
        CsvReadOptions::new().has_header(false).delimiter(b',')
    }

    /// Create Parquet read options with common defaults
    pub fn parquet_options() -> ParquetReadOptions<'static> {
        ParquetReadOptions::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use barks_core::rdd::DistributedRdd;
    use barks_core::traits::RddBase;
    use datafusion::execution::context::SessionContext;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_file_data_source_creation() {
        let data_source = FileDataSource::new("test.csv".to_string(), FileFormat::Csv);
        assert_eq!(data_source.path, "test.csv");
        assert!(matches!(data_source.format, FileFormat::Csv));
    }

    #[test]
    fn test_schema_creation() {
        let schema = utils::create_test_schema();
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(schema.field(2).name(), "value");
    }

    #[test]
    fn test_csv_options_creation() {
        let _options = utils::csv_options();
        // Basic smoke test - ensure options are created without panicking
        // We can't test Debug since CsvReadOptions doesn't implement it

        let _options_no_header = utils::csv_options_no_header();
        // Just ensure they can be created successfully
    }

    #[test]
    fn test_parquet_options_creation() {
        let _options = utils::parquet_options();
        // Basic smoke test - ensure options are created without panicking
        // We can't test Debug since ParquetReadOptions doesn't implement it
    }

    #[tokio::test]
    async fn test_csv_table_registration() -> SqlResult<()> {
        // Create a temporary CSV file
        let mut temp_file = NamedTempFile::new()
            .map_err(|e| SqlError::DataSource(format!("Failed to create temp file: {}", e)))?;
        writeln!(temp_file, "id,name,value")
            .map_err(|e| SqlError::DataSource(format!("Failed to write CSV header: {}", e)))?;
        writeln!(temp_file, "1,Alice,10.5")
            .map_err(|e| SqlError::DataSource(format!("Failed to write CSV data: {}", e)))?;
        writeln!(temp_file, "2,Bob,20.3")
            .map_err(|e| SqlError::DataSource(format!("Failed to write CSV data: {}", e)))?;
        writeln!(temp_file, "3,Charlie,15.7")
            .map_err(|e| SqlError::DataSource(format!("Failed to write CSV data: {}", e)))?;

        let ctx = SessionContext::new();
        let file_path = temp_file
            .path()
            .to_str()
            .ok_or_else(|| SqlError::DataSource("Failed to get temp file path".to_string()))?;

        // Test basic CSV registration
        utils::register_csv_table(&ctx, "test_csv", file_path).await?;

        // Verify the table was registered by executing a simple query
        let df = ctx
            .sql("SELECT COUNT(*) FROM test_csv")
            .await
            .map_err(SqlError::from)?;
        let results = df.collect().await.map_err(SqlError::from)?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_csv_table_registration_with_options() -> SqlResult<()> {
        // Create a temporary CSV file without headers
        let mut temp_file = NamedTempFile::new()
            .map_err(|e| SqlError::DataSource(format!("Failed to create temp file: {}", e)))?;
        writeln!(temp_file, "1,Alice,10.5")
            .map_err(|e| SqlError::DataSource(format!("Failed to write CSV data: {}", e)))?;
        writeln!(temp_file, "2,Bob,20.3")
            .map_err(|e| SqlError::DataSource(format!("Failed to write CSV data: {}", e)))?;

        let ctx = SessionContext::new();
        let file_path = temp_file
            .path()
            .to_str()
            .ok_or_else(|| SqlError::DataSource("Failed to get temp file path".to_string()))?;

        // Test CSV registration with custom options (no header)
        let options = utils::csv_options_no_header();
        utils::register_csv_table_with_options(&ctx, "test_csv_no_header", file_path, options)
            .await?;

        // Verify the table was registered
        let df = ctx
            .sql("SELECT COUNT(*) FROM test_csv_no_header")
            .await
            .map_err(SqlError::from)?;
        let results = df.collect().await.map_err(SqlError::from)?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_i32_rdd_table_provider() {
        let data = vec![1, 2, 3, 4, 5];
        let rdd = Arc::new(DistributedRdd::from_vec(data));
        let schema = <i32 as ToRecordBatch>::to_schema().unwrap();
        let provider = RddTableProvider::new(rdd.as_is_rdd(), schema.clone());

        let provider_schema = provider.schema();
        assert_eq!(provider_schema.fields().len(), 1);
        assert_eq!(provider_schema.field(0).name(), "value");
        assert_eq!(provider_schema.field(0).data_type(), &DataType::Int32);
    }
}
