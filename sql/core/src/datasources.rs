//! Data source adapters for bridging RDDs with DataFusion
//!
//! This module provides adapters that allow RDDs to be used as data sources
//! in DataFusion SQL queries, enabling seamless integration between the
//! distributed RDD model and SQL operations.

use crate::rdd_exec::{RddExec, ToRecordBatchConverter};
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

    /// Register an i32 RDD as a table in DataFusion
    pub async fn register_i32_rdd(
        ctx: &datafusion::execution::context::SessionContext,
        name: &str,
        rdd: Arc<dyn IsRdd>,
    ) -> SqlResult<()> {
        let schema = <i32 as ToRecordBatchConverter>::schema()?;
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
        let schema = <String as ToRecordBatchConverter>::schema()?;
        let provider = Arc::new(RddTableProvider::new(rdd, schema));
        ctx.register_table(name, provider)
            .map_err(|e| SqlError::DataSource(format!("Failed to register table: {}", e)))?;
        Ok(())
    }

    /// Create a simple schema for testing
    pub fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use barks_core::rdd::DistributedRdd;
    use barks_core::traits::RddBase;

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

    #[tokio::test]
    async fn test_i32_rdd_table_provider() {
        let data = vec![1, 2, 3, 4, 5];
        let rdd = Arc::new(DistributedRdd::from_vec(data));
        let schema = <i32 as ToRecordBatchConverter>::schema().unwrap();
        let provider = RddTableProvider::new(rdd.as_is_rdd(), schema.clone());

        let provider_schema = provider.schema();
        assert_eq!(provider_schema.fields().len(), 1);
        assert_eq!(provider_schema.field(0).name(), "value");
        assert_eq!(provider_schema.field(0).data_type(), &DataType::Int32);
    }
}
