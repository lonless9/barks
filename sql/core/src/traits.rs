//! SQL Core traits for Barks
//!
//! This module defines the fundamental abstractions for SQL operations
//! in the Barks framework, integrating DataFusion with distributed execution.

use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;
use thiserror::Error;

/// Error types for SQL operations
#[derive(Error, Debug, Clone)]
pub enum SqlError {
    #[error("DataFusion error: {0}")]
    DataFusion(String),

    #[error("Query planning error: {0}")]
    QueryPlanning(String),

    #[error("Query execution error: {0}")]
    QueryExecution(String),

    #[error("Schema error: {0}")]
    Schema(String),

    #[error("Data source error: {0}")]
    DataSource(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Distributed execution error: {0}")]
    DistributedExecution(String),

    #[error("RDD integration error: {0}")]
    RddIntegration(String),

    #[error("Context error: {0}")]
    Context(String),
}

impl From<DataFusionError> for SqlError {
    fn from(err: DataFusionError) -> Self {
        SqlError::DataFusion(err.to_string())
    }
}

impl From<datafusion::arrow::error::ArrowError> for SqlError {
    fn from(err: datafusion::arrow::error::ArrowError) -> Self {
        SqlError::DataFusion(err.to_string())
    }
}

impl From<SqlError> for DataFusionError {
    fn from(err: SqlError) -> Self {
        match err {
            SqlError::DataFusion(msg) => DataFusionError::External(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                msg,
            ))),
            SqlError::QueryPlanning(msg) => DataFusionError::Plan(msg),
            SqlError::QueryExecution(msg) => DataFusionError::Execution(msg),
            SqlError::Schema(msg) => DataFusionError::External(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Schema error: {}", msg),
            ))),
            SqlError::DataSource(msg) => DataFusionError::External(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                msg,
            ))),
            SqlError::Serialization(msg) => DataFusionError::External(Box::new(
                std::io::Error::new(std::io::ErrorKind::Other, msg),
            )),
            SqlError::DistributedExecution(msg) => DataFusionError::External(Box::new(
                std::io::Error::new(std::io::ErrorKind::Other, msg),
            )),
            SqlError::RddIntegration(msg) => DataFusionError::External(Box::new(
                std::io::Error::new(std::io::ErrorKind::Other, msg),
            )),
            SqlError::Context(msg) => DataFusionError::Context(
                msg,
                Box::new(DataFusionError::IoError(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Context error",
                ))),
            ),
        }
    }
}

impl From<barks_core::traits::RddError> for SqlError {
    fn from(err: barks_core::traits::RddError) -> Self {
        match err {
            barks_core::traits::RddError::ComputationError(msg) => SqlError::QueryExecution(msg),
            barks_core::traits::RddError::SerializationError(msg) => SqlError::Serialization(msg),
            barks_core::traits::RddError::InvalidPartition(idx) => {
                SqlError::RddIntegration(format!("Invalid partition: {}", idx))
            }
            barks_core::traits::RddError::ContextError(msg) => SqlError::Context(msg),
            barks_core::traits::RddError::ShuffleError(msg) => {
                SqlError::DistributedExecution(format!("Shuffle error: {}", msg))
            }
            barks_core::traits::RddError::TaskCreationError(msg) => {
                SqlError::DistributedExecution(format!("Task creation error: {}", msg))
            }
            barks_core::traits::RddError::CheckpointError(msg) => {
                SqlError::RddIntegration(format!("Checkpoint error: {}", msg))
            }
            barks_core::traits::RddError::NotImplemented(msg) => {
                SqlError::QueryExecution(format!("Not implemented: {}", msg))
            }
        }
    }
}

impl From<SqlError> for barks_core::traits::RddError {
    fn from(err: SqlError) -> Self {
        match err {
            SqlError::DataFusion(msg) => barks_core::traits::RddError::ComputationError(msg),
            SqlError::QueryPlanning(msg) => barks_core::traits::RddError::ComputationError(msg),
            SqlError::QueryExecution(msg) => barks_core::traits::RddError::ComputationError(msg),
            SqlError::Schema(msg) => barks_core::traits::RddError::ComputationError(msg),
            SqlError::DataSource(msg) => barks_core::traits::RddError::ComputationError(msg),
            SqlError::Serialization(msg) => barks_core::traits::RddError::SerializationError(msg),
            SqlError::DistributedExecution(msg) => {
                barks_core::traits::RddError::ComputationError(msg)
            }
            SqlError::RddIntegration(msg) => barks_core::traits::RddError::ComputationError(msg),
            SqlError::Context(msg) => barks_core::traits::RddError::ContextError(msg),
        }
    }
}

/// Result type for SQL operations
pub type SqlResult<T> = Result<T, SqlError>;

/// Trait for SQL query engines
#[async_trait]
pub trait SqlQueryEngine: Send + Sync {
    /// Execute a SQL query and return results as RecordBatches
    async fn execute_sql(&self, sql: &str) -> SqlResult<Vec<RecordBatch>>;

    /// Parse SQL into a logical plan
    async fn parse_sql(&self, sql: &str) -> SqlResult<LogicalPlan>;

    /// Optimize a logical plan
    async fn optimize_plan(&self, plan: LogicalPlan) -> SqlResult<LogicalPlan>;

    /// Create a physical plan from a logical plan
    async fn create_physical_plan(&self, plan: LogicalPlan) -> SqlResult<Arc<dyn ExecutionPlan>>;

    /// Execute a physical plan
    async fn execute_physical_plan(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> SqlResult<Vec<RecordBatch>>;
}

/// Trait for data sources that can be used in SQL queries
#[async_trait]
pub trait SqlDataSource: Send + Sync {
    /// Get the schema of this data source
    async fn schema(&self) -> SqlResult<datafusion::arrow::datatypes::SchemaRef>;

    /// Scan the data source and return record batches
    async fn scan(&self, projection: Option<&[usize]>) -> SqlResult<Vec<RecordBatch>>;

    /// Get statistics about this data source
    async fn statistics(&self) -> SqlResult<datafusion::physical_plan::Statistics>;
}

/// Trait for converting between RDDs and DataFusion data structures
pub trait RddDataFusionBridge<T> {
    /// Convert an RDD to RecordBatches
    fn rdd_to_record_batches(
        rdd: Arc<dyn barks_core::traits::RddBase<Item = T>>,
    ) -> SqlResult<Vec<RecordBatch>>;

    /// Convert RecordBatches to an RDD
    fn record_batches_to_rdd(
        batches: Vec<RecordBatch>,
    ) -> SqlResult<Arc<dyn barks_core::traits::RddBase<Item = T>>>;
}

/// Trait for RDD types that can be converted to SQL tables
/// This eliminates the need for downcast_ref in SQL integration
pub trait RddToSql: Send + Sync {
    /// Get the Arrow schema for this RDD type
    fn get_schema(&self) -> SqlResult<datafusion::arrow::datatypes::SchemaRef>;

    /// Create a table provider for this RDD
    fn create_table_provider(&self) -> SqlResult<Arc<dyn datafusion::datasource::TableProvider>>;

    /// Compute partition data and convert to RecordBatch
    fn compute_partition_to_record_batch(
        &self,
        partition: &dyn barks_core::traits::Partition,
    ) -> SqlResult<RecordBatch>;
}

/// Trait for distributed SQL execution
#[async_trait]
pub trait DistributedSqlExecutor: Send + Sync {
    /// Execute a SQL query in a distributed manner
    async fn execute_distributed_sql(
        &self,
        sql: &str,
        context: Arc<barks_core::context::DistributedContext>,
    ) -> SqlResult<Vec<RecordBatch>>;

    /// Split a logical plan into stages for distributed execution
    async fn plan_distributed_execution(
        &self,
        plan: LogicalPlan,
    ) -> SqlResult<Vec<DistributedSqlStage>>;
}

/// Represents a stage in distributed SQL execution
#[derive(Debug, Clone)]
pub struct DistributedSqlStage {
    pub stage_id: usize,
    pub plan: LogicalPlan,
    pub dependencies: Vec<usize>,
    pub shuffle_required: bool,
}

/// Configuration for SQL execution
#[derive(Debug, Clone)]
pub struct SqlConfig {
    pub batch_size: usize,
    pub memory_limit: Option<usize>,
    pub enable_optimization: bool,
    pub enable_distributed_execution: bool,
    pub max_concurrent_tasks: usize,
}

impl Default for SqlConfig {
    fn default() -> Self {
        Self {
            batch_size: 8192,
            memory_limit: None,
            enable_optimization: true,
            enable_distributed_execution: true,
            max_concurrent_tasks: num_cpus::get(),
        }
    }
}

/// Trait for SQL session management
#[async_trait]
pub trait SqlSession: Send + Sync {
    /// Register a table in the session
    async fn register_table(
        &self,
        name: &str,
        data_source: Arc<dyn SqlDataSource>,
    ) -> SqlResult<()>;

    /// Execute SQL in this session
    async fn execute_sql(&self, sql: &str) -> SqlResult<Vec<RecordBatch>>;

    /// Get session configuration
    fn config(&self) -> &SqlConfig;

    /// Set session configuration
    fn set_config(&mut self, config: SqlConfig);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_error_conversion() {
        let sql_err = SqlError::QueryPlanning("test error".to_string());
        let rdd_err: barks_core::traits::RddError = sql_err.into();

        match rdd_err {
            barks_core::traits::RddError::ComputationError(msg) => {
                assert_eq!(msg, "test error");
            }
            _ => panic!("Expected ComputationError"),
        }
    }

    #[test]
    fn test_sql_config_default() {
        let config = SqlConfig::default();
        assert_eq!(config.batch_size, 8192);
        assert!(config.enable_optimization);
        assert!(config.enable_distributed_execution);
    }
}
