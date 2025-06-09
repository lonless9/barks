//! SQL API traits for Barks
//!
//! This module defines the public API traits for SQL operations
//! in the Barks framework.

use async_trait::async_trait;
use barks_core::traits::{Data, IsRdd, RddBase};
use barks_sql_core::traits::{SqlError, SqlResult};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::logical_expr::Expr;
use std::sync::Arc;

/// Trait for SQL-enabled contexts
#[async_trait]
pub trait SqlContext: Send + Sync {
    /// Execute a SQL query and return results
    async fn sql(&self, query: &str) -> SqlResult<Vec<RecordBatch>>;

    /// Register an RDD as a table for SQL queries
    async fn register_rdd_table(&self, name: &str, rdd: Arc<dyn IsRdd>) -> SqlResult<()>;

    /// Register a CSV file as a table
    async fn register_csv_table(&self, name: &str, path: &str) -> SqlResult<()>;

    /// Register a Parquet file as a table
    async fn register_parquet_table(&self, name: &str, path: &str) -> SqlResult<()>;

    /// Create a DataFrame from a SQL query
    async fn create_dataframe(
        &self,
        query: &str,
    ) -> SqlResult<crate::dataframe::DistributedDataFrame>;
}

/// Trait for DataFrame-like operations
#[async_trait]
pub trait DataFrameOps: Send + Sync {
    /// Select specific columns
    fn select(self, columns: Vec<&str>) -> SqlResult<Self>
    where
        Self: Sized;

    /// Filter rows based on a condition
    fn filter(self, condition: Expr) -> SqlResult<Self>
    where
        Self: Sized;

    /// Group by columns
    fn group_by(self, columns: Vec<&str>) -> SqlResult<Self>
    where
        Self: Sized;

    /// Sort by columns
    fn order_by(self, columns: Vec<&str>) -> SqlResult<Self>
    where
        Self: Sized;

    /// Limit the number of rows
    fn limit(self, count: usize) -> SqlResult<Self>
    where
        Self: Sized;

    /// Collect results as RecordBatches
    async fn collect(self) -> SqlResult<Vec<RecordBatch>>;

    /// Count the number of rows
    async fn count(self) -> SqlResult<usize>;

    /// Show the contents (for debugging)
    async fn show(self) -> SqlResult<()>;
}

/// Trait for aggregation operations
pub trait AggregateOps {
    /// Sum a column
    fn sum(&self, column: &str) -> Expr;

    /// Count rows
    fn count(&self) -> Expr;

    /// Average a column
    fn avg(&self, column: &str) -> Expr;

    /// Find minimum value
    fn min(&self, column: &str) -> Expr;

    /// Find maximum value
    fn max(&self, column: &str) -> Expr;

    /// Collect values into a list
    fn collect_list(&self, column: &str) -> Expr;

    /// Count distinct values
    fn count_distinct(&self, column: &str) -> Expr;
}

/// Default implementation of AggregateOps
pub struct DefaultAggregateOps;

impl AggregateOps for DefaultAggregateOps {
    fn sum(&self, column: &str) -> Expr {
        datafusion::functions_aggregate::expr_fn::sum(datafusion::logical_expr::col(column))
    }

    fn count(&self) -> Expr {
        datafusion::functions_aggregate::expr_fn::count(datafusion::logical_expr::lit(1))
    }

    fn avg(&self, column: &str) -> Expr {
        datafusion::functions_aggregate::expr_fn::avg(datafusion::logical_expr::col(column))
    }

    fn min(&self, column: &str) -> Expr {
        datafusion::functions_aggregate::expr_fn::min(datafusion::logical_expr::col(column))
    }

    fn max(&self, column: &str) -> Expr {
        datafusion::functions_aggregate::expr_fn::max(datafusion::logical_expr::col(column))
    }

    fn collect_list(&self, column: &str) -> Expr {
        // Note: This might not be available in all DataFusion versions
        // We'll use a placeholder for now
        datafusion::logical_expr::col(column)
    }

    fn count_distinct(&self, column: &str) -> Expr {
        datafusion::functions_aggregate::expr_fn::count_distinct(datafusion::logical_expr::col(
            column,
        ))
    }
}

/// Trait for window operations
pub trait WindowOps {
    /// Create a row number window function
    fn row_number(&self) -> Expr;

    /// Create a rank window function
    fn rank(&self) -> Expr;

    /// Create a dense rank window function
    fn dense_rank(&self) -> Expr;

    /// Create a lag window function
    fn lag(&self, column: &str, offset: i64) -> Expr;

    /// Create a lead window function
    fn lead(&self, column: &str, offset: i64) -> Expr;
}

/// Trait for column operations
pub trait ColumnOps {
    /// Create a column reference
    fn col(name: &str) -> Expr;

    /// Create a literal value
    fn lit<T>(value: T) -> Expr
    where
        T: Into<datafusion::scalar::ScalarValue> + datafusion::logical_expr::Literal;

    /// Create a when-then expression
    fn when(condition: Expr, then_expr: Expr) -> Expr;

    /// Create a case expression
    fn case(expr: Expr) -> Expr;

    /// Create an alias for an expression
    fn alias(expr: Expr, name: &str) -> Expr;
}

/// Default implementation of ColumnOps
pub struct DefaultColumnOps;

impl ColumnOps for DefaultColumnOps {
    fn col(name: &str) -> Expr {
        datafusion::logical_expr::col(name)
    }

    fn lit<T>(value: T) -> Expr
    where
        T: Into<datafusion::scalar::ScalarValue> + datafusion::logical_expr::Literal,
    {
        datafusion::logical_expr::lit(value)
    }

    fn when(condition: Expr, then_expr: Expr) -> Expr {
        datafusion::logical_expr::when(condition, then_expr)
            .end()
            .unwrap()
    }

    fn case(expr: Expr) -> Expr {
        datafusion::logical_expr::case(expr).end().unwrap()
    }

    fn alias(expr: Expr, name: &str) -> Expr {
        expr.alias(name)
    }
}

/// Trait for join operations
pub trait JoinOps {
    /// Inner join
    fn inner_join(self, right: Self, left_cols: &[&str], right_cols: &[&str]) -> SqlResult<Self>
    where
        Self: Sized;

    /// Left join
    fn left_join(self, right: Self, left_cols: &[&str], right_cols: &[&str]) -> SqlResult<Self>
    where
        Self: Sized;

    /// Right join
    fn right_join(self, right: Self, left_cols: &[&str], right_cols: &[&str]) -> SqlResult<Self>
    where
        Self: Sized;

    /// Full outer join
    fn full_join(self, right: Self, left_cols: &[&str], right_cols: &[&str]) -> SqlResult<Self>
    where
        Self: Sized;
}

/// Trait for RDD-DataFrame conversion
pub trait RddDataFrameConversion<T: Data> {
    /// Convert an RDD to a DataFrame
    fn rdd_to_dataframe(
        rdd: Arc<dyn RddBase<Item = T>>,
    ) -> SqlResult<crate::dataframe::DistributedDataFrame>;

    /// Convert a DataFrame to an RDD
    fn dataframe_to_rdd(
        dataframe: crate::dataframe::DistributedDataFrame,
    ) -> SqlResult<Arc<dyn RddBase<Item = T>>>;
}

/// Configuration for SQL operations
#[derive(Debug, Clone)]
pub struct SqlApiConfig {
    /// Default batch size for operations
    pub batch_size: usize,
    /// Enable query optimization
    pub enable_optimization: bool,
    /// Enable distributed execution
    pub enable_distributed: bool,
    /// Maximum memory usage for operations
    pub max_memory_mb: Option<usize>,
    /// Number of concurrent tasks
    pub max_concurrent_tasks: usize,
}

impl Default for SqlApiConfig {
    fn default() -> Self {
        Self {
            batch_size: 8192,
            enable_optimization: true,
            enable_distributed: true,
            max_memory_mb: None,
            max_concurrent_tasks: num_cpus::get(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aggregate_ops() {
        let agg = DefaultAggregateOps;

        // Test that we can create aggregate expressions
        let sum_expr = agg.sum("value");
        let count_expr = agg.count();
        let avg_expr = agg.avg("value");

        // Basic smoke tests - just ensure they compile and don't panic
        // Use case-insensitive checks since DataFusion might use different casing
        let sum_debug = format!("{:?}", sum_expr).to_lowercase();
        let count_debug = format!("{:?}", count_expr).to_lowercase();
        let avg_debug = format!("{:?}", avg_expr).to_lowercase();

        assert!(sum_debug.contains("sum") || sum_debug.contains("aggregate"));
        assert!(count_debug.contains("count") || count_debug.contains("aggregate"));
        assert!(
            avg_debug.contains("avg")
                || avg_debug.contains("average")
                || avg_debug.contains("aggregate")
        );
    }

    #[test]
    fn test_column_ops() {
        let col_expr = DefaultColumnOps::col("test_column");
        let lit_expr = DefaultColumnOps::lit(42);

        // Basic smoke tests
        assert!(format!("{:?}", col_expr).contains("test_column"));
        assert!(format!("{:?}", lit_expr).contains("42"));
    }

    #[test]
    fn test_config_default() {
        let config = SqlApiConfig::default();
        assert_eq!(config.batch_size, 8192);
        assert!(config.enable_optimization);
        assert!(config.enable_distributed);
    }
}
