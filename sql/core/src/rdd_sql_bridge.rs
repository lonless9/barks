//! Bridge implementations for RDD-to-SQL conversion
//!
//! This module provides implementations of the `RddToSql` trait for specific RDD types,
//! eliminating the need for downcast_ref in SQL integration.

use crate::columnar::ToRecordBatch;
use crate::datasources::RddTableProvider;
use crate::traits::{RddToSql, SqlError, SqlResult};
use barks_core::rdd::DistributedRdd;
use barks_core::traits::{IsRdd, Partition};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use std::sync::Arc;

/// Computes an RDD partition and converts it to a RecordBatch using the `RddToSql` trait.
pub fn rdd_to_record_batch(
    rdd: Arc<dyn IsRdd>,
    partition: &dyn Partition,
) -> SqlResult<RecordBatch> {
    let rdd_any = rdd.as_any();
    // This function encapsulates the downcasting logic.
    if let Some(rdd_i32) = rdd_any.downcast_ref::<DistributedRdd<i32>>() {
        return rdd_i32.compute_partition_to_record_batch(partition);
    }
    if let Some(rdd_string) = rdd_any.downcast_ref::<DistributedRdd<String>>() {
        return rdd_string.compute_partition_to_record_batch(partition);
    }
    if let Some(rdd_tuple) = rdd_any.downcast_ref::<DistributedRdd<(String, i32)>>() {
        return rdd_tuple.compute_partition_to_record_batch(partition);
    }
    if let Some(rdd_tuple) = rdd_any.downcast_ref::<DistributedRdd<(i32, String)>>() {
        return rdd_tuple.compute_partition_to_record_batch(partition);
    }
    if let Some(rdd_tuple) = rdd_any.downcast_ref::<DistributedRdd<(String, String)>>() {
        return rdd_tuple.compute_partition_to_record_batch(partition);
    }
    Err(SqlError::RddIntegration(
        "Unsupported RDD type for RecordBatch conversion".to_string(),
    ))
}

/// Implementation of RddToSql for DistributedRdd<i32>
impl RddToSql for DistributedRdd<i32> {
    fn get_schema(&self) -> SqlResult<SchemaRef> {
        <i32 as ToRecordBatch>::to_schema()
    }

    fn create_table_provider(&self) -> SqlResult<Arc<dyn TableProvider>> {
        let schema = self.get_schema()?;
        let rdd_arc: Arc<dyn IsRdd> = Arc::new(self.clone());
        Ok(Arc::new(RddTableProvider::new(rdd_arc, schema)))
    }

    fn compute_partition_to_record_batch(
        &self,
        partition: &dyn Partition,
    ) -> SqlResult<RecordBatch> {
        let data = self.compute(partition)?;
        <i32 as ToRecordBatch>::to_record_batch(data)
    }
}

/// Implementation of RddToSql for DistributedRdd<String>
impl RddToSql for DistributedRdd<String> {
    fn get_schema(&self) -> SqlResult<SchemaRef> {
        <String as ToRecordBatch>::to_schema()
    }

    fn create_table_provider(&self) -> SqlResult<Arc<dyn TableProvider>> {
        let schema = self.get_schema()?;
        let rdd_arc: Arc<dyn IsRdd> = Arc::new(self.clone());
        Ok(Arc::new(RddTableProvider::new(rdd_arc, schema)))
    }

    fn compute_partition_to_record_batch(
        &self,
        partition: &dyn Partition,
    ) -> SqlResult<RecordBatch> {
        let data = self.compute(partition)?;
        <String as ToRecordBatch>::to_record_batch(data)
    }
}

/// Implementation of RddToSql for DistributedRdd<(String, i32)>
impl RddToSql for DistributedRdd<(String, i32)> {
    fn get_schema(&self) -> SqlResult<SchemaRef> {
        <(String, i32) as ToRecordBatch>::to_schema()
    }

    fn create_table_provider(&self) -> SqlResult<Arc<dyn TableProvider>> {
        let schema = self.get_schema()?;
        let rdd_arc: Arc<dyn IsRdd> = Arc::new(self.clone());
        Ok(Arc::new(RddTableProvider::new(rdd_arc, schema)))
    }

    fn compute_partition_to_record_batch(
        &self,
        partition: &dyn Partition,
    ) -> SqlResult<RecordBatch> {
        let data = self.compute(partition)?;
        <(String, i32) as ToRecordBatch>::to_record_batch(data)
    }
}

/// Implementation of RddToSql for DistributedRdd<(i32, String)>
impl RddToSql for DistributedRdd<(i32, String)> {
    fn get_schema(&self) -> SqlResult<SchemaRef> {
        <(i32, String) as ToRecordBatch>::to_schema()
    }

    fn create_table_provider(&self) -> SqlResult<Arc<dyn TableProvider>> {
        let schema = self.get_schema()?;
        let rdd_arc: Arc<dyn IsRdd> = Arc::new(self.clone());
        Ok(Arc::new(RddTableProvider::new(rdd_arc, schema)))
    }

    fn compute_partition_to_record_batch(
        &self,
        partition: &dyn Partition,
    ) -> SqlResult<RecordBatch> {
        let data = self.compute(partition)?;
        <(i32, String) as ToRecordBatch>::to_record_batch(data)
    }
}

/// Implementation of RddToSql for DistributedRdd<(String, String)>
impl RddToSql for DistributedRdd<(String, String)> {
    fn get_schema(&self) -> SqlResult<SchemaRef> {
        <(String, String) as ToRecordBatch>::to_schema()
    }

    fn create_table_provider(&self) -> SqlResult<Arc<dyn TableProvider>> {
        let schema = self.get_schema()?;
        let rdd_arc: Arc<dyn IsRdd> = Arc::new(self.clone());
        Ok(Arc::new(RddTableProvider::new(rdd_arc, schema)))
    }

    fn compute_partition_to_record_batch(
        &self,
        partition: &dyn Partition,
    ) -> SqlResult<RecordBatch> {
        let data = self.compute(partition)?;
        <(String, String) as ToRecordBatch>::to_record_batch(data)
    }
}

/// Helper function to register an RDD using the RddToSql trait
/// This eliminates the need for downcast_ref in registration
pub async fn register_rdd_with_trait<T>(
    ctx: &datafusion::execution::context::SessionContext,
    name: &str,
    rdd: &T,
) -> SqlResult<()>
where
    T: RddToSql,
{
    let provider = rdd.create_table_provider()?;
    ctx.register_table(name, provider).map_err(|e| {
        crate::traits::SqlError::DataSource(format!("Failed to register table: {}", e))
    })?;
    Ok(())
}

/// Type-safe RDD registration function that uses TypeId to determine the concrete type
/// This is a more practical approach that maintains type safety while being extensible
pub async fn register_rdd_type_safe(
    ctx: &datafusion::execution::context::SessionContext,
    name: &str,
    rdd: Arc<dyn IsRdd>,
) -> SqlResult<()> {
    // Get the TypeId of the RDD's item type
    let rdd_any = rdd.as_any();

    // Handle i32 RDDs
    if let Some(rdd_i32) = rdd_any.downcast_ref::<DistributedRdd<i32>>() {
        return register_rdd_with_trait(ctx, name, rdd_i32).await;
    }

    // Handle String RDDs
    if let Some(rdd_string) = rdd_any.downcast_ref::<DistributedRdd<String>>() {
        return register_rdd_with_trait(ctx, name, rdd_string).await;
    }

    // Handle (String, i32) RDDs
    if let Some(rdd_tuple) = rdd_any.downcast_ref::<DistributedRdd<(String, i32)>>() {
        return register_rdd_with_trait(ctx, name, rdd_tuple).await;
    }

    // Handle (i32, String) RDDs
    if let Some(rdd_tuple) = rdd_any.downcast_ref::<DistributedRdd<(i32, String)>>() {
        return register_rdd_with_trait(ctx, name, rdd_tuple).await;
    }

    // Handle (String, String) RDDs
    if let Some(rdd_tuple) = rdd_any.downcast_ref::<DistributedRdd<(String, String)>>() {
        return register_rdd_with_trait(ctx, name, rdd_tuple).await;
    }

    Err(crate::traits::SqlError::RddIntegration(
        "Unsupported RDD type for SQL registration. Supported types: i32, String, (String, i32), (i32, String), (String, String)".to_string()
    ))
}
