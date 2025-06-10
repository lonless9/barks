//! Execution plan for scanning an RDD.
//!
//! This is a simplified implementation that provides basic RDD-to-SQL integration.

use crate::traits::SqlError;
use async_trait::async_trait;
use barks_core::traits::IsRdd;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::stream;
use std::any::Any;
use std::sync::Arc;

/// Type-safe function to compute RDD partition data and convert to RecordBatch
/// This uses the RddToSql trait to eliminate repetitive downcast_ref logic
fn compute_rdd_partition_to_record_batch(
    rdd: Arc<dyn IsRdd>,
    partition: usize,
) -> Result<RecordBatch, SqlError> {
    use crate::traits::RddToSql;
    let rdd_any = rdd.as_any();
    let rdd_partition = barks_core::traits::BasicPartition::new(partition);

    // Handle i32 RDDs
    if let Some(rdd_i32) = rdd_any.downcast_ref::<barks_core::rdd::DistributedRdd<i32>>() {
        return rdd_i32.compute_partition_to_record_batch(&rdd_partition);
    }

    // Handle String RDDs
    if let Some(rdd_string) = rdd_any.downcast_ref::<barks_core::rdd::DistributedRdd<String>>() {
        return rdd_string.compute_partition_to_record_batch(&rdd_partition);
    }

    // Handle (String, i32) RDDs
    if let Some(rdd_tuple) =
        rdd_any.downcast_ref::<barks_core::rdd::DistributedRdd<(String, i32)>>()
    {
        return rdd_tuple.compute_partition_to_record_batch(&rdd_partition);
    }

    // Handle (i32, String) RDDs
    if let Some(rdd_tuple) =
        rdd_any.downcast_ref::<barks_core::rdd::DistributedRdd<(i32, String)>>()
    {
        return rdd_tuple.compute_partition_to_record_batch(&rdd_partition);
    }

    // Handle (String, String) RDDs
    if let Some(rdd_tuple) =
        rdd_any.downcast_ref::<barks_core::rdd::DistributedRdd<(String, String)>>()
    {
        return rdd_tuple.compute_partition_to_record_batch(&rdd_partition);
    }

    Err(SqlError::RddIntegration(
        "Unsupported RDD type for SQL execution. Supported types: i32, String, (String, i32), (i32, String), (String, String)".to_string()
    ))
}

/// The RddExec execution plan reads data from an RDD partition.
#[derive(Debug)]
pub struct RddExec {
    rdd: Arc<dyn IsRdd>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    properties: PlanProperties,
}

impl RddExec {
    pub fn new(rdd: Arc<dyn IsRdd>, schema: SchemaRef, projection: Option<Vec<usize>>) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(rdd.num_partitions()),
            datafusion::physical_plan::ExecutionMode::Bounded,
        );
        Self {
            rdd,
            schema,
            projection,
            properties,
        }
    }
}

#[async_trait]
impl ExecutionPlan for RddExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        if let Some(projection) = &self.projection {
            let projected_fields: Vec<_> = projection
                .iter()
                .map(|i| self.schema.field(*i).clone())
                .collect();
            Arc::new(Schema::new(projected_fields))
        } else {
            self.schema.clone()
        }
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn name(&self) -> &str {
        "RddExec"
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let rdd = self.rdd.clone();
        let projection = self.projection.clone();

        let schema = self.schema();
        let stream = stream::once(async move {
            let result_batch: Result<RecordBatch, SqlError> =
                compute_rdd_partition_to_record_batch(rdd, partition);

            let batch = result_batch.map_err(|e| DataFusionError::External(Box::new(e)))?;

            if let Some(proj) = projection {
                batch
                    .project(&proj)
                    .map_err(|e| DataFusionError::ArrowError(e, None))
            } else {
                Ok(batch)
            }
        });

        let adapted_stream = RecordBatchStreamAdapter::new(schema, stream);
        Ok(Box::pin(adapted_stream))
    }
}

impl DisplayAs for RddExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "RddExec(rdd_id={})", self.rdd.id())
    }
}
