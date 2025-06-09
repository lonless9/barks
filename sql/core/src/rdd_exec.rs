//! Execution plan for scanning an RDD.
//!
//! This is a simplified implementation that provides basic RDD-to-SQL integration.

use crate::columnar::ToArrowArray;
use crate::columnar::conversion;
use crate::traits::SqlError;
use async_trait::async_trait;
use barks_core::traits::{Data, IsRdd};
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
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

/// A trait to convert a Vec of a specific data type into a RecordBatch.
/// This allows `RddExec` to be generic while handling different data layouts.
pub trait ToRecordBatchConverter: Data {
    fn convert(data: Vec<Self>) -> Result<RecordBatch, SqlError>;
    fn schema() -> Result<SchemaRef, SqlError>;
}

impl ToRecordBatchConverter for i32 {
    fn convert(data: Vec<Self>) -> Result<RecordBatch, SqlError> {
        let schema = Self::schema()?;
        let array = <i32 as ToArrowArray>::to_arrow_array(data)?;
        RecordBatch::try_new(schema, vec![array]).map_err(SqlError::from)
    }

    fn schema() -> Result<SchemaRef, SqlError> {
        Ok(Arc::new(Schema::new(vec![Field::new(
            "value",
            <i32 as ToArrowArray>::arrow_data_type(),
            true,
        )])))
    }
}

impl ToRecordBatchConverter for String {
    fn convert(data: Vec<Self>) -> Result<RecordBatch, SqlError> {
        let schema = Self::schema()?;
        let array = <String as ToArrowArray>::to_arrow_array(data)?;
        RecordBatch::try_new(schema, vec![array]).map_err(SqlError::from)
    }

    fn schema() -> Result<SchemaRef, SqlError> {
        Ok(Arc::new(Schema::new(vec![Field::new(
            "value",
            <String as ToArrowArray>::arrow_data_type(),
            true,
        )])))
    }
}

impl ToRecordBatchConverter for (String, i32) {
    fn convert(data: Vec<Self>) -> Result<RecordBatch, SqlError> {
        conversion::tuples_to_record_batch(data, "c0", "c1")
    }

    fn schema() -> Result<SchemaRef, SqlError> {
        Ok(Arc::new(Schema::new(vec![
            Field::new("c0", <String as ToArrowArray>::arrow_data_type(), true),
            Field::new("c1", <i32 as ToArrowArray>::arrow_data_type(), true),
        ])))
    }
}

impl ToRecordBatchConverter for (i32, String) {
    fn convert(data: Vec<Self>) -> Result<RecordBatch, SqlError> {
        conversion::tuples_to_record_batch(data, "c0", "c1")
    }

    fn schema() -> Result<SchemaRef, SqlError> {
        Ok(Arc::new(Schema::new(vec![
            Field::new("c0", <i32 as ToArrowArray>::arrow_data_type(), true),
            Field::new("c1", <String as ToArrowArray>::arrow_data_type(), true),
        ])))
    }
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
            let result_batch: Result<RecordBatch, SqlError> = if let Some(rdd_i32) =
                rdd.as_any()
                    .downcast_ref::<barks_core::rdd::DistributedRdd<i32>>()
            {
                let rdd_partition = barks_core::traits::BasicPartition::new(partition);
                let data = rdd_i32.compute(&rdd_partition).map_err(SqlError::from)?;
                <i32 as ToRecordBatchConverter>::convert(data)
            } else if let Some(rdd_string) = rdd
                .as_any()
                .downcast_ref::<barks_core::rdd::DistributedRdd<String>>()
            {
                let rdd_partition = barks_core::traits::BasicPartition::new(partition);
                let data = rdd_string.compute(&rdd_partition).map_err(SqlError::from)?;
                <String as ToRecordBatchConverter>::convert(data)
            } else if let Some(rdd_string_i32) = rdd
                .as_any()
                .downcast_ref::<barks_core::rdd::DistributedRdd<(String, i32)>>()
            {
                let rdd_partition = barks_core::traits::BasicPartition::new(partition);
                let data = rdd_string_i32
                    .compute(&rdd_partition)
                    .map_err(SqlError::from)?;
                <(String, i32) as ToRecordBatchConverter>::convert(data)
            } else if let Some(rdd_i32_string) = rdd
                .as_any()
                .downcast_ref::<barks_core::rdd::DistributedRdd<(i32, String)>>()
            {
                let rdd_partition = barks_core::traits::BasicPartition::new(partition);
                let data = rdd_i32_string
                    .compute(&rdd_partition)
                    .map_err(SqlError::from)?;
                <(i32, String) as ToRecordBatchConverter>::convert(data)
            } else {
                return Err(DataFusionError::NotImplemented(format!(
                    "Unsupported RDD type for SQL execution: {:?}",
                    rdd
                )));
            };

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
