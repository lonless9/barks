//! Columnar data processing utilities for SQL operations
//!
//! This module provides utilities for converting between row-based RDD data
//! and columnar Arrow data structures used by DataFusion.

use crate::traits::{SqlError, SqlResult};
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, StringArray,
    UInt32Array, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// Trait for converting data types to Arrow arrays
pub trait ToArrowArray {
    /// Convert a vector of values to an Arrow array
    fn to_arrow_array(values: Vec<Self>) -> SqlResult<ArrayRef>
    where
        Self: Sized;

    /// Get the Arrow data type for this Rust type
    fn arrow_data_type() -> DataType;
}

// Implementations for common types
impl ToArrowArray for i32 {
    fn to_arrow_array(values: Vec<Self>) -> SqlResult<ArrayRef> {
        Ok(Arc::new(Int32Array::from(values)))
    }

    fn arrow_data_type() -> DataType {
        DataType::Int32
    }
}

impl ToArrowArray for i64 {
    fn to_arrow_array(values: Vec<Self>) -> SqlResult<ArrayRef> {
        Ok(Arc::new(Int64Array::from(values)))
    }

    fn arrow_data_type() -> DataType {
        DataType::Int64
    }
}

impl ToArrowArray for u32 {
    fn to_arrow_array(values: Vec<Self>) -> SqlResult<ArrayRef> {
        Ok(Arc::new(UInt32Array::from(values)))
    }

    fn arrow_data_type() -> DataType {
        DataType::UInt32
    }
}

impl ToArrowArray for u64 {
    fn to_arrow_array(values: Vec<Self>) -> SqlResult<ArrayRef> {
        Ok(Arc::new(UInt64Array::from(values)))
    }

    fn arrow_data_type() -> DataType {
        DataType::UInt64
    }
}

impl ToArrowArray for f32 {
    fn to_arrow_array(values: Vec<Self>) -> SqlResult<ArrayRef> {
        Ok(Arc::new(Float32Array::from(values)))
    }

    fn arrow_data_type() -> DataType {
        DataType::Float32
    }
}

impl ToArrowArray for f64 {
    fn to_arrow_array(values: Vec<Self>) -> SqlResult<ArrayRef> {
        Ok(Arc::new(Float64Array::from(values)))
    }

    fn arrow_data_type() -> DataType {
        DataType::Float64
    }
}

impl ToArrowArray for String {
    fn to_arrow_array(values: Vec<Self>) -> SqlResult<ArrayRef> {
        Ok(Arc::new(StringArray::from(values)))
    }

    fn arrow_data_type() -> DataType {
        DataType::Utf8
    }
}

impl ToArrowArray for bool {
    fn to_arrow_array(values: Vec<Self>) -> SqlResult<ArrayRef> {
        Ok(Arc::new(BooleanArray::from(values)))
    }

    fn arrow_data_type() -> DataType {
        DataType::Boolean
    }
}

/// Trait for converting RDD partition data (a Vec<T>) into a RecordBatch.
pub trait ToRecordBatch {
    /// Convert a vector of items into a single RecordBatch.
    fn to_record_batch(data: Vec<Self>) -> SqlResult<RecordBatch>
    where
        Self: Sized;

    /// Get the Arrow schema corresponding to this type.
    fn to_schema() -> SqlResult<SchemaRef>;
}

impl ToRecordBatch for i32 {
    fn to_record_batch(data: Vec<Self>) -> SqlResult<RecordBatch> {
        let schema = Self::to_schema()?;
        let array = i32::to_arrow_array(data)?;
        RecordBatch::try_new(schema, vec![array]).map_err(SqlError::from)
    }

    fn to_schema() -> SqlResult<SchemaRef> {
        Ok(Arc::new(Schema::new(vec![Field::new(
            "value",
            i32::arrow_data_type(),
            true,
        )])))
    }
}

impl ToRecordBatch for String {
    fn to_record_batch(data: Vec<Self>) -> SqlResult<RecordBatch> {
        let schema = Self::to_schema()?;
        let array = String::to_arrow_array(data)?;
        RecordBatch::try_new(schema, vec![array]).map_err(SqlError::from)
    }

    fn to_schema() -> SqlResult<SchemaRef> {
        Ok(Arc::new(Schema::new(vec![Field::new(
            "value",
            String::arrow_data_type(),
            true,
        )])))
    }
}

impl ToRecordBatch for (String, i32) {
    fn to_record_batch(data: Vec<Self>) -> SqlResult<RecordBatch> {
        conversion::tuples_to_record_batch(data, "c0", "c1")
    }

    fn to_schema() -> SqlResult<SchemaRef> {
        Ok(Arc::new(Schema::new(vec![
            Field::new("c0", String::arrow_data_type(), true),
            Field::new("c1", i32::arrow_data_type(), true),
        ])))
    }
}

impl ToRecordBatch for (i32, String) {
    fn to_record_batch(data: Vec<Self>) -> SqlResult<RecordBatch> {
        conversion::tuples_to_record_batch(data, "c0", "c1")
    }

    fn to_schema() -> SqlResult<SchemaRef> {
        Ok(Arc::new(Schema::new(vec![
            Field::new("c0", i32::arrow_data_type(), true),
            Field::new("c1", String::arrow_data_type(), true),
        ])))
    }
}

/// Trait for converting from Arrow arrays back to Rust types
pub trait FromArrowArray: Sized {
    /// Convert an Arrow array to a vector of values
    fn from_arrow_array(array: &dyn Array) -> SqlResult<Vec<Self>>;
}

impl FromArrowArray for i32 {
    fn from_arrow_array(array: &dyn Array) -> SqlResult<Vec<Self>> {
        let int_array = array
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| SqlError::Schema("Expected Int32Array".to_string()))?;

        let mut values = Vec::with_capacity(int_array.len());
        for i in 0..int_array.len() {
            if int_array.is_null(i) {
                return Err(SqlError::Schema("Null values not supported".to_string()));
            }
            values.push(int_array.value(i));
        }
        Ok(values)
    }
}

impl FromArrowArray for String {
    fn from_arrow_array(array: &dyn Array) -> SqlResult<Vec<Self>> {
        let string_array = array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| SqlError::Schema("Expected StringArray".to_string()))?;

        let mut values = Vec::with_capacity(string_array.len());
        for i in 0..string_array.len() {
            if string_array.is_null(i) {
                return Err(SqlError::Schema("Null values not supported".to_string()));
            }
            values.push(string_array.value(i).to_string());
        }
        Ok(values)
    }
}

/// Utility for creating RecordBatches from typed data
pub struct RecordBatchBuilder {
    schema: SchemaRef,
    columns: Vec<ArrayRef>,
}

impl RecordBatchBuilder {
    /// Create a new RecordBatch builder with the given schema
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            columns: Vec::new(),
        }
    }

    /// Add a column of data to the builder
    pub fn add_column<T: ToArrowArray>(mut self, values: Vec<T>) -> SqlResult<Self> {
        let array = T::to_arrow_array(values)?;
        self.columns.push(array);
        Ok(self)
    }

    /// Build the RecordBatch
    pub fn build(self) -> SqlResult<RecordBatch> {
        if self.columns.len() != self.schema.fields().len() {
            return Err(SqlError::Schema(format!(
                "Column count mismatch: expected {}, got {}",
                self.schema.fields().len(),
                self.columns.len()
            )));
        }

        RecordBatch::try_new(self.schema, self.columns)
            .map_err(|e| SqlError::Schema(format!("Failed to create RecordBatch: {}", e)))
    }
}

/// Utility for extracting data from RecordBatches
pub struct RecordBatchExtractor {
    batch: RecordBatch,
}

impl RecordBatchExtractor {
    /// Create a new extractor for the given RecordBatch
    pub fn new(batch: RecordBatch) -> Self {
        Self { batch }
    }

    /// Extract a column as a vector of values
    pub fn extract_column<T: FromArrowArray>(&self, column_index: usize) -> SqlResult<Vec<T>> {
        if column_index >= self.batch.num_columns() {
            return Err(SqlError::Schema(format!(
                "Column index {} out of bounds (max: {})",
                column_index,
                self.batch.num_columns() - 1
            )));
        }

        let array = self.batch.column(column_index);
        T::from_arrow_array(array.as_ref())
    }

    /// Get the schema of the RecordBatch
    pub fn schema(&self) -> SchemaRef {
        self.batch.schema()
    }

    /// Get the number of rows
    pub fn num_rows(&self) -> usize {
        self.batch.num_rows()
    }

    /// Get the number of columns
    pub fn num_columns(&self) -> usize {
        self.batch.num_columns()
    }
}

/// Columnar data conversion utilities
pub mod conversion {
    use super::*;

    /// Convert a vector of tuples to a RecordBatch
    pub fn tuples_to_record_batch<T1, T2>(
        tuples: Vec<(T1, T2)>,
        field1_name: &str,
        field2_name: &str,
    ) -> SqlResult<RecordBatch>
    where
        T1: ToArrowArray,
        T2: ToArrowArray,
    {
        let (col1, col2): (Vec<T1>, Vec<T2>) = tuples.into_iter().unzip();

        let schema = Arc::new(Schema::new(vec![
            Field::new(field1_name, T1::arrow_data_type(), false),
            Field::new(field2_name, T2::arrow_data_type(), false),
        ]));

        RecordBatchBuilder::new(schema)
            .add_column(col1)?
            .add_column(col2)?
            .build()
    }

    /// Convert a RecordBatch to a vector of tuples
    pub fn record_batch_to_tuples<T1, T2>(batch: RecordBatch) -> SqlResult<Vec<(T1, T2)>>
    where
        T1: FromArrowArray,
        T2: FromArrowArray,
    {
        if batch.num_columns() != 2 {
            return Err(SqlError::Schema(format!(
                "Expected 2 columns, got {}",
                batch.num_columns()
            )));
        }

        let extractor = RecordBatchExtractor::new(batch);
        let col1 = extractor.extract_column::<T1>(0)?;
        let col2 = extractor.extract_column::<T2>(1)?;

        if col1.len() != col2.len() {
            return Err(SqlError::Schema("Column length mismatch".to_string()));
        }

        Ok(col1.into_iter().zip(col2).collect())
    }

    /// Create a simple single-column RecordBatch
    pub fn single_column_batch<T: ToArrowArray>(
        values: Vec<T>,
        column_name: &str,
    ) -> SqlResult<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            column_name,
            T::arrow_data_type(),
            false,
        )]));

        RecordBatchBuilder::new(schema).add_column(values)?.build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_i32_to_arrow_array() {
        let values = vec![1, 2, 3, 4, 5];
        let array = i32::to_arrow_array(values).unwrap();

        assert_eq!(array.len(), 5);
        assert_eq!(array.data_type(), &DataType::Int32);
    }

    #[test]
    fn test_string_to_arrow_array() {
        let values = vec!["hello".to_string(), "world".to_string()];
        let array = String::to_arrow_array(values).unwrap();

        assert_eq!(array.len(), 2);
        assert_eq!(array.data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_record_batch_builder() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batch = RecordBatchBuilder::new(schema)
            .add_column(vec![1, 2, 3])
            .unwrap()
            .add_column(vec!["a".to_string(), "b".to_string(), "c".to_string()])
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_tuples_to_record_batch() {
        let tuples = vec![
            (1, "a".to_string()),
            (2, "b".to_string()),
            (3, "c".to_string()),
        ];
        let batch = conversion::tuples_to_record_batch(tuples, "id", "name").unwrap();

        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_single_column_batch() {
        let values = vec![10, 20, 30];
        let batch = conversion::single_column_batch(values, "value").unwrap();

        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.schema().field(0).name(), "value");
    }
}
