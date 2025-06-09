//! SQL type definitions for Barks
//!
//! This module provides type definitions and utilities for SQL operations.

use datafusion::arrow::datatypes::DataType;
use serde::{Deserialize, Serialize};

/// SQL data types supported by Barks
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SqlDataType {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    String,
    Binary,
    Date,
    Timestamp,
    Decimal { precision: u8, scale: i8 },
    List(Box<SqlDataType>),
    Struct(Vec<SqlField>),
}

/// SQL field definition
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SqlField {
    pub name: String,
    pub data_type: SqlDataType,
    pub nullable: bool,
}

impl SqlField {
    pub fn new(name: String, data_type: SqlDataType, nullable: bool) -> Self {
        Self {
            name,
            data_type,
            nullable,
        }
    }
}

/// SQL schema definition
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SqlSchema {
    pub fields: Vec<SqlField>,
}

impl SqlSchema {
    pub fn new(fields: Vec<SqlField>) -> Self {
        Self { fields }
    }

    pub fn empty() -> Self {
        Self { fields: Vec::new() }
    }
}

impl From<SqlDataType> for DataType {
    fn from(sql_type: SqlDataType) -> Self {
        match sql_type {
            SqlDataType::Boolean => DataType::Boolean,
            SqlDataType::Int8 => DataType::Int8,
            SqlDataType::Int16 => DataType::Int16,
            SqlDataType::Int32 => DataType::Int32,
            SqlDataType::Int64 => DataType::Int64,
            SqlDataType::UInt8 => DataType::UInt8,
            SqlDataType::UInt16 => DataType::UInt16,
            SqlDataType::UInt32 => DataType::UInt32,
            SqlDataType::UInt64 => DataType::UInt64,
            SqlDataType::Float32 => DataType::Float32,
            SqlDataType::Float64 => DataType::Float64,
            SqlDataType::String => DataType::Utf8,
            SqlDataType::Binary => DataType::Binary,
            SqlDataType::Date => DataType::Date32,
            SqlDataType::Timestamp => {
                DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None)
            }
            SqlDataType::Decimal { precision, scale } => DataType::Decimal128(precision, scale),
            SqlDataType::List(inner) => DataType::List(Arc::new(
                datafusion::arrow::datatypes::Field::new("item", (*inner).into(), true),
            )),
            SqlDataType::Struct(fields) => {
                let arrow_fields: Vec<_> = fields
                    .into_iter()
                    .map(|f| {
                        datafusion::arrow::datatypes::Field::new(
                            f.name,
                            f.data_type.into(),
                            f.nullable,
                        )
                    })
                    .collect();
                DataType::Struct(arrow_fields.into())
            }
        }
    }
}

use std::sync::Arc;

impl From<DataType> for SqlDataType {
    fn from(arrow_type: DataType) -> Self {
        match arrow_type {
            DataType::Boolean => SqlDataType::Boolean,
            DataType::Int8 => SqlDataType::Int8,
            DataType::Int16 => SqlDataType::Int16,
            DataType::Int32 => SqlDataType::Int32,
            DataType::Int64 => SqlDataType::Int64,
            DataType::UInt8 => SqlDataType::UInt8,
            DataType::UInt16 => SqlDataType::UInt16,
            DataType::UInt32 => SqlDataType::UInt32,
            DataType::UInt64 => SqlDataType::UInt64,
            DataType::Float32 => SqlDataType::Float32,
            DataType::Float64 => SqlDataType::Float64,
            DataType::Utf8 => SqlDataType::String,
            DataType::Binary => SqlDataType::Binary,
            DataType::Date32 => SqlDataType::Date,
            DataType::Timestamp(_, _) => SqlDataType::Timestamp,
            DataType::Decimal128(precision, scale) => SqlDataType::Decimal { precision, scale },
            DataType::List(field) => SqlDataType::List(Box::new(field.data_type().clone().into())),
            DataType::Struct(fields) => {
                let sql_fields: Vec<_> = fields
                    .iter()
                    .map(|f| {
                        SqlField::new(
                            f.name().clone(),
                            f.data_type().clone().into(),
                            f.is_nullable(),
                        )
                    })
                    .collect();
                SqlDataType::Struct(sql_fields)
            }
            _ => SqlDataType::String, // Default fallback
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_data_type_conversion() {
        let sql_type = SqlDataType::Int32;
        let arrow_type: DataType = sql_type.into();
        assert_eq!(arrow_type, DataType::Int32);

        let back_to_sql: SqlDataType = arrow_type.into();
        assert_eq!(back_to_sql, SqlDataType::Int32);
    }

    #[test]
    fn test_sql_field_creation() {
        let field = SqlField::new("test".to_string(), SqlDataType::String, true);
        assert_eq!(field.name, "test");
        assert_eq!(field.data_type, SqlDataType::String);
        assert!(field.nullable);
    }

    #[test]
    fn test_sql_schema_creation() {
        let fields = vec![
            SqlField::new("id".to_string(), SqlDataType::Int32, false),
            SqlField::new("name".to_string(), SqlDataType::String, true),
        ];
        let schema = SqlSchema::new(fields);
        assert_eq!(schema.fields.len(), 2);
    }
}
