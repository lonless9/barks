//! Literal value representations for Barks data types.
//!
//! Based on Sail "https://github.com/lakehq/sail"

use std::str::FromStr;

use arrow::datatypes::i256;
use barks_common::error::{CommonError, Result};
use half::f16;
use serde::de::Error as SerdeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::types::{self, IntervalUnit, PrimitiveType, TimeUnit};
use crate::types::{NoMetadata, serializable as types_serializable};

/// Represents a literal value for any of the supported data types.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum Literal {
    Null,
    Boolean {
        value: Option<bool>,
    },
    Int8 {
        value: Option<i8>,
    },
    Int16 {
        value: Option<i16>,
    },
    Int32 {
        value: Option<i32>,
    },
    Int64 {
        value: Option<i64>,
    },
    UInt8 {
        value: Option<u8>,
    },
    UInt16 {
        value: Option<u16>,
    },
    UInt32 {
        value: Option<u32>,
    },
    UInt64 {
        value: Option<u64>,
    },
    Float16 {
        value: Option<f16>,
    },
    Float32 {
        value: Option<f32>,
    },
    Float64 {
        value: Option<f64>,
    },
    TimestampSecond {
        seconds: Option<i64>,
        timestamp_type: types::TimestampType,
    },
    TimestampMillisecond {
        milliseconds: Option<i64>,
        timestamp_type: types::TimestampType,
    },
    TimestampMicrosecond {
        microseconds: Option<i64>,
        timestamp_type: types::TimestampType,
    },
    TimestampNanosecond {
        nanoseconds: Option<i64>,
        timestamp_type: types::TimestampType,
    },
    Date32 {
        days: Option<i32>,
    },
    Date64 {
        milliseconds: Option<i64>,
    },
    Time32Second {
        seconds: Option<i32>,
    },
    Time32Millisecond {
        milliseconds: Option<i32>,
    },
    Time64Microsecond {
        microseconds: Option<i64>,
    },
    Time64Nanosecond {
        nanoseconds: Option<i64>,
    },
    DurationSecond {
        seconds: Option<i64>,
    },
    DurationMillisecond {
        milliseconds: Option<i64>,
    },
    DurationMicrosecond {
        microseconds: Option<i64>,
    },
    DurationNanosecond {
        nanoseconds: Option<i64>,
    },
    IntervalYearMonth {
        months: Option<i32>,
    },
    IntervalDayTime {
        value: Option<IntervalDayTime>,
    },
    IntervalMonthDayNano {
        value: Option<IntervalMonthDayNano>,
    },
    Binary {
        value: Option<Vec<u8>>,
    },
    FixedSizeBinary {
        size: i32,
        value: Option<Vec<u8>>,
    },
    LargeBinary {
        value: Option<Vec<u8>>,
    },
    BinaryView {
        value: Option<Vec<u8>>,
    },
    Utf8 {
        value: Option<String>,
    },
    LargeUtf8 {
        value: Option<String>,
    },
    Utf8View {
        value: Option<String>,
    },
    List {
        data_type: Box<types_serializable::DataType>,
        values: Option<Vec<Literal>>,
    },
    FixedSizeList {
        length: i32,
        data_type: Box<types_serializable::DataType>,
        values: Option<Vec<Literal>>,
    },
    LargeList {
        data_type: Box<types_serializable::DataType>,
        values: Option<Vec<Literal>>,
    },
    Struct {
        data_type: types_serializable::DataType,
        values: Option<Vec<Literal>>,
    },
    Union {
        union_fields: types_serializable::UnionFields,
        union_mode: types::UnionMode,
        value: Option<(i8, Box<Literal>)>,
    },
    Dictionary {
        key_type: Box<types_serializable::DataType>,
        value_type: Box<types_serializable::DataType>,
        value: Option<Box<Literal>>,
    },
    Decimal128 {
        precision: u8,
        scale: i8,
        #[serde(
            serialize_with = "serialize_optional",
            deserialize_with = "deserialize_optional"
        )]
        value: Option<i128>,
    },
    Decimal256 {
        precision: u8,
        scale: i8,
        #[serde(
            serialize_with = "serialize_optional",
            deserialize_with = "deserialize_optional"
        )]
        value: Option<i256>,
    },
    Map {
        key_type: Box<types_serializable::DataType>,
        value_type: Box<types_serializable::DataType>,
        keys: Option<Vec<Literal>>,
        values: Option<Vec<Literal>>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IntervalDayTime {
    pub days: i32,
    pub milliseconds: i32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IntervalMonthDayNano {
    pub months: i32,
    pub days: i32,
    pub nanoseconds: i64,
}

fn serialize_optional<T, S>(
    value: &Option<T>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    T: ToString,
    S: Serializer,
{
    match value {
        Some(num) => serializer.serialize_some(&num.to_string()),
        None => serializer.serialize_none(),
    }
}

fn deserialize_optional<'de, T, D>(deserializer: D) -> std::result::Result<Option<T>, D::Error>
where
    T: FromStr,
    T::Err: std::fmt::Display,
    D: Deserializer<'de>,
{
    let s: Option<String> = Option::deserialize(deserializer)?;
    match s {
        Some(s) => T::from_str(&s).map(Some).map_err(SerdeError::custom),
        None => Ok(None),
    }
}

/// Creates a null `Literal` corresponding to the given data type.
pub fn data_type_to_null_literal(data_type: &types::DataType<NoMetadata>) -> Result<Literal> {
    let error = |x: &types::DataType<NoMetadata>| {
        CommonError::invalid(format!("null literal not supported for type: {x:?}"))
    };

    match &data_type.kind {
        types::DataTypeKind::Primitive(p) => match p {
            PrimitiveType::Null => Ok(Literal::Null),
            PrimitiveType::Boolean => Ok(Literal::Boolean { value: None }),
            PrimitiveType::Int8 => Ok(Literal::Int8 { value: None }),
            PrimitiveType::Int16 => Ok(Literal::Int16 { value: None }),
            PrimitiveType::Int32 => Ok(Literal::Int32 { value: None }),
            PrimitiveType::Int64 => Ok(Literal::Int64 { value: None }),
            PrimitiveType::UInt8 => Ok(Literal::UInt8 { value: None }),
            PrimitiveType::UInt16 => Ok(Literal::UInt16 { value: None }),
            PrimitiveType::UInt32 => Ok(Literal::UInt32 { value: None }),
            PrimitiveType::UInt64 => Ok(Literal::UInt64 { value: None }),
            PrimitiveType::Float16 => Ok(Literal::Float16 { value: None }),
            PrimitiveType::Float32 => Ok(Literal::Float32 { value: None }),
            PrimitiveType::Float64 => Ok(Literal::Float64 { value: None }),
            PrimitiveType::Date32 => Ok(Literal::Date32 { days: None }),
            PrimitiveType::Date64 => Ok(Literal::Date64 { milliseconds: None }),
            PrimitiveType::Binary => Ok(Literal::Binary { value: None }),
            PrimitiveType::LargeBinary => Ok(Literal::LargeBinary { value: None }),
            PrimitiveType::BinaryView => Ok(Literal::BinaryView { value: None }),
            PrimitiveType::Utf8 => Ok(Literal::Utf8 { value: None }),
            PrimitiveType::LargeUtf8 => Ok(Literal::LargeUtf8 { value: None }),
            PrimitiveType::Utf8View => Ok(Literal::Utf8View { value: None }),
            PrimitiveType::ConfiguredBinary => Ok(Literal::Binary { value: None }),
        },
        types::DataTypeKind::Timestamp {
            time_unit,
            timestamp_type,
        } => match time_unit {
            TimeUnit::Second => Ok(Literal::TimestampSecond {
                seconds: None,
                timestamp_type: timestamp_type.clone(),
            }),
            TimeUnit::Millisecond => Ok(Literal::TimestampMillisecond {
                milliseconds: None,
                timestamp_type: timestamp_type.clone(),
            }),
            TimeUnit::Microsecond => Ok(Literal::TimestampMicrosecond {
                microseconds: None,
                timestamp_type: timestamp_type.clone(),
            }),
            TimeUnit::Nanosecond => Ok(Literal::TimestampNanosecond {
                nanoseconds: None,
                timestamp_type: timestamp_type.clone(),
            }),
        },
        types::DataTypeKind::Time32 { time_unit } => match time_unit {
            TimeUnit::Second => Ok(Literal::Time32Second { seconds: None }),
            TimeUnit::Millisecond => Ok(Literal::Time32Millisecond { milliseconds: None }),
            TimeUnit::Microsecond | TimeUnit::Nanosecond => Err(error(data_type)),
        },
        types::DataTypeKind::Time64 { time_unit } => match time_unit {
            TimeUnit::Second | TimeUnit::Millisecond => Err(error(data_type)),
            TimeUnit::Microsecond => Ok(Literal::Time64Microsecond { microseconds: None }),
            TimeUnit::Nanosecond => Ok(Literal::Time64Nanosecond { nanoseconds: None }),
        },
        types::DataTypeKind::Duration { time_unit } => match time_unit {
            TimeUnit::Second => Ok(Literal::DurationSecond { seconds: None }),
            TimeUnit::Millisecond => Ok(Literal::DurationMillisecond { milliseconds: None }),
            TimeUnit::Microsecond => Ok(Literal::DurationMicrosecond { microseconds: None }),
            TimeUnit::Nanosecond => Ok(Literal::DurationNanosecond { nanoseconds: None }),
        },
        types::DataTypeKind::Interval { interval_unit, .. } => match interval_unit {
            IntervalUnit::YearMonth => Ok(Literal::IntervalYearMonth { months: None }),
            IntervalUnit::DayTime => Ok(Literal::IntervalDayTime { value: None }),
            IntervalUnit::MonthDayNano => Ok(Literal::IntervalMonthDayNano { value: None }),
        },
        types::DataTypeKind::FixedSizeBinary { size } => Ok(Literal::FixedSizeBinary {
            size: *size,
            value: None,
        }),
        types::DataTypeKind::List { data_type, .. } => Ok(Literal::List {
            data_type: Box::new((**data_type).clone().into()),
            values: None,
        }),
        types::DataTypeKind::FixedSizeList {
            length, data_type, ..
        } => Ok(Literal::FixedSizeList {
            length: *length,
            data_type: Box::new((**data_type).clone().into()),
            values: None,
        }),
        types::DataTypeKind::LargeList { data_type, .. } => Ok(Literal::LargeList {
            data_type: Box::new((**data_type).clone().into()),
            values: None,
        }),
        types::DataTypeKind::Struct { .. } => Ok(Literal::Struct {
            data_type: data_type.clone().into(),
            values: None,
        }),
        types::DataTypeKind::Union {
            union_fields,
            union_mode,
        } => Ok(Literal::Union {
            union_fields: union_fields.clone().into(),
            union_mode: *union_mode,
            value: None,
        }),
        types::DataTypeKind::Dictionary {
            key_type,
            value_type,
        } => Ok(Literal::Dictionary {
            key_type: Box::new((**key_type).clone().into()),
            value_type: Box::new((**value_type).clone().into()),
            value: None,
        }),
        types::DataTypeKind::Decimal128 { precision, scale } => Ok(Literal::Decimal128 {
            precision: *precision,
            scale: *scale,
            value: None,
        }),
        types::DataTypeKind::Decimal256 { precision, scale } => Ok(Literal::Decimal256 {
            precision: *precision,
            scale: *scale,
            value: None,
        }),
        types::DataTypeKind::Map {
            key_type,
            value_type,
            ..
        } => Ok(Literal::Map {
            key_type: Box::new((**key_type).clone().into()),
            value_type: Box::new((**value_type).clone().into()),
            keys: None,
            values: None,
        }),
        types::DataTypeKind::ConfiguredUtf8 { .. } => Ok(Literal::Utf8 { value: None }),
        types::DataTypeKind::UserDefined { sql_type, .. } => data_type_to_null_literal(sql_type),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DataType, DataTypeKind, TimestampType};
    use arrow::datatypes::i256;
    use half::f16;

    #[test]
    fn test_literal_creation() {
        // Test basic literal creation
        let null_lit = Literal::Null;
        assert_eq!(null_lit, Literal::Null);

        let bool_lit = Literal::Boolean { value: Some(true) };
        match bool_lit {
            Literal::Boolean { value: Some(true) } => {}
            _ => panic!("Expected boolean literal"),
        }

        let int_lit = Literal::Int32 { value: Some(42) };
        match int_lit {
            Literal::Int32 { value: Some(42) } => {}
            _ => panic!("Expected int32 literal"),
        }

        let string_lit = Literal::Utf8 {
            value: Some("hello".to_string()),
        };
        match string_lit {
            Literal::Utf8 { value: Some(s) } if s == "hello" => {}
            _ => panic!("Expected string literal"),
        }
    }

    #[test]
    fn test_numeric_literals() {
        // Test all numeric types
        let i8_lit = Literal::Int8 { value: Some(-128) };
        let i16_lit = Literal::Int16 {
            value: Some(-32768),
        };
        let i32_lit = Literal::Int32 {
            value: Some(-2147483648),
        };
        let i64_lit = Literal::Int64 {
            value: Some(-9223372036854775808),
        };

        let u8_lit = Literal::UInt8 { value: Some(255) };
        let u16_lit = Literal::UInt16 { value: Some(65535) };
        let u32_lit = Literal::UInt32 {
            value: Some(4294967295),
        };
        let u64_lit = Literal::UInt64 {
            value: Some(18446744073709551615),
        };

        let f16_lit = Literal::Float16 {
            value: Some(f16::from_f32(std::f32::consts::PI)),
        };
        let f32_lit = Literal::Float32 {
            value: Some(std::f32::consts::PI),
        };
        let f64_lit = Literal::Float64 {
            value: Some(std::f64::consts::PI),
        };

        // Verify they can be pattern matched
        match i8_lit {
            Literal::Int8 { value: Some(-128) } => {}
            _ => panic!("Expected i8 literal"),
        }

        // Test all integer types
        match i16_lit {
            Literal::Int16 {
                value: Some(-32768),
            } => {}
            _ => panic!("Expected i16 literal"),
        }

        match i32_lit {
            Literal::Int32 {
                value: Some(-2147483648),
            } => {}
            _ => panic!("Expected i32 literal"),
        }

        match i64_lit {
            Literal::Int64 {
                value: Some(-9223372036854775808),
            } => {}
            _ => panic!("Expected i64 literal"),
        }

        // Test unsigned integer types
        match u8_lit {
            Literal::UInt8 { value: Some(255) } => {}
            _ => panic!("Expected u8 literal"),
        }

        match u16_lit {
            Literal::UInt16 { value: Some(65535) } => {}
            _ => panic!("Expected u16 literal"),
        }

        match u32_lit {
            Literal::UInt32 {
                value: Some(4294967295),
            } => {}
            _ => panic!("Expected u32 literal"),
        }

        match u64_lit {
            Literal::UInt64 {
                value: Some(18446744073709551615),
            } => {}
            _ => panic!("Expected u64 literal"),
        }

        // Test floating point types
        match f16_lit {
            Literal::Float16 { value: Some(v) }
                if (v.to_f32() - std::f32::consts::PI).abs() < 0.01 => {}
            _ => panic!("Expected f16 literal"),
        }

        match f32_lit {
            Literal::Float32 { value: Some(v) }
                if (v - std::f32::consts::PI).abs() < f32::EPSILON => {}
            _ => panic!("Expected f32 literal"),
        }

        match f64_lit {
            Literal::Float64 { value: Some(v) }
                if (v - std::f64::consts::PI).abs() < f64::EPSILON => {}
            _ => panic!("Expected f64 literal"),
        }
    }

    #[test]
    fn test_temporal_literals() {
        // Test timestamp literals
        let ts_sec = Literal::TimestampSecond {
            seconds: Some(1640995200),
            timestamp_type: TimestampType::WithoutTimeZone,
        };

        let ts_ms = Literal::TimestampMillisecond {
            milliseconds: Some(1640995200000),
            timestamp_type: TimestampType::WithLocalTimeZone,
        };

        let ts_us = Literal::TimestampMicrosecond {
            microseconds: Some(1640995200000000),
            timestamp_type: TimestampType::Configured,
        };

        let ts_ns = Literal::TimestampNanosecond {
            nanoseconds: Some(1640995200000000000),
            timestamp_type: TimestampType::WithoutTimeZone,
        };

        // Test date literals
        let date32_lit = Literal::Date32 { days: Some(18628) };
        let date64_lit = Literal::Date64 {
            milliseconds: Some(1640995200000),
        };

        // Test time literals
        let time32_sec = Literal::Time32Second {
            seconds: Some(3661),
        };
        let time32_ms = Literal::Time32Millisecond {
            milliseconds: Some(3661000),
        };
        let time64_us = Literal::Time64Microsecond {
            microseconds: Some(3661000000),
        };
        let time64_ns = Literal::Time64Nanosecond {
            nanoseconds: Some(3661000000000),
        };

        // Test duration literals
        let dur_sec = Literal::DurationSecond {
            seconds: Some(3600),
        };
        let dur_ms = Literal::DurationMillisecond {
            milliseconds: Some(3600000),
        };
        let dur_us = Literal::DurationMicrosecond {
            microseconds: Some(3600000000),
        };
        let dur_ns = Literal::DurationNanosecond {
            nanoseconds: Some(3600000000000),
        };

        // Verify pattern matching works
        match ts_sec {
            Literal::TimestampSecond {
                seconds: Some(1640995200),
                timestamp_type: TimestampType::WithoutTimeZone,
            } => {}
            _ => panic!("Expected timestamp second literal"),
        }

        match ts_ms {
            Literal::TimestampMillisecond {
                milliseconds: Some(1640995200000),
                timestamp_type: TimestampType::WithLocalTimeZone,
            } => {}
            _ => panic!("Expected timestamp millisecond literal"),
        }

        match ts_us {
            Literal::TimestampMicrosecond {
                microseconds: Some(1640995200000000),
                timestamp_type: TimestampType::Configured,
            } => {}
            _ => panic!("Expected timestamp microsecond literal"),
        }

        match ts_ns {
            Literal::TimestampNanosecond {
                nanoseconds: Some(1640995200000000000),
                timestamp_type: TimestampType::WithoutTimeZone,
            } => {}
            _ => panic!("Expected timestamp nanosecond literal"),
        }

        match date32_lit {
            Literal::Date32 { days: Some(18628) } => {}
            _ => panic!("Expected date32 literal"),
        }

        match date64_lit {
            Literal::Date64 {
                milliseconds: Some(1640995200000),
            } => {}
            _ => panic!("Expected date64 literal"),
        }

        // Test time literals
        match time32_sec {
            Literal::Time32Second {
                seconds: Some(3661),
            } => {}
            _ => panic!("Expected time32 second literal"),
        }

        match time32_ms {
            Literal::Time32Millisecond {
                milliseconds: Some(3661000),
            } => {}
            _ => panic!("Expected time32 millisecond literal"),
        }

        match time64_us {
            Literal::Time64Microsecond {
                microseconds: Some(3661000000),
            } => {}
            _ => panic!("Expected time64 microsecond literal"),
        }

        match time64_ns {
            Literal::Time64Nanosecond {
                nanoseconds: Some(3661000000000),
            } => {}
            _ => panic!("Expected time64 nanosecond literal"),
        }

        // Test duration literals
        match dur_sec {
            Literal::DurationSecond {
                seconds: Some(3600),
            } => {}
            _ => panic!("Expected duration second literal"),
        }

        match dur_ms {
            Literal::DurationMillisecond {
                milliseconds: Some(3600000),
            } => {}
            _ => panic!("Expected duration millisecond literal"),
        }

        match dur_us {
            Literal::DurationMicrosecond {
                microseconds: Some(3600000000),
            } => {}
            _ => panic!("Expected duration microsecond literal"),
        }

        match dur_ns {
            Literal::DurationNanosecond {
                nanoseconds: Some(3600000000000),
            } => {}
            _ => panic!("Expected duration nanosecond literal"),
        }
    }

    #[test]
    fn test_interval_literals() {
        // Test interval year-month
        let interval_ym = Literal::IntervalYearMonth { months: Some(14) };

        // Test interval day-time
        let interval_dt = Literal::IntervalDayTime {
            value: Some(IntervalDayTime {
                days: 5,
                milliseconds: 3600000,
            }),
        };

        // Test interval month-day-nano
        let interval_mdn = Literal::IntervalMonthDayNano {
            value: Some(IntervalMonthDayNano {
                months: 2,
                days: 15,
                nanoseconds: 3600000000000,
            }),
        };

        // Verify pattern matching
        match interval_ym {
            Literal::IntervalYearMonth { months: Some(14) } => {}
            _ => panic!("Expected interval year-month literal"),
        }

        match interval_dt {
            Literal::IntervalDayTime {
                value:
                    Some(IntervalDayTime {
                        days: 5,
                        milliseconds: 3600000,
                    }),
            } => {}
            _ => panic!("Expected interval day-time literal"),
        }

        match interval_mdn {
            Literal::IntervalMonthDayNano {
                value:
                    Some(IntervalMonthDayNano {
                        months: 2,
                        days: 15,
                        nanoseconds: 3600000000000,
                    }),
            } => {}
            _ => panic!("Expected interval month-day-nano literal"),
        }
    }

    #[test]
    fn test_binary_literals() {
        let binary_data = vec![0x48, 0x65, 0x6c, 0x6c, 0x6f]; // "Hello" in bytes

        let binary_lit = Literal::Binary {
            value: Some(binary_data.clone()),
        };

        let fixed_binary_lit = Literal::FixedSizeBinary {
            size: 5,
            value: Some(binary_data.clone()),
        };

        let large_binary_lit = Literal::LargeBinary {
            value: Some(binary_data.clone()),
        };

        let binary_view_lit = Literal::BinaryView {
            value: Some(binary_data.clone()),
        };

        // Verify pattern matching
        match binary_lit {
            Literal::Binary { value: Some(data) } if data == binary_data => {}
            _ => panic!("Expected binary literal"),
        }

        match fixed_binary_lit {
            Literal::FixedSizeBinary {
                size: 5,
                value: Some(data),
            } if data == binary_data => {}
            _ => panic!("Expected fixed size binary literal"),
        }

        match large_binary_lit {
            Literal::LargeBinary { value: Some(data) } if data == binary_data => {}
            _ => panic!("Expected large binary literal"),
        }

        match binary_view_lit {
            Literal::BinaryView { value: Some(data) } if data == binary_data => {}
            _ => panic!("Expected binary view literal"),
        }
    }

    #[test]
    fn test_string_literals() {
        let test_string = "Hello, World!".to_string();

        let utf8_lit = Literal::Utf8 {
            value: Some(test_string.clone()),
        };

        let large_utf8_lit = Literal::LargeUtf8 {
            value: Some(test_string.clone()),
        };

        let utf8_view_lit = Literal::Utf8View {
            value: Some(test_string.clone()),
        };

        // Test null string literals
        let null_utf8 = Literal::Utf8 { value: None };
        let null_large_utf8 = Literal::LargeUtf8 { value: None };
        let null_utf8_view = Literal::Utf8View { value: None };

        // Verify pattern matching
        match utf8_lit {
            Literal::Utf8 { value: Some(s) } if s == test_string => {}
            _ => panic!("Expected UTF-8 literal"),
        }

        match large_utf8_lit {
            Literal::LargeUtf8 { value: Some(s) } if s == test_string => {}
            _ => panic!("Expected large UTF-8 literal"),
        }

        match utf8_view_lit {
            Literal::Utf8View { value: Some(s) } if s == test_string => {}
            _ => panic!("Expected UTF-8 view literal"),
        }

        match null_utf8 {
            Literal::Utf8 { value: None } => {}
            _ => panic!("Expected null UTF-8 literal"),
        }

        match null_large_utf8 {
            Literal::LargeUtf8 { value: None } => {}
            _ => panic!("Expected null large UTF-8 literal"),
        }

        match null_utf8_view {
            Literal::Utf8View { value: None } => {}
            _ => panic!("Expected null UTF-8 view literal"),
        }
    }

    #[test]
    fn test_decimal_literals() {
        // Test Decimal128
        let decimal128_lit = Literal::Decimal128 {
            precision: 10,
            scale: 2,
            value: Some(12345),
        };

        let null_decimal128 = Literal::Decimal128 {
            precision: 38,
            scale: 10,
            value: None,
        };

        // Test Decimal256
        let decimal256_lit = Literal::Decimal256 {
            precision: 50,
            scale: 5,
            value: Some(i256::from_i128(123456789)),
        };

        let null_decimal256 = Literal::Decimal256 {
            precision: 76,
            scale: 20,
            value: None,
        };

        // Verify pattern matching
        match decimal128_lit {
            Literal::Decimal128 {
                precision: 10,
                scale: 2,
                value: Some(12345),
            } => {}
            _ => panic!("Expected Decimal128 literal"),
        }

        match null_decimal128 {
            Literal::Decimal128 {
                precision: 38,
                scale: 10,
                value: None,
            } => {}
            _ => panic!("Expected null Decimal128 literal"),
        }

        match decimal256_lit {
            Literal::Decimal256 {
                precision: 50,
                scale: 5,
                value: Some(_),
            } => {}
            _ => panic!("Expected Decimal256 literal"),
        }

        match null_decimal256 {
            Literal::Decimal256 {
                precision: 76,
                scale: 20,
                value: None,
            } => {}
            _ => panic!("Expected null Decimal256 literal"),
        }
    }

    #[test]
    fn test_complex_literals() {
        // Create a simple data type for testing
        let element_type = types_serializable::DataType {
            kind: types_serializable::DataTypeKind::Primitive(PrimitiveType::Int32),
            metadata: NoMetadata,
        };

        // Test List literal
        let list_lit = Literal::List {
            data_type: Box::new(element_type.clone()),
            values: Some(vec![
                Literal::Int32 { value: Some(1) },
                Literal::Int32 { value: Some(2) },
                Literal::Int32 { value: Some(3) },
            ]),
        };

        // Test FixedSizeList literal
        let fixed_list_lit = Literal::FixedSizeList {
            length: 3,
            data_type: Box::new(element_type.clone()),
            values: Some(vec![
                Literal::Int32 { value: Some(10) },
                Literal::Int32 { value: Some(20) },
                Literal::Int32 { value: Some(30) },
            ]),
        };

        // Test LargeList literal
        let large_list_lit = Literal::LargeList {
            data_type: Box::new(element_type.clone()),
            values: Some(vec![
                Literal::Int32 { value: Some(100) },
                Literal::Int32 { value: Some(200) },
            ]),
        };

        // Test null list
        let null_list = Literal::List {
            data_type: Box::new(element_type),
            values: None,
        };

        // Verify pattern matching
        match list_lit {
            Literal::List {
                data_type: _,
                values: Some(vals),
            } if vals.len() == 3 => {}
            _ => panic!("Expected list literal with 3 elements"),
        }

        match fixed_list_lit {
            Literal::FixedSizeList {
                length: 3,
                data_type: _,
                values: Some(vals),
            } if vals.len() == 3 => {}
            _ => panic!("Expected fixed size list literal"),
        }

        match large_list_lit {
            Literal::LargeList {
                data_type: _,
                values: Some(vals),
            } if vals.len() == 2 => {}
            _ => panic!("Expected large list literal with 2 elements"),
        }

        match null_list {
            Literal::List {
                data_type: _,
                values: None,
            } => {}
            _ => panic!("Expected null list literal"),
        }
    }

    #[test]
    fn test_struct_literal() {
        // Create a struct data type
        let struct_type = types_serializable::DataType {
            kind: types_serializable::DataTypeKind::Struct {
                fields: types_serializable::Fields::new(vec![
                    types_serializable::Field {
                        name: "id".to_string(),
                        data_type: types_serializable::DataType {
                            kind: types_serializable::DataTypeKind::Primitive(PrimitiveType::Int32),
                            metadata: NoMetadata,
                        },
                        nullable: false,
                        metadata: vec![],
                    },
                    types_serializable::Field {
                        name: "name".to_string(),
                        data_type: types_serializable::DataType {
                            kind: types_serializable::DataTypeKind::Primitive(PrimitiveType::Utf8),
                            metadata: NoMetadata,
                        },
                        nullable: true,
                        metadata: vec![],
                    },
                ]),
            },
            metadata: NoMetadata,
        };

        let struct_lit = Literal::Struct {
            data_type: struct_type,
            values: Some(vec![
                Literal::Int32 { value: Some(42) },
                Literal::Utf8 {
                    value: Some("test".to_string()),
                },
            ]),
        };

        // Verify pattern matching
        match struct_lit {
            Literal::Struct {
                data_type: _,
                values: Some(vals),
            } if vals.len() == 2 => {}
            _ => panic!("Expected struct literal with 2 fields"),
        }
    }

    #[test]
    fn test_map_literal() {
        let key_type = types_serializable::DataType {
            kind: types_serializable::DataTypeKind::Primitive(PrimitiveType::Utf8),
            metadata: NoMetadata,
        };

        let value_type = types_serializable::DataType {
            kind: types_serializable::DataTypeKind::Primitive(PrimitiveType::Int32),
            metadata: NoMetadata,
        };

        let map_lit = Literal::Map {
            key_type: Box::new(key_type),
            value_type: Box::new(value_type),
            keys: Some(vec![
                Literal::Utf8 {
                    value: Some("key1".to_string()),
                },
                Literal::Utf8 {
                    value: Some("key2".to_string()),
                },
            ]),
            values: Some(vec![
                Literal::Int32 { value: Some(10) },
                Literal::Int32 { value: Some(20) },
            ]),
        };

        // Test null map
        let null_map = Literal::Map {
            key_type: Box::new(types_serializable::DataType {
                kind: types_serializable::DataTypeKind::Primitive(PrimitiveType::Utf8),
                metadata: NoMetadata,
            }),
            value_type: Box::new(types_serializable::DataType {
                kind: types_serializable::DataTypeKind::Primitive(PrimitiveType::Int32),
                metadata: NoMetadata,
            }),
            keys: None,
            values: None,
        };

        // Verify pattern matching
        match map_lit {
            Literal::Map {
                key_type: _,
                value_type: _,
                keys: Some(k),
                values: Some(v),
            } if k.len() == 2 && v.len() == 2 => {}
            _ => panic!("Expected map literal with 2 key-value pairs"),
        }

        match null_map {
            Literal::Map {
                key_type: _,
                value_type: _,
                keys: None,
                values: None,
            } => {}
            _ => panic!("Expected null map literal"),
        }
    }

    #[test]
    fn test_data_type_to_null_literal_primitives() {
        // Test all primitive types
        let null_type = DataType::new(DataTypeKind::Primitive(PrimitiveType::Null));
        let result = data_type_to_null_literal(&null_type).unwrap();
        assert_eq!(result, Literal::Null);

        let bool_type = DataType::new(DataTypeKind::Primitive(PrimitiveType::Boolean));
        let result = data_type_to_null_literal(&bool_type).unwrap();
        assert_eq!(result, Literal::Boolean { value: None });

        let int32_type = DataType::new(DataTypeKind::Primitive(PrimitiveType::Int32));
        let result = data_type_to_null_literal(&int32_type).unwrap();
        assert_eq!(result, Literal::Int32 { value: None });

        let utf8_type = DataType::new(DataTypeKind::Primitive(PrimitiveType::Utf8));
        let result = data_type_to_null_literal(&utf8_type).unwrap();
        assert_eq!(result, Literal::Utf8 { value: None });

        let binary_type = DataType::new(DataTypeKind::Primitive(PrimitiveType::Binary));
        let result = data_type_to_null_literal(&binary_type).unwrap();
        assert_eq!(result, Literal::Binary { value: None });

        let date32_type = DataType::new(DataTypeKind::Primitive(PrimitiveType::Date32));
        let result = data_type_to_null_literal(&date32_type).unwrap();
        assert_eq!(result, Literal::Date32 { days: None });
    }

    #[test]
    fn test_data_type_to_null_literal_temporal() {
        // Test timestamp types
        let ts_sec_type = DataType::new(DataTypeKind::Timestamp {
            time_unit: TimeUnit::Second,
            timestamp_type: TimestampType::WithoutTimeZone,
        });
        let result = data_type_to_null_literal(&ts_sec_type).unwrap();
        match result {
            Literal::TimestampSecond {
                seconds: None,
                timestamp_type: TimestampType::WithoutTimeZone,
            } => {}
            _ => panic!("Expected null timestamp second literal"),
        }

        let ts_ms_type = DataType::new(DataTypeKind::Timestamp {
            time_unit: TimeUnit::Millisecond,
            timestamp_type: TimestampType::WithLocalTimeZone,
        });
        let result = data_type_to_null_literal(&ts_ms_type).unwrap();
        match result {
            Literal::TimestampMillisecond {
                milliseconds: None,
                timestamp_type: TimestampType::WithLocalTimeZone,
            } => {}
            _ => panic!("Expected null timestamp millisecond literal"),
        }

        // Test time types
        let time32_sec_type = DataType::new(DataTypeKind::Time32 {
            time_unit: TimeUnit::Second,
        });
        let result = data_type_to_null_literal(&time32_sec_type).unwrap();
        assert_eq!(result, Literal::Time32Second { seconds: None });

        let time64_us_type = DataType::new(DataTypeKind::Time64 {
            time_unit: TimeUnit::Microsecond,
        });
        let result = data_type_to_null_literal(&time64_us_type).unwrap();
        assert_eq!(result, Literal::Time64Microsecond { microseconds: None });

        // Test duration types
        let dur_ns_type = DataType::new(DataTypeKind::Duration {
            time_unit: TimeUnit::Nanosecond,
        });
        let result = data_type_to_null_literal(&dur_ns_type).unwrap();
        assert_eq!(result, Literal::DurationNanosecond { nanoseconds: None });
    }

    #[test]
    fn test_data_type_to_null_literal_intervals() {
        // Test interval year-month
        let interval_ym_type = DataType::new(DataTypeKind::Interval {
            interval_unit: IntervalUnit::YearMonth,
            start_field: None,
            end_field: None,
        });
        let result = data_type_to_null_literal(&interval_ym_type).unwrap();
        assert_eq!(result, Literal::IntervalYearMonth { months: None });

        // Test interval day-time
        let interval_dt_type = DataType::new(DataTypeKind::Interval {
            interval_unit: IntervalUnit::DayTime,
            start_field: None,
            end_field: None,
        });
        let result = data_type_to_null_literal(&interval_dt_type).unwrap();
        assert_eq!(result, Literal::IntervalDayTime { value: None });

        // Test interval month-day-nano
        let interval_mdn_type = DataType::new(DataTypeKind::Interval {
            interval_unit: IntervalUnit::MonthDayNano,
            start_field: None,
            end_field: None,
        });
        let result = data_type_to_null_literal(&interval_mdn_type).unwrap();
        assert_eq!(result, Literal::IntervalMonthDayNano { value: None });
    }

    #[test]
    fn test_data_type_to_null_literal_complex() {
        // Test fixed size binary
        let fixed_binary_type = DataType::new(DataTypeKind::FixedSizeBinary { size: 16 });
        let result = data_type_to_null_literal(&fixed_binary_type).unwrap();
        assert_eq!(
            result,
            Literal::FixedSizeBinary {
                size: 16,
                value: None
            }
        );

        // Test decimal types
        let decimal128_type = DataType::new(DataTypeKind::Decimal128 {
            precision: 10,
            scale: 2,
        });
        let result = data_type_to_null_literal(&decimal128_type).unwrap();
        assert_eq!(
            result,
            Literal::Decimal128 {
                precision: 10,
                scale: 2,
                value: None
            }
        );

        let decimal256_type = DataType::new(DataTypeKind::Decimal256 {
            precision: 50,
            scale: 5,
        });
        let result = data_type_to_null_literal(&decimal256_type).unwrap();
        assert_eq!(
            result,
            Literal::Decimal256 {
                precision: 50,
                scale: 5,
                value: None
            }
        );

        // Test list type
        let element_type = DataType::new(DataTypeKind::Primitive(PrimitiveType::Int32));
        let list_type = DataType::new(DataTypeKind::List {
            data_type: Box::new(element_type),
            nullable: true,
        });
        let result = data_type_to_null_literal(&list_type).unwrap();
        match result {
            Literal::List {
                data_type: _,
                values: None,
            } => {}
            _ => panic!("Expected null list literal"),
        }
    }

    #[test]
    fn test_data_type_to_null_literal_errors() {
        // Test invalid time32 with microsecond (should error)
        let invalid_time32 = DataType::new(DataTypeKind::Time32 {
            time_unit: TimeUnit::Microsecond,
        });
        assert!(data_type_to_null_literal(&invalid_time32).is_err());

        // Test invalid time32 with nanosecond (should error)
        let invalid_time32_ns = DataType::new(DataTypeKind::Time32 {
            time_unit: TimeUnit::Nanosecond,
        });
        assert!(data_type_to_null_literal(&invalid_time32_ns).is_err());

        // Test invalid time64 with second (should error)
        let invalid_time64_sec = DataType::new(DataTypeKind::Time64 {
            time_unit: TimeUnit::Second,
        });
        assert!(data_type_to_null_literal(&invalid_time64_sec).is_err());

        // Test invalid time64 with millisecond (should error)
        let invalid_time64_ms = DataType::new(DataTypeKind::Time64 {
            time_unit: TimeUnit::Millisecond,
        });
        assert!(data_type_to_null_literal(&invalid_time64_ms).is_err());
    }

    #[test]
    fn test_serialization_functions() {
        // Test that the custom serialization functions work with Decimal literals
        // The serialize_optional and deserialize_optional functions are used internally
        // by the Decimal128 and Decimal256 literals

        // Test Decimal128 with custom serialization
        let decimal_lit = Literal::Decimal128 {
            precision: 10,
            scale: 2,
            value: Some(12345),
        };

        // This should serialize the value as a string due to the custom serializer
        let json = serde_json::to_string(&decimal_lit).unwrap();
        assert!(json.contains("\"12345\""));

        // And deserialize back correctly
        let deserialized: Literal = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, decimal_lit);

        // Test null decimal
        let null_decimal = Literal::Decimal128 {
            precision: 10,
            scale: 2,
            value: None,
        };

        let json = serde_json::to_string(&null_decimal).unwrap();
        let deserialized: Literal = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, null_decimal);
    }

    #[test]
    fn test_literal_serialization() {
        // Test basic literal serialization
        let int_lit = Literal::Int32 { value: Some(42) };
        let json = serde_json::to_string(&int_lit).expect("Failed to serialize literal");
        assert!(json.contains("42"));
        assert!(json.contains("int32"));

        // Test deserialization
        let deserialized: Literal =
            serde_json::from_str(&json).expect("Failed to deserialize literal");
        assert_eq!(deserialized, int_lit);

        // Test null literal
        let null_lit = Literal::Null;
        let json = serde_json::to_string(&null_lit).expect("Failed to serialize null literal");
        let deserialized: Literal =
            serde_json::from_str(&json).expect("Failed to deserialize null literal");
        assert_eq!(deserialized, null_lit);

        // Test string literal
        let string_lit = Literal::Utf8 {
            value: Some("hello world".to_string()),
        };
        let json = serde_json::to_string(&string_lit).expect("Failed to serialize string literal");
        let deserialized: Literal =
            serde_json::from_str(&json).expect("Failed to deserialize string literal");
        assert_eq!(deserialized, string_lit);
    }

    #[test]
    fn test_decimal_serialization() {
        // Test Decimal128 serialization with custom serializer
        let decimal_lit = Literal::Decimal128 {
            precision: 10,
            scale: 2,
            value: Some(12345),
        };

        let json = serde_json::to_string(&decimal_lit).expect("Failed to serialize decimal128");
        assert!(json.contains("\"12345\"")); // Should be serialized as string

        let deserialized: Literal =
            serde_json::from_str(&json).expect("Failed to deserialize decimal128");
        assert_eq!(deserialized, decimal_lit);

        // Test Decimal256 serialization
        let decimal256_lit = Literal::Decimal256 {
            precision: 50,
            scale: 5,
            value: Some(i256::from_i128(98765)),
        };

        let json = serde_json::to_string(&decimal256_lit).expect("Failed to serialize decimal256");
        let deserialized: Literal =
            serde_json::from_str(&json).expect("Failed to deserialize decimal256");
        assert_eq!(deserialized, decimal256_lit);
    }

    #[test]
    fn test_interval_structs() {
        // Test IntervalDayTime
        let interval_dt = IntervalDayTime {
            days: 5,
            milliseconds: 3600000,
        };

        assert_eq!(interval_dt.days, 5);
        assert_eq!(interval_dt.milliseconds, 3600000);

        // Test serialization
        let json =
            serde_json::to_string(&interval_dt).expect("Failed to serialize IntervalDayTime");
        let deserialized: IntervalDayTime =
            serde_json::from_str(&json).expect("Failed to deserialize IntervalDayTime");
        assert_eq!(deserialized, interval_dt);

        // Test IntervalMonthDayNano
        let interval_mdn = IntervalMonthDayNano {
            months: 2,
            days: 15,
            nanoseconds: 3600000000000,
        };

        assert_eq!(interval_mdn.months, 2);
        assert_eq!(interval_mdn.days, 15);
        assert_eq!(interval_mdn.nanoseconds, 3600000000000);

        // Test serialization
        let json =
            serde_json::to_string(&interval_mdn).expect("Failed to serialize IntervalMonthDayNano");
        let deserialized: IntervalMonthDayNano =
            serde_json::from_str(&json).expect("Failed to deserialize IntervalMonthDayNano");
        assert_eq!(deserialized, interval_mdn);
    }

    #[test]
    fn test_literal_equality() {
        // Test that identical literals are equal
        let lit1 = Literal::Int32 { value: Some(42) };
        let lit2 = Literal::Int32 { value: Some(42) };
        assert_eq!(lit1, lit2);

        // Test that different literals are not equal
        let lit3 = Literal::Int32 { value: Some(43) };
        assert_ne!(lit1, lit3);

        // Test null vs non-null
        let null_lit = Literal::Int32 { value: None };
        assert_ne!(lit1, null_lit);

        // Test different types
        let float_lit = Literal::Float32 { value: Some(42.0) };
        assert_ne!(lit1, float_lit);
    }

    #[test]
    fn test_literal_debug() {
        // Test that Debug trait works for all literal types
        let literals = vec![
            Literal::Null,
            Literal::Boolean { value: Some(true) },
            Literal::Int32 { value: Some(42) },
            Literal::Utf8 {
                value: Some("test".to_string()),
            },
            Literal::Binary {
                value: Some(vec![1, 2, 3]),
            },
            Literal::Date32 { days: Some(18628) },
            Literal::IntervalYearMonth { months: Some(12) },
        ];

        for literal in literals {
            let debug_str = format!("{:?}", literal);
            assert!(!debug_str.is_empty());
        }
    }

    #[test]
    fn test_union_literal() {
        // Create union fields for testing
        let union_fields = types_serializable::UnionFields::new(vec![
            (
                0,
                types_serializable::Field {
                    name: "int_field".to_string(),
                    data_type: types_serializable::DataType {
                        kind: types_serializable::DataTypeKind::Primitive(PrimitiveType::Int32),
                        metadata: NoMetadata,
                    },
                    nullable: false,
                    metadata: vec![],
                },
            ),
            (
                1,
                types_serializable::Field {
                    name: "string_field".to_string(),
                    data_type: types_serializable::DataType {
                        kind: types_serializable::DataTypeKind::Primitive(PrimitiveType::Utf8),
                        metadata: NoMetadata,
                    },
                    nullable: true,
                    metadata: vec![],
                },
            ),
        ]);

        // Test sparse union
        let union_lit = Literal::Union {
            union_fields: union_fields.clone(),
            union_mode: types::UnionMode::Sparse,
            value: Some((0, Box::new(Literal::Int32 { value: Some(42) }))),
        };

        // Test dense union
        let dense_union_lit = Literal::Union {
            union_fields: union_fields.clone(),
            union_mode: types::UnionMode::Dense,
            value: Some((
                1,
                Box::new(Literal::Utf8 {
                    value: Some("test".to_string()),
                }),
            )),
        };

        // Test null union
        let null_union = Literal::Union {
            union_fields,
            union_mode: types::UnionMode::Sparse,
            value: None,
        };

        // Verify pattern matching
        match union_lit {
            Literal::Union {
                union_fields: _,
                union_mode: types::UnionMode::Sparse,
                value: Some((0, boxed_val)),
            } => match *boxed_val {
                Literal::Int32 { value: Some(42) } => {}
                _ => panic!("Expected int32 value in union"),
            },
            _ => panic!("Expected sparse union literal"),
        }

        match dense_union_lit {
            Literal::Union {
                union_fields: _,
                union_mode: types::UnionMode::Dense,
                value: Some((1, boxed_val)),
            } => match *boxed_val {
                Literal::Utf8 { value: Some(s) } if s == "test" => {}
                _ => panic!("Expected string value in union"),
            },
            _ => panic!("Expected dense union literal"),
        }

        match null_union {
            Literal::Union {
                union_fields: _,
                union_mode: types::UnionMode::Sparse,
                value: None,
            } => {}
            _ => panic!("Expected null union literal"),
        }
    }

    #[test]
    fn test_dictionary_literal() {
        let key_type = types_serializable::DataType {
            kind: types_serializable::DataTypeKind::Primitive(PrimitiveType::Int32),
            metadata: NoMetadata,
        };

        let value_type = types_serializable::DataType {
            kind: types_serializable::DataTypeKind::Primitive(PrimitiveType::Utf8),
            metadata: NoMetadata,
        };

        // Test dictionary with value
        let dict_lit = Literal::Dictionary {
            key_type: Box::new(key_type.clone()),
            value_type: Box::new(value_type.clone()),
            value: Some(Box::new(Literal::Utf8 {
                value: Some("dictionary_value".to_string()),
            })),
        };

        // Test null dictionary
        let null_dict = Literal::Dictionary {
            key_type: Box::new(key_type),
            value_type: Box::new(value_type),
            value: None,
        };

        // Verify pattern matching
        match dict_lit {
            Literal::Dictionary {
                key_type: _,
                value_type: _,
                value: Some(boxed_val),
            } => match *boxed_val {
                Literal::Utf8 { value: Some(s) } if s == "dictionary_value" => {}
                _ => panic!("Expected string value in dictionary"),
            },
            _ => panic!("Expected dictionary literal with value"),
        }

        match null_dict {
            Literal::Dictionary {
                key_type: _,
                value_type: _,
                value: None,
            } => {}
            _ => panic!("Expected null dictionary literal"),
        }
    }

    #[test]
    fn test_data_type_to_null_literal_configured_utf8() {
        // Test ConfiguredUtf8 type
        let configured_utf8_type = DataType::new(DataTypeKind::ConfiguredUtf8 {
            utf8_type: types::Utf8Type::Configured,
        });
        let result = data_type_to_null_literal(&configured_utf8_type).unwrap();
        assert_eq!(result, Literal::Utf8 { value: None });
    }

    #[test]
    fn test_data_type_to_null_literal_user_defined() {
        // Test UserDefined type that recursively calls data_type_to_null_literal
        let inner_type = DataType::new(DataTypeKind::Primitive(PrimitiveType::Int32));
        let user_defined_type = DataType::new(DataTypeKind::UserDefined {
            name: "test_udt".to_string(),
            jvm_class: None,
            python_class: None,
            serialized_python_class: None,
            sql_type: Box::new(inner_type),
        });
        let result = data_type_to_null_literal(&user_defined_type).unwrap();
        assert_eq!(result, Literal::Int32 { value: None });
    }

    #[test]
    fn test_data_type_to_null_literal_configured_binary() {
        // Test ConfiguredBinary type
        let configured_binary_type =
            DataType::new(DataTypeKind::Primitive(PrimitiveType::ConfiguredBinary));
        let result = data_type_to_null_literal(&configured_binary_type).unwrap();
        assert_eq!(result, Literal::Binary { value: None });
    }

    #[test]
    fn test_serialization_edge_cases() {
        // Test serialization with invalid string for deserialization
        let invalid_json =
            r#"{"decimal128": {"precision": 10, "scale": 2, "value": "invalid_number"}}"#;
        let result: std::result::Result<Literal, _> = serde_json::from_str(invalid_json);
        assert!(result.is_err());

        // Test serialization of null decimal values
        let null_decimal128 = Literal::Decimal128 {
            precision: 10,
            scale: 2,
            value: None,
        };
        let json = serde_json::to_string(&null_decimal128).unwrap();
        let deserialized: Literal = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, null_decimal128);

        // Test serialization of null decimal256 values
        let null_decimal256 = Literal::Decimal256 {
            precision: 50,
            scale: 5,
            value: None,
        };
        let json = serde_json::to_string(&null_decimal256).unwrap();
        let deserialized: Literal = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, null_decimal256);
    }

    #[test]
    fn test_literal_clone() {
        // Test that all literal types can be cloned
        let original = Literal::Struct {
            data_type: types_serializable::DataType {
                kind: types_serializable::DataTypeKind::Struct {
                    fields: types_serializable::Fields::new(vec![types_serializable::Field {
                        name: "test_field".to_string(),
                        data_type: types_serializable::DataType {
                            kind: types_serializable::DataTypeKind::Primitive(PrimitiveType::Int32),
                            metadata: NoMetadata,
                        },
                        nullable: true,
                        metadata: vec![],
                    }]),
                },
                metadata: NoMetadata,
            },
            values: Some(vec![Literal::Int32 { value: Some(42) }]),
        };

        let cloned = original.clone();
        assert_eq!(original, cloned);
    }

    #[test]
    fn test_literal_partial_eq() {
        // Test PartialEq implementation for complex types
        let lit1 = Literal::Map {
            key_type: Box::new(types_serializable::DataType {
                kind: types_serializable::DataTypeKind::Primitive(PrimitiveType::Utf8),
                metadata: NoMetadata,
            }),
            value_type: Box::new(types_serializable::DataType {
                kind: types_serializable::DataTypeKind::Primitive(PrimitiveType::Int32),
                metadata: NoMetadata,
            }),
            keys: Some(vec![Literal::Utf8 {
                value: Some("key1".to_string()),
            }]),
            values: Some(vec![Literal::Int32 { value: Some(100) }]),
        };

        let lit2 = Literal::Map {
            key_type: Box::new(types_serializable::DataType {
                kind: types_serializable::DataTypeKind::Primitive(PrimitiveType::Utf8),
                metadata: NoMetadata,
            }),
            value_type: Box::new(types_serializable::DataType {
                kind: types_serializable::DataTypeKind::Primitive(PrimitiveType::Int32),
                metadata: NoMetadata,
            }),
            keys: Some(vec![Literal::Utf8 {
                value: Some("key1".to_string()),
            }]),
            values: Some(vec![Literal::Int32 { value: Some(100) }]),
        };

        let lit3 = Literal::Map {
            key_type: Box::new(types_serializable::DataType {
                kind: types_serializable::DataTypeKind::Primitive(PrimitiveType::Utf8),
                metadata: NoMetadata,
            }),
            value_type: Box::new(types_serializable::DataType {
                kind: types_serializable::DataTypeKind::Primitive(PrimitiveType::Int32),
                metadata: NoMetadata,
            }),
            keys: Some(vec![Literal::Utf8 {
                value: Some("key2".to_string()),
            }]),
            values: Some(vec![Literal::Int32 { value: Some(200) }]),
        };

        assert_eq!(lit1, lit2);
        assert_ne!(lit1, lit3);
    }

    #[test]
    fn test_interval_struct_edge_cases() {
        // Test IntervalDayTime with extreme values
        let extreme_interval = IntervalDayTime {
            days: i32::MAX,
            milliseconds: i32::MIN,
        };

        let json = serde_json::to_string(&extreme_interval).unwrap();
        let deserialized: IntervalDayTime = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, extreme_interval);

        // Test IntervalMonthDayNano with extreme values
        let extreme_interval_mdn = IntervalMonthDayNano {
            months: i32::MAX,
            days: i32::MIN,
            nanoseconds: i64::MAX,
        };

        let json = serde_json::to_string(&extreme_interval_mdn).unwrap();
        let deserialized: IntervalMonthDayNano = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, extreme_interval_mdn);
    }

    #[test]
    fn test_data_type_to_null_literal_all_primitives() {
        // Test all primitive types to ensure complete coverage
        let primitive_types = vec![
            PrimitiveType::Null,
            PrimitiveType::Boolean,
            PrimitiveType::Int8,
            PrimitiveType::Int16,
            PrimitiveType::Int32,
            PrimitiveType::Int64,
            PrimitiveType::UInt8,
            PrimitiveType::UInt16,
            PrimitiveType::UInt32,
            PrimitiveType::UInt64,
            PrimitiveType::Float16,
            PrimitiveType::Float32,
            PrimitiveType::Float64,
            PrimitiveType::Date32,
            PrimitiveType::Date64,
            PrimitiveType::Binary,
            PrimitiveType::LargeBinary,
            PrimitiveType::BinaryView,
            PrimitiveType::Utf8,
            PrimitiveType::LargeUtf8,
            PrimitiveType::Utf8View,
            PrimitiveType::ConfiguredBinary,
        ];

        for primitive_type in primitive_types {
            let data_type = DataType::new(DataTypeKind::Primitive(primitive_type.clone()));
            let result = data_type_to_null_literal(&data_type);
            assert!(
                result.is_ok(),
                "Failed for primitive type: {:?}",
                primitive_type
            );
        }
    }
}
