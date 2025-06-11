//! Data type system for Barks Core
//!
//! Generic type system supporting metadata attachment
//! Based on Sail "https://github.com/lakehq/sail"

use std::fmt::Display;
use std::ops::Deref;
use std::sync::Arc;

use barks_common::error::CommonError;
use num_enum::TryFromPrimitive;
use serde::{Deserialize, Serialize};

/// Maximum precision for 128-bit decimal values
pub const ARROW_DECIMAL128_MAX_PRECISION: u8 = 38;
/// Maximum scale for 128-bit decimal values
pub const ARROW_DECIMAL128_MAX_SCALE: i8 = 38;
/// Maximum precision for 256-bit decimal values
pub const ARROW_DECIMAL256_MAX_PRECISION: u8 = 76;
/// Maximum scale for 256-bit decimal values
pub const ARROW_DECIMAL256_MAX_SCALE: i8 = 76;

// --- Type System Core ---

/// Trait for metadata attachable to data type nodes
pub trait DataTypeMetadata:
    std::fmt::Debug + Clone + Send + Sync + Serialize + serde::de::DeserializeOwned
{
}

/// Empty metadata for cases where no additional data is needed
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NoMetadata;
impl DataTypeMetadata for NoMetadata {}

/// Primary data type structure containing type definition and metadata
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DataType<M: DataTypeMetadata = NoMetadata> {
    /// Core data type definition
    pub kind: DataTypeKind<M>,
    /// Attached metadata
    pub metadata: M,
}

impl DataType<NoMetadata> {
    /// Create new data type without metadata
    pub fn new(kind: DataTypeKind<NoMetadata>) -> Self {
        Self {
            kind,
            metadata: NoMetadata,
        }
    }
}

/// Primitive data types without parameters
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PrimitiveType {
    Null,
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float16,
    Float32,
    Float64,
    Date32,
    Date64,
    Binary,
    LargeBinary,
    BinaryView,
    Utf8,
    LargeUtf8,
    Utf8View,
    ConfiguredBinary,
}

/// Core data type variants with optional metadata
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum DataTypeKind<M: DataTypeMetadata = NoMetadata> {
    /// Primitive data types without parameters
    Primitive(PrimitiveType),
    /// Timestamp with optional time zone
    Timestamp {
        time_unit: TimeUnit,
        timestamp_type: TimestampType,
    },
    /// 32-bit time since midnight
    Time32 { time_unit: TimeUnit },
    /// 64-bit time since midnight
    Time64 { time_unit: TimeUnit },
    /// Elapsed time duration
    Duration { time_unit: TimeUnit },
    /// Calendar interval
    Interval {
        interval_unit: IntervalUnit,
        start_field: Option<IntervalFieldType>,
        end_field: Option<IntervalFieldType>,
    },
    /// Fixed-size binary data
    FixedSizeBinary { size: i32 },
    /// Variable-length list
    List {
        data_type: Box<DataType<M>>,
        nullable: bool,
    },
    /// Fixed-length list
    FixedSizeList {
        data_type: Box<DataType<M>>,
        nullable: bool,
        length: i32,
    },
    /// Large variable-length list with 64-bit offsets
    LargeList {
        data_type: Box<DataType<M>>,
        nullable: bool,
    },
    /// Nested structure with sub-fields
    Struct { fields: Fields<M> },
    /// Union of different types
    Union {
        union_fields: UnionFields<M>,
        union_mode: UnionMode,
    },
    /// Dictionary-encoded array
    Dictionary {
        key_type: Box<DataType<M>>,
        value_type: Box<DataType<M>>,
    },
    /// 128-bit decimal value
    Decimal128 { precision: u8, scale: i8 },
    /// 256-bit decimal value
    Decimal256 { precision: u8, scale: i8 },
    /// Map logical type
    Map {
        key_type: Box<DataType<M>>,
        value_type: Box<DataType<M>>,
        value_type_nullable: bool,
        keys_sorted: bool,
    },
    /// User-defined type with language bindings
    UserDefined {
        /// e.g. "udt"
        name: String,
        jvm_class: Option<String>,
        python_class: Option<String>,
        serialized_python_class: Option<String>,
        sql_type: Box<DataType<M>>,
    },
    /// Configuration-based string type
    ConfiguredUtf8 { utf8_type: Utf8Type },
}

impl<M: DataTypeMetadata + Default> DataType<M> {
    pub fn into_schema(self, default_field_name: &str, nullable: bool) -> Schema<M> {
        let fields = match self.kind {
            DataTypeKind::Struct { fields } => fields,
            _ => Fields::from(vec![Field {
                name: default_field_name.to_string(),
                data_type: self,
                nullable,
                metadata: vec![],
            }]),
        };
        Schema { fields }
    }
}

// --- Supporting Structs and Enums (Now Generic) ---

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub struct Field<M: DataTypeMetadata = NoMetadata> {
    pub name: String,
    pub data_type: DataType<M>,
    pub nullable: bool,
    pub metadata: Vec<(String, String)>,
}

pub type FieldRef<M> = Arc<Field<M>>;

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub struct Fields<M: DataTypeMetadata = NoMetadata>(Arc<[FieldRef<M>]>);

impl<M: DataTypeMetadata> Fields<M> {
    pub fn empty() -> Self {
        Self(Arc::new([]))
    }
}

impl<M: DataTypeMetadata> Default for Fields<M> {
    fn default() -> Self {
        Self::empty()
    }
}

impl<M: DataTypeMetadata> FromIterator<Field<M>> for Fields<M> {
    fn from_iter<T: IntoIterator<Item = Field<M>>>(iter: T) -> Self {
        iter.into_iter().map(Arc::new).collect()
    }
}

impl<M: DataTypeMetadata> FromIterator<FieldRef<M>> for Fields<M> {
    fn from_iter<T: IntoIterator<Item = FieldRef<M>>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl<M: DataTypeMetadata> From<Vec<Field<M>>> for Fields<M> {
    fn from(fields: Vec<Field<M>>) -> Self {
        fields.into_iter().collect()
    }
}

impl<M: DataTypeMetadata> From<Vec<FieldRef<M>>> for Fields<M> {
    fn from(fields: Vec<FieldRef<M>>) -> Self {
        Self(fields.into())
    }
}

impl<M: DataTypeMetadata> From<&[FieldRef<M>]> for Fields<M> {
    fn from(fields: &[FieldRef<M>]) -> Self {
        Self(fields.into())
    }
}

impl<M: DataTypeMetadata, const N: usize> From<[FieldRef<M>; N]> for Fields<M> {
    fn from(fields: [FieldRef<M>; N]) -> Self {
        Self(Arc::new(fields))
    }
}

impl<M: DataTypeMetadata> Deref for Fields<M> {
    type Target = [FieldRef<M>];

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl<'a, M: DataTypeMetadata> IntoIterator for &'a Fields<M> {
    type Item = &'a FieldRef<M>;
    type IntoIter = std::slice::Iter<'a, FieldRef<M>>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub struct UnionFields<M: DataTypeMetadata = NoMetadata>(Arc<[(i8, FieldRef<M>)]>);

impl<M: DataTypeMetadata> UnionFields<M> {
    pub fn empty() -> Self {
        Self(Arc::from([]))
    }
}

impl<M: DataTypeMetadata> Deref for UnionFields<M> {
    type Target = [(i8, FieldRef<M>)];

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl<M: DataTypeMetadata> FromIterator<(i8, FieldRef<M>)> for UnionFields<M> {
    fn from_iter<T: IntoIterator<Item = (i8, FieldRef<M>)>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

#[derive(Default, Debug, Clone, PartialEq)]
pub struct Schema<M: DataTypeMetadata = NoMetadata> {
    pub fields: Fields<M>,
}

// --- Serializable Types Module ---

/// Serializable versions of the core types for JSON/binary serialization
pub mod serializable {
    use super::*;
    use serde::{Deserialize, Serialize};

    /// Serializable version of DataType with NoMetadata
    #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct DataType {
        pub kind: DataTypeKind,
        pub metadata: NoMetadata,
    }

    /// Serializable version of DataTypeKind with NoMetadata
    #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub enum DataTypeKind {
        /// Primitive data types without parameters
        Primitive(PrimitiveType),
        /// Timestamp with optional time zone
        Timestamp {
            time_unit: TimeUnit,
            timestamp_type: TimestampType,
        },
        /// Time type (32-bit)
        Time32 { time_unit: TimeUnit },
        /// Time type (64-bit)
        Time64 { time_unit: TimeUnit },
        /// Duration type
        Duration { time_unit: TimeUnit },
        /// Interval type
        Interval {
            interval_unit: IntervalUnit,
            start_field: Option<IntervalFieldType>,
            end_field: Option<IntervalFieldType>,
        },
        /// Binary data with fixed size
        FixedSizeBinary { size: i32 },
        /// Variable-length list
        List {
            data_type: Box<DataType>,
            nullable: bool,
        },
        /// Fixed-length list
        FixedSizeList {
            data_type: Box<DataType>,
            nullable: bool,
            length: i32,
        },
        /// Large variable-length list with 64-bit offsets
        LargeList {
            data_type: Box<DataType>,
            nullable: bool,
        },
        /// Nested structure with sub-fields
        Struct { fields: Fields },
        /// Union of different types
        Union {
            union_fields: UnionFields,
            union_mode: UnionMode,
        },
        /// Dictionary-encoded array
        Dictionary {
            key_type: Box<DataType>,
            value_type: Box<DataType>,
        },
        /// 128-bit decimal value
        Decimal128 { precision: u8, scale: i8 },
        /// 256-bit decimal value
        Decimal256 { precision: u8, scale: i8 },
        /// Map logical type
        Map {
            key_type: Box<DataType>,
            value_type: Box<DataType>,
            value_type_nullable: bool,
            keys_sorted: bool,
        },
        /// User-defined type with language bindings
        UserDefined {
            name: String,
            jvm_class: Option<String>,
            python_class: Option<String>,
            serialized_python_class: Option<String>,
            sql_type: Box<DataType>,
        },
        /// Configuration-based string type
        ConfiguredUtf8 { utf8_type: Utf8Type },
    }

    /// Serializable version of Field with NoMetadata
    #[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Field {
        pub name: String,
        pub data_type: DataType,
        pub nullable: bool,
        pub metadata: Vec<(String, String)>,
    }

    /// Serializable version of Fields with NoMetadata
    #[derive(
        Default, Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Ord, Serialize, Deserialize,
    )]
    #[serde(transparent)]
    pub struct Fields(pub Vec<Field>);

    impl Fields {
        pub fn new(fields: Vec<Field>) -> Self {
            Self(fields)
        }
    }

    impl std::ops::Deref for Fields {
        type Target = [Field];
        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    /// Serializable version of UnionFields with NoMetadata
    #[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
    #[serde(transparent)]
    pub struct UnionFields(Vec<(i8, Field)>);

    impl UnionFields {
        pub fn new(fields: Vec<(i8, Field)>) -> Self {
            Self(fields)
        }
    }

    impl std::ops::Deref for UnionFields {
        type Target = [(i8, Field)];
        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    /// Serializable version of Schema with NoMetadata
    #[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Schema {
        pub fields: Fields,
    }
}

// --- Auxiliary Enums and Structs ---

/// Timestamp type variants
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TimestampType {
    Configured,
    WithLocalTimeZone,
    WithoutTimeZone,
}

/// Union mode for union data types
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    TryFromPrimitive,
)]
#[serde(rename_all = "camelCase")]
#[num_enum(error_type(name = CommonError, constructor = UnionMode::invalid))]
#[repr(i32)]
pub enum UnionMode {
    Sparse = 0,
    Dense = 1,
}

impl UnionMode {
    fn invalid(value: i32) -> CommonError {
        CommonError::invalid(format!("invalid union mode: {value}"))
    }
}

impl Display for UnionMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UnionMode::Sparse => write!(f, "Sparse"),
            UnionMode::Dense => write!(f, "Dense"),
        }
    }
}

/// Interval unit types for calendar intervals
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    TryFromPrimitive,
)]
#[serde(rename_all = "camelCase")]
#[num_enum(error_type(name = CommonError, constructor = IntervalUnit::invalid))]
#[repr(i32)]
pub enum IntervalUnit {
    YearMonth = 0,
    DayTime = 1,
    MonthDayNano = 2,
}

impl IntervalUnit {
    fn invalid(value: i32) -> CommonError {
        CommonError::invalid(format!("invalid interval unit: {value}"))
    }
}

/// Interval field types for calendar intervals
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    TryFromPrimitive,
)]
#[serde(rename_all = "camelCase")]
#[num_enum(error_type(name = CommonError, constructor = IntervalFieldType::invalid))]
#[repr(i32)]
pub enum IntervalFieldType {
    Year = 0,
    Month = 1,
    Day = 2,
    Hour = 3,
    Minute = 4,
    Second = 5,
}

impl IntervalFieldType {
    fn invalid(value: i32) -> CommonError {
        CommonError::invalid(format!("invalid interval field type: {value}"))
    }
}

/// Time unit precision for temporal types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TimeUnit {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

/// UTF-8 string type variants
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Utf8Type {
    Configured,
    VarChar { length: u32 },
    Char { length: u32 },
}

// --- Validation and Helper Methods ---

impl DataTypeKind<NoMetadata> {
    /// Check if this data type is numeric
    pub fn is_numeric(&self) -> bool {
        match self {
            DataTypeKind::Primitive(p) => p.is_numeric(),
            DataTypeKind::Decimal128 { .. } | DataTypeKind::Decimal256 { .. } => true,
            _ => false,
        }
    }

    /// Check if this data type is temporal
    pub fn is_temporal(&self) -> bool {
        matches!(
            self,
            DataTypeKind::Timestamp { .. }
                | DataTypeKind::Time32 { .. }
                | DataTypeKind::Time64 { .. }
                | DataTypeKind::Duration { .. }
                | DataTypeKind::Interval { .. }
                | DataTypeKind::Primitive(PrimitiveType::Date32)
                | DataTypeKind::Primitive(PrimitiveType::Date64)
        )
    }

    /// Check if this data type is a string type
    pub fn is_string(&self) -> bool {
        matches!(
            self,
            DataTypeKind::Primitive(
                PrimitiveType::Utf8 | PrimitiveType::LargeUtf8 | PrimitiveType::Utf8View
            ) | DataTypeKind::ConfiguredUtf8 { .. }
        )
    }

    /// Check if this data type is binary
    pub fn is_binary(&self) -> bool {
        matches!(
            self,
            DataTypeKind::Primitive(
                PrimitiveType::Binary
                    | PrimitiveType::LargeBinary
                    | PrimitiveType::BinaryView
                    | PrimitiveType::ConfiguredBinary
            ) | DataTypeKind::FixedSizeBinary { .. }
        )
    }

    /// Check if this data type is nested (contains other types)
    pub fn is_nested(&self) -> bool {
        matches!(
            self,
            DataTypeKind::List { .. }
                | DataTypeKind::FixedSizeList { .. }
                | DataTypeKind::LargeList { .. }
                | DataTypeKind::Struct { .. }
                | DataTypeKind::Union { .. }
                | DataTypeKind::Map { .. }
        )
    }
}

impl PrimitiveType {
    /// Check if this primitive type is numeric
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            PrimitiveType::Int8
                | PrimitiveType::Int16
                | PrimitiveType::Int32
                | PrimitiveType::Int64
                | PrimitiveType::UInt8
                | PrimitiveType::UInt16
                | PrimitiveType::UInt32
                | PrimitiveType::UInt64
                | PrimitiveType::Float16
                | PrimitiveType::Float32
                | PrimitiveType::Float64
        )
    }

    /// Check if this primitive type is integer
    pub fn is_integer(&self) -> bool {
        matches!(
            self,
            PrimitiveType::Int8
                | PrimitiveType::Int16
                | PrimitiveType::Int32
                | PrimitiveType::Int64
                | PrimitiveType::UInt8
                | PrimitiveType::UInt16
                | PrimitiveType::UInt32
                | PrimitiveType::UInt64
        )
    }

    /// Check if this primitive type is floating point
    pub fn is_floating_point(&self) -> bool {
        matches!(
            self,
            PrimitiveType::Float16 | PrimitiveType::Float32 | PrimitiveType::Float64
        )
    }

    /// Check if this primitive type is signed
    pub fn is_signed(&self) -> bool {
        matches!(
            self,
            PrimitiveType::Int8
                | PrimitiveType::Int16
                | PrimitiveType::Int32
                | PrimitiveType::Int64
                | PrimitiveType::Float16
                | PrimitiveType::Float32
                | PrimitiveType::Float64
        )
    }
}

/// Validation functions for decimal types
impl DataTypeKind<NoMetadata> {
    /// Validate decimal128 precision and scale
    pub fn validate_decimal128(precision: u8, scale: i8) -> Result<(), CommonError> {
        if precision == 0 || precision > ARROW_DECIMAL128_MAX_PRECISION {
            return Err(CommonError::invalid(format!(
                "Decimal128 precision {} must be between 1 and {}",
                precision, ARROW_DECIMAL128_MAX_PRECISION
            )));
        }
        if !(0..=ARROW_DECIMAL128_MAX_SCALE).contains(&scale) || scale as u8 > precision {
            return Err(CommonError::invalid(format!(
                "Decimal128 scale {} must be between 0 and min({}, {})",
                scale, ARROW_DECIMAL128_MAX_SCALE, precision
            )));
        }
        Ok(())
    }

    /// Validate decimal256 precision and scale
    pub fn validate_decimal256(precision: u8, scale: i8) -> Result<(), CommonError> {
        if precision == 0 || precision > ARROW_DECIMAL256_MAX_PRECISION {
            return Err(CommonError::invalid(format!(
                "Decimal256 precision {} must be between 1 and {}",
                precision, ARROW_DECIMAL256_MAX_PRECISION
            )));
        }
        if !(0..=ARROW_DECIMAL256_MAX_SCALE).contains(&scale) || scale as u8 > precision {
            return Err(CommonError::invalid(format!(
                "Decimal256 scale {} must be between 0 and min({}, {})",
                scale, ARROW_DECIMAL256_MAX_SCALE, precision
            )));
        }
        Ok(())
    }
}

/// Convenience constructors for common data types
impl DataType<NoMetadata> {
    /// Create a boolean data type
    pub fn boolean() -> Self {
        Self::new(DataTypeKind::Primitive(PrimitiveType::Boolean))
    }

    /// Create an int32 data type
    pub fn int32() -> Self {
        Self::new(DataTypeKind::Primitive(PrimitiveType::Int32))
    }

    /// Create an int64 data type
    pub fn int64() -> Self {
        Self::new(DataTypeKind::Primitive(PrimitiveType::Int64))
    }

    /// Create a float32 data type
    pub fn float32() -> Self {
        Self::new(DataTypeKind::Primitive(PrimitiveType::Float32))
    }

    /// Create a float64 data type
    pub fn float64() -> Self {
        Self::new(DataTypeKind::Primitive(PrimitiveType::Float64))
    }

    /// Create a UTF-8 string data type
    pub fn utf8() -> Self {
        Self::new(DataTypeKind::Primitive(PrimitiveType::Utf8))
    }

    /// Create a binary data type
    pub fn binary() -> Self {
        Self::new(DataTypeKind::Primitive(PrimitiveType::Binary))
    }

    /// Create a timestamp data type with nanosecond precision
    pub fn timestamp_nanos() -> Self {
        Self::new(DataTypeKind::Timestamp {
            time_unit: TimeUnit::Nanosecond,
            timestamp_type: TimestampType::WithoutTimeZone,
        })
    }

    /// Create a date32 data type
    pub fn date32() -> Self {
        Self::new(DataTypeKind::Primitive(PrimitiveType::Date32))
    }

    /// Create a decimal128 data type with validation
    pub fn decimal128(precision: u8, scale: i8) -> Result<Self, CommonError> {
        DataTypeKind::validate_decimal128(precision, scale)?;
        Ok(Self::new(DataTypeKind::Decimal128 { precision, scale }))
    }

    /// Create a decimal256 data type with validation
    pub fn decimal256(precision: u8, scale: i8) -> Result<Self, CommonError> {
        DataTypeKind::validate_decimal256(precision, scale)?;
        Ok(Self::new(DataTypeKind::Decimal256 { precision, scale }))
    }

    /// Create a list data type
    pub fn list(element_type: DataType<NoMetadata>, nullable: bool) -> Self {
        Self::new(DataTypeKind::List {
            data_type: Box::new(element_type),
            nullable,
        })
    }

    /// Create a struct data type
    pub fn struct_type(fields: Fields<NoMetadata>) -> Self {
        Self::new(DataTypeKind::Struct { fields })
    }
}

/// Field constructor helpers
impl Field<NoMetadata> {
    /// Create a new field
    pub fn new<S: Into<String>>(name: S, data_type: DataType<NoMetadata>, nullable: bool) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable,
            metadata: vec![],
        }
    }

    /// Create a new field with metadata
    pub fn with_metadata<S: Into<String>>(
        name: S,
        data_type: DataType<NoMetadata>,
        nullable: bool,
        metadata: Vec<(String, String)>,
    ) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable,
            metadata,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_primitive_type_checks() {
        assert!(PrimitiveType::Int32.is_numeric());
        assert!(PrimitiveType::Int32.is_integer());
        assert!(!PrimitiveType::Int32.is_floating_point());
        assert!(PrimitiveType::Int32.is_signed());

        assert!(PrimitiveType::UInt32.is_numeric());
        assert!(PrimitiveType::UInt32.is_integer());
        assert!(!PrimitiveType::UInt32.is_floating_point());
        assert!(!PrimitiveType::UInt32.is_signed());

        assert!(PrimitiveType::Float64.is_numeric());
        assert!(!PrimitiveType::Float64.is_integer());
        assert!(PrimitiveType::Float64.is_floating_point());
        assert!(PrimitiveType::Float64.is_signed());

        assert!(!PrimitiveType::Utf8.is_numeric());
        assert!(!PrimitiveType::Boolean.is_numeric());
    }

    #[test]
    fn test_data_type_checks() {
        let int_type = DataType::int32();
        assert!(int_type.kind.is_numeric());
        assert!(!int_type.kind.is_temporal());
        assert!(!int_type.kind.is_string());
        assert!(!int_type.kind.is_binary());
        assert!(!int_type.kind.is_nested());

        let string_type = DataType::utf8();
        assert!(!string_type.kind.is_numeric());
        assert!(!string_type.kind.is_temporal());
        assert!(string_type.kind.is_string());
        assert!(!string_type.kind.is_binary());
        assert!(!string_type.kind.is_nested());

        let timestamp_type = DataType::timestamp_nanos();
        assert!(!timestamp_type.kind.is_numeric());
        assert!(timestamp_type.kind.is_temporal());
        assert!(!timestamp_type.kind.is_string());
        assert!(!timestamp_type.kind.is_binary());
        assert!(!timestamp_type.kind.is_nested());

        let list_type = DataType::list(DataType::int32(), true);
        assert!(!list_type.kind.is_numeric());
        assert!(!list_type.kind.is_temporal());
        assert!(!list_type.kind.is_string());
        assert!(!list_type.kind.is_binary());
        assert!(list_type.kind.is_nested());
    }

    #[test]
    fn test_decimal_validation() {
        // Valid decimal128
        assert!(DataType::decimal128(10, 2).is_ok());
        assert!(DataType::decimal128(38, 10).is_ok());

        // Invalid decimal128 - precision too high
        assert!(DataType::decimal128(39, 2).is_err());

        // Invalid decimal128 - scale too high
        assert!(DataType::decimal128(10, 11).is_err());

        // Invalid decimal128 - zero precision
        assert!(DataType::decimal128(0, 0).is_err());

        // Valid decimal256
        assert!(DataType::decimal256(50, 10).is_ok());
        assert!(DataType::decimal256(76, 20).is_ok());

        // Invalid decimal256 - precision too high
        assert!(DataType::decimal256(77, 2).is_err());
    }

    #[test]
    fn test_field_creation() {
        let field = Field::new("test_field", DataType::int32(), true);
        assert_eq!(field.name, "test_field");
        assert!(field.nullable);
        assert!(field.metadata.is_empty());

        let field_with_metadata = Field::with_metadata(
            "test_field_meta",
            DataType::utf8(),
            false,
            vec![("key".to_string(), "value".to_string())],
        );
        assert_eq!(field_with_metadata.name, "test_field_meta");
        assert!(!field_with_metadata.nullable);
        assert_eq!(field_with_metadata.metadata.len(), 1);
    }

    #[test]
    fn test_fields_collection() {
        let field1 = Field::new("field1", DataType::int32(), true);
        let field2 = Field::new("field2", DataType::utf8(), false);

        let fields = Fields::from(vec![field1, field2]);
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name, "field1");
        assert_eq!(fields[1].name, "field2");

        let empty_fields = Fields::<NoMetadata>::empty();
        assert_eq!(empty_fields.len(), 0);
    }

    #[test]
    fn test_schema_creation() {
        let field1 = Field::new("id", DataType::int64(), false);
        let field2 = Field::new("name", DataType::utf8(), true);
        let fields = Fields::from(vec![field1, field2]);

        let schema = Schema { fields };
        assert_eq!(schema.fields.len(), 2);
    }

    #[test]
    fn test_into_schema() {
        let struct_type = DataType::struct_type(Fields::from(vec![
            Field::new("x", DataType::float64(), false),
            Field::new("y", DataType::float64(), false),
        ]));

        let schema = struct_type.into_schema("point", false);
        assert_eq!(schema.fields.len(), 2);

        let simple_type = DataType::int32();
        let schema = simple_type.into_schema("value", true);
        assert_eq!(schema.fields.len(), 1);
        assert_eq!(schema.fields[0].name, "value");
        assert!(schema.fields[0].nullable);
    }

    #[test]
    fn test_enum_display() {
        assert_eq!(format!("{}", UnionMode::Sparse), "Sparse");
        assert_eq!(format!("{}", UnionMode::Dense), "Dense");
    }

    #[test]
    fn test_enum_try_from() {
        assert_eq!(UnionMode::try_from(0).unwrap(), UnionMode::Sparse);
        assert_eq!(UnionMode::try_from(1).unwrap(), UnionMode::Dense);
        assert!(UnionMode::try_from(2).is_err());

        assert_eq!(IntervalUnit::try_from(0).unwrap(), IntervalUnit::YearMonth);
        assert_eq!(IntervalUnit::try_from(1).unwrap(), IntervalUnit::DayTime);
        assert_eq!(
            IntervalUnit::try_from(2).unwrap(),
            IntervalUnit::MonthDayNano
        );
        assert!(IntervalUnit::try_from(3).is_err());
    }

    #[test]
    fn test_serializable_types() {
        use crate::types::serializable;

        // Create a simple schema
        let field1 = serializable::Field {
            name: "id".to_string(),
            data_type: serializable::DataType {
                kind: serializable::DataTypeKind::Primitive(PrimitiveType::Int32),
                metadata: NoMetadata,
            },
            nullable: false,
            metadata: vec![],
        };

        let field2 = serializable::Field {
            name: "name".to_string(),
            data_type: serializable::DataType {
                kind: serializable::DataTypeKind::Primitive(PrimitiveType::Utf8),
                metadata: NoMetadata,
            },
            nullable: true,
            metadata: vec![],
        };

        let schema = serializable::Schema {
            fields: serializable::Fields::new(vec![field1, field2]),
        };

        // Test JSON serialization
        let json = serde_json::to_string(&schema).expect("Failed to serialize to JSON");
        assert!(json.contains("\"id\""));
        assert!(json.contains("\"name\""));
        assert!(json.contains("\"int32\""));
        assert!(json.contains("\"utf8\""));

        // Test JSON deserialization
        let deserialized: serializable::Schema =
            serde_json::from_str(&json).expect("Failed to deserialize from JSON");
        assert_eq!(deserialized.fields.len(), 2);
        assert_eq!(deserialized.fields[0].name, "id");
        assert_eq!(deserialized.fields[1].name, "name");
    }

    #[test]
    fn test_data_type_conversion() {
        // Test basic primitive type conversion
        let data_type = DataType {
            kind: DataTypeKind::Primitive(PrimitiveType::Int32),
            metadata: NoMetadata,
        };

        let serializable_type: serializable::DataType = data_type.into();
        match serializable_type.kind {
            serializable::DataTypeKind::Primitive(PrimitiveType::Int32) => {}
            _ => panic!("Expected primitive type conversion to work"),
        }
    }

    #[test]
    fn test_complex_data_type_conversion() {
        // Test list type conversion
        let element_type = DataType {
            kind: DataTypeKind::Primitive(PrimitiveType::Utf8),
            metadata: NoMetadata,
        };

        let list_type = DataType {
            kind: DataTypeKind::List {
                data_type: Box::new(element_type),
                nullable: true,
            },
            metadata: NoMetadata,
        };

        let serializable_type: serializable::DataType = list_type.into();
        match serializable_type.kind {
            serializable::DataTypeKind::List {
                data_type,
                nullable,
            } => {
                assert!(nullable);
                match data_type.kind {
                    serializable::DataTypeKind::Primitive(PrimitiveType::Utf8) => {}
                    _ => panic!("Expected nested type conversion to work"),
                }
            }
            _ => panic!("Expected list type conversion to work"),
        }
    }

    #[test]
    fn test_schema_conversion() {
        let field1 = Field {
            name: "id".to_string(),
            data_type: DataType {
                kind: DataTypeKind::Primitive(PrimitiveType::Int64),
                metadata: NoMetadata,
            },
            nullable: false,
            metadata: vec![],
        };

        let field2 = Field {
            name: "name".to_string(),
            data_type: DataType {
                kind: DataTypeKind::Primitive(PrimitiveType::Utf8),
                metadata: NoMetadata,
            },
            nullable: true,
            metadata: vec![],
        };

        let schema = Schema {
            fields: Fields::from(vec![field1, field2]),
        };

        let serializable_schema: serializable::Schema = schema.into();
        assert_eq!(serializable_schema.fields.len(), 2);
        assert_eq!(serializable_schema.fields[0].name, "id");
        assert_eq!(serializable_schema.fields[1].name, "name");
        assert!(!serializable_schema.fields[0].nullable);
        assert!(serializable_schema.fields[1].nullable);
    }
}

// --- Conversion Implementations ---

#[allow(clippy::items_after_test_module)]
/// Conversion from generic DataType<M> to serializable DataType
impl<M: DataTypeMetadata> From<DataType<M>> for serializable::DataType {
    fn from(data_type: DataType<M>) -> Self {
        Self {
            kind: data_type.kind.into(),
            metadata: NoMetadata,
        }
    }
}

/// Conversion from generic DataTypeKind<M> to serializable DataTypeKind
impl<M: DataTypeMetadata> From<DataTypeKind<M>> for serializable::DataTypeKind {
    fn from(kind: DataTypeKind<M>) -> Self {
        match kind {
            DataTypeKind::Primitive(p) => serializable::DataTypeKind::Primitive(p),
            DataTypeKind::Timestamp {
                time_unit,
                timestamp_type,
            } => serializable::DataTypeKind::Timestamp {
                time_unit,
                timestamp_type,
            },
            DataTypeKind::Time32 { time_unit } => serializable::DataTypeKind::Time32 { time_unit },
            DataTypeKind::Time64 { time_unit } => serializable::DataTypeKind::Time64 { time_unit },
            DataTypeKind::Duration { time_unit } => {
                serializable::DataTypeKind::Duration { time_unit }
            }
            DataTypeKind::Interval {
                interval_unit,
                start_field,
                end_field,
            } => serializable::DataTypeKind::Interval {
                interval_unit,
                start_field,
                end_field,
            },
            DataTypeKind::FixedSizeBinary { size } => {
                serializable::DataTypeKind::FixedSizeBinary { size }
            }
            DataTypeKind::List {
                data_type,
                nullable,
            } => serializable::DataTypeKind::List {
                data_type: Box::new((*data_type).into()),
                nullable,
            },
            DataTypeKind::FixedSizeList {
                data_type,
                nullable,
                length,
            } => serializable::DataTypeKind::FixedSizeList {
                data_type: Box::new((*data_type).into()),
                nullable,
                length,
            },
            DataTypeKind::LargeList {
                data_type,
                nullable,
            } => serializable::DataTypeKind::LargeList {
                data_type: Box::new((*data_type).into()),
                nullable,
            },
            DataTypeKind::Struct { fields } => serializable::DataTypeKind::Struct {
                fields: fields.into(),
            },
            DataTypeKind::Union {
                union_fields,
                union_mode,
            } => serializable::DataTypeKind::Union {
                union_fields: union_fields.into(),
                union_mode,
            },
            DataTypeKind::Dictionary {
                key_type,
                value_type,
            } => serializable::DataTypeKind::Dictionary {
                key_type: Box::new((*key_type).into()),
                value_type: Box::new((*value_type).into()),
            },
            DataTypeKind::Decimal128 { precision, scale } => {
                serializable::DataTypeKind::Decimal128 { precision, scale }
            }
            DataTypeKind::Decimal256 { precision, scale } => {
                serializable::DataTypeKind::Decimal256 { precision, scale }
            }
            DataTypeKind::Map {
                key_type,
                value_type,
                value_type_nullable,
                keys_sorted,
            } => serializable::DataTypeKind::Map {
                key_type: Box::new((*key_type).into()),
                value_type: Box::new((*value_type).into()),
                value_type_nullable,
                keys_sorted,
            },
            DataTypeKind::UserDefined {
                name,
                jvm_class,
                python_class,
                serialized_python_class,
                sql_type,
            } => serializable::DataTypeKind::UserDefined {
                name,
                jvm_class,
                python_class,
                serialized_python_class,
                sql_type: Box::new((*sql_type).into()),
            },
            DataTypeKind::ConfiguredUtf8 { utf8_type } => {
                serializable::DataTypeKind::ConfiguredUtf8 { utf8_type }
            }
        }
    }
}

/// Conversion from generic Field<M> to serializable Field
impl<M: DataTypeMetadata> From<Field<M>> for serializable::Field {
    fn from(field: Field<M>) -> Self {
        Self {
            name: field.name,
            data_type: field.data_type.into(),
            nullable: field.nullable,
            metadata: field.metadata,
        }
    }
}

/// Conversion from generic Fields<M> to serializable Fields
impl<M: DataTypeMetadata> From<Fields<M>> for serializable::Fields {
    fn from(fields: Fields<M>) -> Self {
        let converted_fields: Vec<serializable::Field> = fields
            .iter()
            .map(|field_ref| (**field_ref).clone().into())
            .collect();
        serializable::Fields::new(converted_fields)
    }
}

/// Conversion from generic UnionFields<M> to serializable UnionFields
impl<M: DataTypeMetadata> From<UnionFields<M>> for serializable::UnionFields {
    fn from(union_fields: UnionFields<M>) -> Self {
        let converted_fields: Vec<(i8, serializable::Field)> = union_fields
            .0
            .as_ref()
            .iter()
            .map(|(id, field_ref)| (*id, (**field_ref).clone().into()))
            .collect();
        serializable::UnionFields::new(converted_fields)
    }
}

/// Conversion from generic Schema<M> to serializable Schema
impl<M: DataTypeMetadata> From<Schema<M>> for serializable::Schema {
    fn from(schema: Schema<M>) -> Self {
        Self {
            fields: schema.fields.into(),
        }
    }
}
