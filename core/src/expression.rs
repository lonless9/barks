//! Logical expression definitions for Barks.
//!
//! Generic expression system supporting metadata attachment.
//! Based on Sail "https://github.com/lakehq/sail"

use barks_common::error::CommonError;
use num_enum::{IntoPrimitive, TryFromPrimitive};

use crate::literal::Literal;
use crate::logical_plan::QueryPlan;
use crate::types::{DataType, DataTypeMetadata, NoMetadata, TimestampType};

// --- Generic Expression Representation ---

/// Primary expression structure with metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct Expr<M: DataTypeMetadata = NoMetadata> {
    pub kind: ExprKind<M>,
    pub metadata: M,
}

impl Expr<NoMetadata> {
    /// Creates a new expression without metadata.
    pub fn new(kind: ExprKind<NoMetadata>) -> Self {
        Self {
            kind,
            metadata: NoMetadata,
        }
    }
}

/// Core expression variants with optional metadata.
#[derive(Debug, Clone, PartialEq)]
pub enum ExprKind<M: DataTypeMetadata = NoMetadata> {
    Literal(Literal),
    UnresolvedAttribute {
        name: ObjectName,
        plan_id: Option<i64>,
        is_metadata_column: bool,
    },
    UnresolvedFunction(UnresolvedFunction<M>),
    UnresolvedStar {
        target: Option<ObjectName>,
        plan_id: Option<i64>,
        wildcard_options: WildcardOptions<M>,
    },
    Alias {
        expr: Box<Expr<M>>,
        /// A single identifier, or multiple identifiers for multi-alias.
        name: Vec<Identifier>,
        metadata: Option<Vec<(String, String)>>,
    },
    Cast {
        expr: Box<Expr<M>>,
        cast_to_type: DataType<M>,
        /// Whether to rename the expression to `CAST(... AS ...)`.
        rename: bool,
    },
    UnresolvedRegex {
        /// The regular expression to match column names.
        col_name: String,
        plan_id: Option<i64>,
    },
    SortOrder(SortOrder<M>),
    LambdaFunction {
        function: Box<Expr<M>>,
        arguments: Vec<UnresolvedNamedLambdaVariable>,
    },
    Window {
        window_function: Box<Expr<M>>,
        window: Window<M>,
    },
    UnresolvedExtractValue {
        child: Box<Expr<M>>,
        extraction: Box<Expr<M>>,
    },
    UpdateFields {
        struct_expression: Box<Expr<M>>,
        field_name: ObjectName,
        value_expression: Option<Box<Expr<M>>>,
    },
    UnresolvedNamedLambdaVariable(UnresolvedNamedLambdaVariable),
    CommonInlineUserDefinedFunction(CommonInlineUserDefinedFunction<M>),
    CallFunction {
        function_name: ObjectName,
        arguments: Vec<Expr<M>>,
    },
    // extensions
    Placeholder(String),
    Rollup(Vec<Expr<M>>),
    Cube(Vec<Expr<M>>),
    GroupingSets(Vec<Vec<Expr<M>>>),
    InSubquery {
        expr: Box<Expr<M>>,
        subquery: Box<QueryPlan<M>>,
        negated: bool,
    },
    ScalarSubquery {
        subquery: Box<QueryPlan<M>>,
    },
    Exists {
        subquery: Box<QueryPlan<M>>,
        negated: bool,
    },
    InList {
        expr: Box<Expr<M>>,
        list: Vec<Expr<M>>,
        negated: bool,
    },
    IsFalse(Box<Expr<M>>),
    IsNotFalse(Box<Expr<M>>),
    IsTrue(Box<Expr<M>>),
    IsNotTrue(Box<Expr<M>>),
    IsNull(Box<Expr<M>>),
    IsNotNull(Box<Expr<M>>),
    IsUnknown(Box<Expr<M>>),
    IsNotUnknown(Box<Expr<M>>),
    Between {
        expr: Box<Expr<M>>,
        negated: bool,
        low: Box<Expr<M>>,
        high: Box<Expr<M>>,
    },
    IsDistinctFrom {
        left: Box<Expr<M>>,
        right: Box<Expr<M>>,
    },
    IsNotDistinctFrom {
        left: Box<Expr<M>>,
        right: Box<Expr<M>>,
    },
    SimilarTo {
        expr: Box<Expr<M>>,
        pattern: Box<Expr<M>>,
        negated: bool,
        escape_char: Option<char>,
        case_insensitive: bool,
    },
    Table {
        expr: Box<Expr<M>>,
    },
    UnresolvedDate {
        value: String,
    },
    UnresolvedTimestamp {
        value: String,
        timestamp_type: TimestampType,
    },
}

// --- Supporting Structs (Generic and Concrete) ---

/// An identifier with only one part.
/// It is the raw value without quotes or escape characters.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct Identifier(String);

impl From<String> for Identifier {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for Identifier {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl From<Identifier> for String {
    fn from(id: Identifier) -> Self {
        id.0
    }
}

impl AsRef<str> for Identifier {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// An object name with potentially multiple parts.
/// Each part is a raw value without quotes or escape characters.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct ObjectName(Vec<Identifier>);

impl ObjectName {
    pub fn bare(name: impl Into<Identifier>) -> Self {
        Self(vec![name.into()])
    }

    pub fn child(self, name: impl Into<Identifier>) -> Self {
        let mut names = self.0;
        names.push(name.into());
        Self(names)
    }

    pub fn parts(&self) -> &[Identifier] {
        &self.0
    }
}

impl<T, S> From<T> for ObjectName
where
    T: IntoIterator<Item = S>,
    S: Into<Identifier>,
{
    fn from(value: T) -> Self {
        Self(value.into_iter().map(|x| x.into()).collect())
    }
}

impl From<ObjectName> for Vec<String> {
    fn from(name: ObjectName) -> Self {
        name.0.into_iter().map(String::from).collect()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SortOrder<M: DataTypeMetadata = NoMetadata> {
    pub child: Box<Expr<M>>,
    pub direction: SortDirection,
    pub null_ordering: NullOrdering,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum SortDirection {
    Unspecified,
    Ascending,
    Descending,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum NullOrdering {
    Unspecified,
    NullsFirst,
    NullsLast,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UnresolvedFunction<M: DataTypeMetadata = NoMetadata> {
    pub function_name: ObjectName,
    /// A list of positional arguments.
    pub arguments: Vec<Expr<M>>,
    /// A list of named arguments, which must come after positional arguments.
    pub named_arguments: Vec<(Identifier, Expr<M>)>,
    pub is_distinct: bool,
    pub is_user_defined_function: bool,
    pub is_internal: Option<bool>,
    pub ignore_nulls: Option<bool>,
    pub filter: Option<Box<Expr<M>>>,
    pub order_by: Option<Vec<SortOrder<M>>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Window<M: DataTypeMetadata = NoMetadata> {
    Named(Identifier),
    Unnamed {
        cluster_by: Vec<Expr<M>>,
        partition_by: Vec<Expr<M>>,
        order_by: Vec<SortOrder<M>>,
        frame: Option<WindowFrame<M>>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct WindowFrame<M: DataTypeMetadata = NoMetadata> {
    pub frame_type: WindowFrameType,
    pub lower: WindowFrameBoundary<M>,
    pub upper: WindowFrameBoundary<M>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum WindowFrameType {
    Row,
    Range,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WindowFrameBoundary<M: DataTypeMetadata = NoMetadata> {
    CurrentRow,
    UnboundedPreceding,
    UnboundedFollowing,
    Preceding(Box<Expr<M>>),
    Following(Box<Expr<M>>),
    /// An alternative way to specify a window frame boundary, where
    /// a negative value is a preceding boundary and a positive value is a following boundary.
    Value(Box<Expr<M>>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct CommonInlineUserDefinedFunction<M: DataTypeMetadata = NoMetadata> {
    pub function_name: Identifier,
    pub deterministic: bool,
    pub is_distinct: bool,
    pub arguments: Vec<Expr<M>>,
    pub function: serializable::FunctionDefinition,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WildcardOptions<M: DataTypeMetadata = NoMetadata> {
    pub ilike_pattern: Option<String>,
    pub exclude_columns: Option<Vec<Identifier>>,
    pub except_columns: Option<Vec<Identifier>>,
    pub replace_columns: Option<Vec<WildcardReplaceColumn<M>>>,
    pub rename_columns: Option<Vec<IdentifierWithAlias>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WildcardReplaceColumn<M: DataTypeMetadata = NoMetadata> {
    pub expression: Box<Expr<M>>,
    pub column_name: Identifier,
    pub as_keyword: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CommonInlineUserDefinedTableFunction<M: DataTypeMetadata = NoMetadata> {
    pub function_name: Identifier,
    pub deterministic: bool,
    pub arguments: Vec<Expr<M>>,
    pub function: serializable::TableFunctionDefinition,
}

// --- Serializable Expression Representation ---

/// Serializable versions of expression types for JSON/binary serialization.
pub mod serializable {
    use super::{NullOrdering, PySparkUdfType, SortDirection, WindowFrameType};
    use crate::literal::Literal;
    use crate::logical_plan::serializable::QueryPlan;
    use crate::types::TimestampType;
    use crate::types::serializable::DataType;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
    pub enum Expr {
        Literal(Literal),
        UnresolvedAttribute {
            name: super::ObjectName,
            plan_id: Option<i64>,
            is_metadata_column: bool,
        },
        UnresolvedFunction(UnresolvedFunction),
        UnresolvedStar {
            target: Option<super::ObjectName>,
            plan_id: Option<i64>,
            wildcard_options: WildcardOptions,
        },
        Alias {
            expr: Box<Expr>,
            name: Vec<super::Identifier>,
            metadata: Option<Vec<(String, String)>>,
        },
        Cast {
            expr: Box<Expr>,
            cast_to_type: DataType,
            rename: bool,
        },
        UnresolvedRegex {
            col_name: String,
            plan_id: Option<i64>,
        },
        SortOrder(SortOrder),
        LambdaFunction {
            function: Box<Expr>,
            arguments: Vec<UnresolvedNamedLambdaVariable>,
        },
        Window {
            window_function: Box<Expr>,
            window: Window,
        },
        UnresolvedExtractValue {
            child: Box<Expr>,
            extraction: Box<Expr>,
        },
        UpdateFields {
            struct_expression: Box<Expr>,
            field_name: super::ObjectName,
            value_expression: Option<Box<Expr>>,
        },
        UnresolvedNamedLambdaVariable(UnresolvedNamedLambdaVariable),
        CommonInlineUserDefinedFunction(CommonInlineUserDefinedFunction),
        CallFunction {
            function_name: super::ObjectName,
            arguments: Vec<Expr>,
        },
        // extensions
        Placeholder(String),
        Rollup(Vec<Expr>),
        Cube(Vec<Expr>),
        GroupingSets(Vec<Vec<Expr>>),
        InSubquery {
            expr: Box<Expr>,
            subquery: Box<QueryPlan>,
            negated: bool,
        },
        ScalarSubquery {
            subquery: Box<QueryPlan>,
        },
        Exists {
            subquery: Box<QueryPlan>,
            negated: bool,
        },
        InList {
            expr: Box<Expr>,
            list: Vec<Expr>,
            negated: bool,
        },
        IsFalse(Box<Expr>),
        IsNotFalse(Box<Expr>),
        IsTrue(Box<Expr>),
        IsNotTrue(Box<Expr>),
        IsNull(Box<Expr>),
        IsNotNull(Box<Expr>),
        IsUnknown(Box<Expr>),
        IsNotUnknown(Box<Expr>),
        Between {
            expr: Box<Expr>,
            negated: bool,
            low: Box<Expr>,
            high: Box<Expr>,
        },
        IsDistinctFrom {
            left: Box<Expr>,
            right: Box<Expr>,
        },
        IsNotDistinctFrom {
            left: Box<Expr>,
            right: Box<Expr>,
        },
        SimilarTo {
            expr: Box<Expr>,
            pattern: Box<Expr>,
            negated: bool,
            escape_char: Option<char>,
            case_insensitive: bool,
        },
        Table {
            expr: Box<Expr>,
        },
        UnresolvedDate {
            value: String,
        },
        UnresolvedTimestamp {
            value: String,
            timestamp_type: TimestampType,
        },
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct SortOrder {
        pub child: Box<Expr>,
        pub direction: SortDirection,
        pub null_ordering: NullOrdering,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct UnresolvedFunction {
        pub function_name: super::ObjectName,
        pub arguments: Vec<Expr>,
        pub named_arguments: Vec<(super::Identifier, Expr)>,
        pub is_distinct: bool,
        pub is_user_defined_function: bool,
        pub is_internal: Option<bool>,
        pub ignore_nulls: Option<bool>,
        pub filter: Option<Box<Expr>>,
        pub order_by: Option<Vec<SortOrder>>,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
    pub enum Window {
        Named(super::Identifier),
        Unnamed {
            cluster_by: Vec<Expr>,
            partition_by: Vec<Expr>,
            order_by: Vec<SortOrder>,
            frame: Option<WindowFrame>,
        },
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct WindowFrame {
        pub frame_type: WindowFrameType,
        pub lower: WindowFrameBoundary,
        pub upper: WindowFrameBoundary,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub enum WindowFrameBoundary {
        CurrentRow,
        UnboundedPreceding,
        UnboundedFollowing,
        Preceding(Box<Expr>),
        Following(Box<Expr>),
        Value(Box<Expr>),
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct CommonInlineUserDefinedFunction {
        pub function_name: super::Identifier,
        pub deterministic: bool,
        pub is_distinct: bool,
        pub arguments: Vec<Expr>,
        #[serde(flatten)]
        pub function: FunctionDefinition,
    }

    #[allow(clippy::enum_variant_names)]
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
    pub enum FunctionDefinition {
        PythonUdf {
            output_type: DataType,
            eval_type: PySparkUdfType,
            command: Vec<u8>,
            python_version: String,
            additional_includes: Vec<String>,
        },
        ScalarScalaUdf {
            payload: Vec<u8>,
            input_types: Vec<DataType>,
            output_type: DataType,
            nullable: bool,
            aggregate: bool,
        },
        JavaUdf {
            class_name: String,
            output_type: Option<DataType>,
            aggregate: bool,
        },
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct CommonInlineUserDefinedTableFunction {
        pub function_name: super::Identifier,
        pub deterministic: bool,
        pub arguments: Vec<Expr>,
        #[serde(flatten)]
        pub function: TableFunctionDefinition,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
    pub enum TableFunctionDefinition {
        PythonUdtf {
            return_type: DataType,
            eval_type: PySparkUdfType,
            command: Vec<u8>,
            python_version: String,
        },
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct UnresolvedNamedLambdaVariable {
        pub name: super::ObjectName,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
    #[serde(rename_all = "camelCase")]
    pub struct WildcardOptions {
        pub ilike_pattern: Option<String>,
        pub exclude_columns: Option<Vec<super::Identifier>>,
        pub except_columns: Option<Vec<super::Identifier>>,
        pub replace_columns: Option<Vec<WildcardReplaceColumn>>,
        pub rename_columns: Option<Vec<super::IdentifierWithAlias>>,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct WildcardReplaceColumn {
        pub expression: Box<Expr>,
        pub column_name: super::Identifier,
        pub as_keyword: bool,
    }
}

// Re-export concrete types for convenience, if they didn't need to be generic.
pub use serializable::{FunctionDefinition, TableFunctionDefinition};

// --- Conversion Logic Between Logical and Serializable Models ---

/// Conversion from generic Expr<M> to serializable Expr
impl<M: DataTypeMetadata> From<Expr<M>> for serializable::Expr {
    fn from(expr: Expr<M>) -> Self {
        expr.kind.into()
    }
}

/// Conversion from generic ExprKind<M> to serializable Expr
impl<M: DataTypeMetadata> From<ExprKind<M>> for serializable::Expr {
    fn from(kind: ExprKind<M>) -> Self {
        match kind {
            ExprKind::Literal(literal) => serializable::Expr::Literal(literal),
            ExprKind::UnresolvedAttribute {
                name,
                plan_id,
                is_metadata_column,
            } => serializable::Expr::UnresolvedAttribute {
                name,
                plan_id,
                is_metadata_column,
            },
            ExprKind::UnresolvedFunction(func) => {
                serializable::Expr::UnresolvedFunction(func.into())
            }
            ExprKind::UnresolvedStar {
                target,
                plan_id,
                wildcard_options,
            } => serializable::Expr::UnresolvedStar {
                target,
                plan_id,
                wildcard_options: wildcard_options.into(),
            },
            ExprKind::Alias {
                expr,
                name,
                metadata,
            } => serializable::Expr::Alias {
                expr: Box::new((*expr).into()),
                name,
                metadata,
            },
            ExprKind::Cast {
                expr,
                cast_to_type,
                rename,
            } => serializable::Expr::Cast {
                expr: Box::new((*expr).into()),
                cast_to_type: cast_to_type.into(),
                rename,
            },
            ExprKind::UnresolvedRegex { col_name, plan_id } => {
                serializable::Expr::UnresolvedRegex { col_name, plan_id }
            }
            ExprKind::SortOrder(sort_order) => serializable::Expr::SortOrder(sort_order.into()),
            ExprKind::LambdaFunction {
                function,
                arguments,
            } => serializable::Expr::LambdaFunction {
                function: Box::new((*function).into()),
                arguments: arguments.into_iter().map(|arg| arg.into()).collect(),
            },
            ExprKind::Window {
                window_function,
                window,
            } => serializable::Expr::Window {
                window_function: Box::new((*window_function).into()),
                window: window.into(),
            },
            ExprKind::UnresolvedExtractValue { child, extraction } => {
                serializable::Expr::UnresolvedExtractValue {
                    child: Box::new((*child).into()),
                    extraction: Box::new((*extraction).into()),
                }
            }
            ExprKind::UpdateFields {
                struct_expression,
                field_name,
                value_expression,
            } => serializable::Expr::UpdateFields {
                struct_expression: Box::new((*struct_expression).into()),
                field_name,
                value_expression: value_expression.map(|expr| Box::new((*expr).into())),
            },
            ExprKind::UnresolvedNamedLambdaVariable(var) => {
                serializable::Expr::UnresolvedNamedLambdaVariable(var.into())
            }
            ExprKind::CommonInlineUserDefinedFunction(func) => {
                serializable::Expr::CommonInlineUserDefinedFunction(func.into())
            }
            ExprKind::CallFunction {
                function_name,
                arguments,
            } => serializable::Expr::CallFunction {
                function_name,
                arguments: arguments.into_iter().map(|arg| arg.into()).collect(),
            },
            // extensions
            ExprKind::Placeholder(placeholder) => serializable::Expr::Placeholder(placeholder),
            ExprKind::Rollup(exprs) => {
                serializable::Expr::Rollup(exprs.into_iter().map(|expr| expr.into()).collect())
            }
            ExprKind::Cube(exprs) => {
                serializable::Expr::Cube(exprs.into_iter().map(|expr| expr.into()).collect())
            }
            ExprKind::GroupingSets(sets) => serializable::Expr::GroupingSets(
                sets.into_iter()
                    .map(|set| set.into_iter().map(|expr| expr.into()).collect())
                    .collect(),
            ),
            ExprKind::InSubquery {
                expr,
                subquery,
                negated,
            } => serializable::Expr::InSubquery {
                expr: Box::new((*expr).into()),
                subquery: Box::new((*subquery).into()),
                negated,
            },
            ExprKind::ScalarSubquery { subquery } => serializable::Expr::ScalarSubquery {
                subquery: Box::new((*subquery).into()),
            },
            ExprKind::Exists { subquery, negated } => serializable::Expr::Exists {
                subquery: Box::new((*subquery).into()),
                negated,
            },
            ExprKind::InList {
                expr,
                list,
                negated,
            } => serializable::Expr::InList {
                expr: Box::new((*expr).into()),
                list: list.into_iter().map(|item| item.into()).collect(),
                negated,
            },
            ExprKind::IsFalse(expr) => serializable::Expr::IsFalse(Box::new((*expr).into())),
            ExprKind::IsNotFalse(expr) => serializable::Expr::IsNotFalse(Box::new((*expr).into())),
            ExprKind::IsTrue(expr) => serializable::Expr::IsTrue(Box::new((*expr).into())),
            ExprKind::IsNotTrue(expr) => serializable::Expr::IsNotTrue(Box::new((*expr).into())),
            ExprKind::IsNull(expr) => serializable::Expr::IsNull(Box::new((*expr).into())),
            ExprKind::IsNotNull(expr) => serializable::Expr::IsNotNull(Box::new((*expr).into())),
            ExprKind::IsUnknown(expr) => serializable::Expr::IsUnknown(Box::new((*expr).into())),
            ExprKind::IsNotUnknown(expr) => {
                serializable::Expr::IsNotUnknown(Box::new((*expr).into()))
            }
            ExprKind::Between {
                expr,
                negated,
                low,
                high,
            } => serializable::Expr::Between {
                expr: Box::new((*expr).into()),
                negated,
                low: Box::new((*low).into()),
                high: Box::new((*high).into()),
            },
            ExprKind::IsDistinctFrom { left, right } => serializable::Expr::IsDistinctFrom {
                left: Box::new((*left).into()),
                right: Box::new((*right).into()),
            },
            ExprKind::IsNotDistinctFrom { left, right } => serializable::Expr::IsNotDistinctFrom {
                left: Box::new((*left).into()),
                right: Box::new((*right).into()),
            },
            ExprKind::SimilarTo {
                expr,
                pattern,
                negated,
                escape_char,
                case_insensitive,
            } => serializable::Expr::SimilarTo {
                expr: Box::new((*expr).into()),
                pattern: Box::new((*pattern).into()),
                negated,
                escape_char,
                case_insensitive,
            },
            ExprKind::Table { expr } => serializable::Expr::Table {
                expr: Box::new((*expr).into()),
            },
            ExprKind::UnresolvedDate { value } => serializable::Expr::UnresolvedDate { value },
            ExprKind::UnresolvedTimestamp {
                value,
                timestamp_type,
            } => serializable::Expr::UnresolvedTimestamp {
                value,
                timestamp_type,
            },
        }
    }
}

/// Conversion from generic UnresolvedFunction<M> to serializable UnresolvedFunction
impl<M: DataTypeMetadata> From<UnresolvedFunction<M>> for serializable::UnresolvedFunction {
    fn from(func: UnresolvedFunction<M>) -> Self {
        Self {
            function_name: func.function_name,
            arguments: func.arguments.into_iter().map(|arg| arg.into()).collect(),
            named_arguments: func
                .named_arguments
                .into_iter()
                .map(|(name, expr)| (name, expr.into()))
                .collect(),
            is_distinct: func.is_distinct,
            is_user_defined_function: func.is_user_defined_function,
            is_internal: func.is_internal,
            ignore_nulls: func.ignore_nulls,
            filter: func.filter.map(|f| Box::new((*f).into())),
            order_by: func
                .order_by
                .map(|orders| orders.into_iter().map(|order| order.into()).collect()),
        }
    }
}

/// Conversion from generic SortOrder<M> to serializable SortOrder
impl<M: DataTypeMetadata> From<SortOrder<M>> for serializable::SortOrder {
    fn from(sort_order: SortOrder<M>) -> Self {
        Self {
            child: Box::new((*sort_order.child).into()),
            direction: sort_order.direction,
            null_ordering: sort_order.null_ordering,
        }
    }
}

/// Conversion from generic WildcardOptions<M> to serializable WildcardOptions
impl<M: DataTypeMetadata> From<WildcardOptions<M>> for serializable::WildcardOptions {
    fn from(options: WildcardOptions<M>) -> Self {
        Self {
            ilike_pattern: options.ilike_pattern,
            exclude_columns: options.exclude_columns,
            except_columns: options.except_columns,
            replace_columns: options
                .replace_columns
                .map(|cols| cols.into_iter().map(|col| col.into()).collect()),
            rename_columns: options.rename_columns,
        }
    }
}

/// Conversion from generic WildcardReplaceColumn<M> to serializable WildcardReplaceColumn
impl<M: DataTypeMetadata> From<WildcardReplaceColumn<M>> for serializable::WildcardReplaceColumn {
    fn from(col: WildcardReplaceColumn<M>) -> Self {
        Self {
            expression: Box::new((*col.expression).into()),
            column_name: col.column_name,
            as_keyword: col.as_keyword,
        }
    }
}

/// Conversion from generic Window<M> to serializable Window
impl<M: DataTypeMetadata> From<Window<M>> for serializable::Window {
    fn from(window: Window<M>) -> Self {
        match window {
            Window::Named(name) => serializable::Window::Named(name),
            Window::Unnamed {
                cluster_by,
                partition_by,
                order_by,
                frame,
            } => serializable::Window::Unnamed {
                cluster_by: cluster_by.into_iter().map(|expr| expr.into()).collect(),
                partition_by: partition_by.into_iter().map(|expr| expr.into()).collect(),
                order_by: order_by.into_iter().map(|order| order.into()).collect(),
                frame: frame.map(|f| f.into()),
            },
        }
    }
}

/// Conversion from generic WindowFrame<M> to serializable WindowFrame
impl<M: DataTypeMetadata> From<WindowFrame<M>> for serializable::WindowFrame {
    fn from(frame: WindowFrame<M>) -> Self {
        Self {
            frame_type: frame.frame_type,
            lower: frame.lower.into(),
            upper: frame.upper.into(),
        }
    }
}

/// Conversion from generic WindowFrameBoundary<M> to serializable WindowFrameBoundary
impl<M: DataTypeMetadata> From<WindowFrameBoundary<M>> for serializable::WindowFrameBoundary {
    fn from(boundary: WindowFrameBoundary<M>) -> Self {
        match boundary {
            WindowFrameBoundary::CurrentRow => serializable::WindowFrameBoundary::CurrentRow,
            WindowFrameBoundary::UnboundedPreceding => {
                serializable::WindowFrameBoundary::UnboundedPreceding
            }
            WindowFrameBoundary::UnboundedFollowing => {
                serializable::WindowFrameBoundary::UnboundedFollowing
            }
            WindowFrameBoundary::Preceding(expr) => {
                serializable::WindowFrameBoundary::Preceding(Box::new((*expr).into()))
            }
            WindowFrameBoundary::Following(expr) => {
                serializable::WindowFrameBoundary::Following(Box::new((*expr).into()))
            }
            WindowFrameBoundary::Value(expr) => {
                serializable::WindowFrameBoundary::Value(Box::new((*expr).into()))
            }
        }
    }
}

/// Conversion from generic CommonInlineUserDefinedFunction<M> to serializable CommonInlineUserDefinedFunction
impl<M: DataTypeMetadata> From<CommonInlineUserDefinedFunction<M>>
    for serializable::CommonInlineUserDefinedFunction
{
    fn from(func: CommonInlineUserDefinedFunction<M>) -> Self {
        Self {
            function_name: func.function_name,
            deterministic: func.deterministic,
            is_distinct: func.is_distinct,
            arguments: func.arguments.into_iter().map(|arg| arg.into()).collect(),
            function: func.function,
        }
    }
}

/// Conversion from generic CommonInlineUserDefinedTableFunction<M> to serializable CommonInlineUserDefinedTableFunction
impl<M: DataTypeMetadata> From<CommonInlineUserDefinedTableFunction<M>>
    for serializable::CommonInlineUserDefinedTableFunction
{
    fn from(func: CommonInlineUserDefinedTableFunction<M>) -> Self {
        Self {
            function_name: func.function_name,
            deterministic: func.deterministic,
            arguments: func.arguments.into_iter().map(|arg| arg.into()).collect(),
            function: func.function,
        }
    }
}

/// Conversion from UnresolvedNamedLambdaVariable to serializable UnresolvedNamedLambdaVariable
impl From<UnresolvedNamedLambdaVariable> for serializable::UnresolvedNamedLambdaVariable {
    fn from(var: UnresolvedNamedLambdaVariable) -> Self {
        Self { name: var.name }
    }
}

// --- Supporting Enums (Concrete) ---

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    TryFromPrimitive,
    IntoPrimitive,
    serde::Serialize,
    serde::Deserialize,
)]
#[num_enum(error_type(name = CommonError, constructor = PySparkUdfType::invalid))]
#[repr(i32)]
pub enum PySparkUdfType {
    None = 0,
    Batched = 100,
    ArrowBatched = 101,
    ScalarPandas = 200,
    GroupedMapPandas = 201,
    GroupedAggPandas = 202,
    WindowAggPandas = 203,
    ScalarPandasIter = 204,
    MapPandasIter = 205,
    CogroupedMapPandas = 206,
    MapArrowIter = 207,
    GroupedMapPandasWithState = 208,
    Table = 300,
    ArrowTable = 301,
}

impl PySparkUdfType {
    fn invalid(v: i32) -> CommonError {
        CommonError::invalid(format!("invalid PySpark UDF type: {}", v))
    }

    pub fn is_table_function(&self) -> bool {
        matches!(self, PySparkUdfType::Table | PySparkUdfType::ArrowTable)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct UnresolvedNamedLambdaVariable {
    pub name: ObjectName,
}

/// An identifier with an alias.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IdentifierWithAlias {
    pub identifier: Identifier,
    pub alias: Identifier,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::literal::Literal;
    use crate::types::{DataType, NoMetadata};

    // --- Core Expression Tests ---

    #[test]
    fn test_expr_creation() {
        let expr = Expr::new(ExprKind::Literal(Literal::Int32 { value: Some(42) }));
        assert_eq!(expr.metadata, NoMetadata);
        match expr.kind {
            ExprKind::Literal(Literal::Int32 { value: Some(42) }) => {}
            _ => panic!("Expected literal expression"),
        }
    }

    #[test]
    fn test_expr_kinds_literal() {
        let expr: ExprKind<NoMetadata> = ExprKind::Literal(Literal::Utf8 {
            value: Some("test".to_string()),
        });
        match expr {
            ExprKind::Literal(Literal::Utf8 { value: Some(s) }) => assert_eq!(s, "test"),
            _ => panic!("Expected string literal"),
        }
    }

    #[test]
    fn test_expr_kinds_unresolved_attribute() {
        let expr: ExprKind<NoMetadata> = ExprKind::UnresolvedAttribute {
            name: ObjectName::bare("column1"),
            plan_id: Some(123),
            is_metadata_column: false,
        };
        match expr {
            ExprKind::UnresolvedAttribute {
                name,
                plan_id,
                is_metadata_column,
            } => {
                assert_eq!(name, ObjectName::bare("column1"));
                assert_eq!(plan_id, Some(123));
                assert!(!is_metadata_column);
            }
            _ => panic!("Expected unresolved attribute"),
        }
    }

    #[test]
    fn test_expr_kinds_alias() {
        let expr = ExprKind::Alias {
            expr: Box::new(Expr::new(ExprKind::Literal(Literal::Int32 {
                value: Some(42),
            }))),
            name: vec![Identifier::from("alias_name")],
            metadata: Some(vec![("key".to_string(), "value".to_string())]),
        };
        match expr {
            ExprKind::Alias { name, metadata, .. } => {
                assert_eq!(name.len(), 1);
                assert_eq!(name[0].as_ref(), "alias_name");
                assert!(metadata.is_some());
            }
            _ => panic!("Expected alias expression"),
        }
    }

    #[test]
    fn test_expr_kinds_cast() {
        let expr: ExprKind<NoMetadata> = ExprKind::Cast {
            expr: Box::new(Expr::new(ExprKind::Literal(Literal::Int32 {
                value: Some(42),
            }))),
            cast_to_type: DataType::utf8(),
            rename: true,
        };
        match expr {
            ExprKind::Cast {
                cast_to_type,
                rename,
                ..
            } => {
                assert_eq!(cast_to_type, DataType::utf8());
                assert!(rename);
            }
            _ => panic!("Expected cast expression"),
        }
    }

    #[test]
    fn test_expr_kinds_unresolved_regex() {
        let expr: ExprKind<NoMetadata> = ExprKind::UnresolvedRegex {
            col_name: "col_*".to_string(),
            plan_id: Some(456),
        };
        match expr {
            ExprKind::UnresolvedRegex { col_name, plan_id } => {
                assert_eq!(col_name, "col_*");
                assert_eq!(plan_id, Some(456));
            }
            _ => panic!("Expected unresolved regex"),
        }
    }

    #[test]
    fn test_expr_kinds_boolean_operations() {
        let base_expr = Box::new(Expr::new(ExprKind::Literal(Literal::Boolean {
            value: Some(true),
        })));

        let is_true = ExprKind::IsTrue(base_expr.clone());
        let is_not_true = ExprKind::IsNotTrue(base_expr.clone());
        let is_false = ExprKind::IsFalse(base_expr.clone());
        let is_not_false = ExprKind::IsNotFalse(base_expr.clone());
        let is_null = ExprKind::IsNull(base_expr.clone());
        let is_not_null = ExprKind::IsNotNull(base_expr.clone());
        let is_unknown = ExprKind::IsUnknown(base_expr.clone());
        let is_not_unknown = ExprKind::IsNotUnknown(base_expr);

        match is_true {
            ExprKind::IsTrue(_) => {}
            _ => panic!("Expected IsTrue"),
        }
        match is_not_true {
            ExprKind::IsNotTrue(_) => {}
            _ => panic!("Expected IsNotTrue"),
        }
        match is_false {
            ExprKind::IsFalse(_) => {}
            _ => panic!("Expected IsFalse"),
        }
        match is_not_false {
            ExprKind::IsNotFalse(_) => {}
            _ => panic!("Expected IsNotFalse"),
        }
        match is_null {
            ExprKind::IsNull(_) => {}
            _ => panic!("Expected IsNull"),
        }
        match is_not_null {
            ExprKind::IsNotNull(_) => {}
            _ => panic!("Expected IsNotNull"),
        }
        match is_unknown {
            ExprKind::IsUnknown(_) => {}
            _ => panic!("Expected IsUnknown"),
        }
        match is_not_unknown {
            ExprKind::IsNotUnknown(_) => {}
            _ => panic!("Expected IsNotUnknown"),
        }
    }

    #[test]
    fn test_expr_kinds_between() {
        let expr = ExprKind::Between {
            expr: Box::new(Expr::new(ExprKind::Literal(Literal::Int32 {
                value: Some(5),
            }))),
            negated: false,
            low: Box::new(Expr::new(ExprKind::Literal(Literal::Int32 {
                value: Some(1),
            }))),
            high: Box::new(Expr::new(ExprKind::Literal(Literal::Int32 {
                value: Some(10),
            }))),
        };
        match expr {
            ExprKind::Between { negated, .. } => {
                assert!(!negated);
            }
            _ => panic!("Expected Between expression"),
        }
    }

    #[test]
    fn test_expr_kinds_in_list() {
        let expr = ExprKind::InList {
            expr: Box::new(Expr::new(ExprKind::Literal(Literal::Int32 {
                value: Some(5),
            }))),
            list: vec![
                Expr::new(ExprKind::Literal(Literal::Int32 { value: Some(1) })),
                Expr::new(ExprKind::Literal(Literal::Int32 { value: Some(5) })),
                Expr::new(ExprKind::Literal(Literal::Int32 { value: Some(10) })),
            ],
            negated: false,
        };
        match expr {
            ExprKind::InList { list, negated, .. } => {
                assert_eq!(list.len(), 3);
                assert!(!negated);
            }
            _ => panic!("Expected InList expression"),
        }
    }

    #[test]
    fn test_expr_kinds_similar_to() {
        let expr = ExprKind::SimilarTo {
            expr: Box::new(Expr::new(ExprKind::Literal(Literal::Utf8 {
                value: Some("test".to_string()),
            }))),
            pattern: Box::new(Expr::new(ExprKind::Literal(Literal::Utf8 {
                value: Some("t%".to_string()),
            }))),
            negated: false,
            escape_char: Some('\\'),
            case_insensitive: true,
        };
        match expr {
            ExprKind::SimilarTo {
                negated,
                escape_char,
                case_insensitive,
                ..
            } => {
                assert!(!negated);
                assert_eq!(escape_char, Some('\\'));
                assert!(case_insensitive);
            }
            _ => panic!("Expected SimilarTo expression"),
        }
    }

    #[test]
    fn test_expr_kinds_temporal() {
        let date_expr: ExprKind<NoMetadata> = ExprKind::UnresolvedDate {
            value: "2023-01-01".to_string(),
        };
        let timestamp_expr: ExprKind<NoMetadata> = ExprKind::UnresolvedTimestamp {
            value: "2023-01-01 12:00:00".to_string(),
            timestamp_type: crate::types::TimestampType::WithoutTimeZone,
        };

        match date_expr {
            ExprKind::UnresolvedDate { value } => {
                assert_eq!(value, "2023-01-01");
            }
            _ => panic!("Expected UnresolvedDate"),
        }
        match timestamp_expr {
            ExprKind::UnresolvedTimestamp {
                value,
                timestamp_type,
            } => {
                assert_eq!(value, "2023-01-01 12:00:00");
                assert_eq!(timestamp_type, crate::types::TimestampType::WithoutTimeZone);
            }
            _ => panic!("Expected UnresolvedTimestamp"),
        }
    }

    // --- Supporting Struct Tests ---

    #[test]
    fn test_identifier_creation() {
        let id1 = Identifier::from("test");
        let id2 = Identifier::from("test".to_string());

        assert_eq!(id1.as_ref(), "test");
        assert_eq!(id2.as_ref(), "test");
        assert_eq!(id1, id2);

        let string_from_id: String = id1.into();
        assert_eq!(string_from_id, "test");
    }

    #[test]
    fn test_object_name_creation() {
        let name1 = ObjectName::bare("table");
        let name2 = ObjectName::from(vec!["schema", "table"]);
        let name3 = name1.clone().child("column");

        assert_eq!(name1.parts().len(), 1);
        assert_eq!(name2.parts().len(), 2);
        assert_eq!(name3.parts().len(), 2);

        let parts: Vec<String> = name2.into();
        assert_eq!(parts, vec!["schema".to_string(), "table".to_string()]);
    }

    #[test]
    fn test_sort_order_creation() {
        let sort = SortOrder {
            child: Box::new(Expr::new(ExprKind::Literal(Literal::Utf8 {
                value: Some("test".to_string()),
            }))),
            direction: SortDirection::Descending,
            null_ordering: NullOrdering::NullsLast,
        };

        assert_eq!(sort.direction, SortDirection::Descending);
        assert_eq!(sort.null_ordering, NullOrdering::NullsLast);
    }

    #[test]
    fn test_sort_direction_enum() {
        let directions = vec![
            SortDirection::Unspecified,
            SortDirection::Ascending,
            SortDirection::Descending,
        ];

        for direction in directions {
            // Test serialization
            let json = serde_json::to_string(&direction).expect("Failed to serialize");
            let deserialized: SortDirection =
                serde_json::from_str(&json).expect("Failed to deserialize");
            assert_eq!(direction, deserialized);
        }
    }

    #[test]
    fn test_null_ordering_enum() {
        let orderings = vec![
            NullOrdering::Unspecified,
            NullOrdering::NullsFirst,
            NullOrdering::NullsLast,
        ];

        for ordering in orderings {
            // Test serialization
            let json = serde_json::to_string(&ordering).expect("Failed to serialize");
            let deserialized: NullOrdering =
                serde_json::from_str(&json).expect("Failed to deserialize");
            assert_eq!(ordering, deserialized);
        }
    }

    #[test]
    fn test_unresolved_function_creation() {
        let func = UnresolvedFunction {
            function_name: ObjectName::bare("sum"),
            arguments: vec![
                Expr::new(ExprKind::Literal(Literal::Int32 { value: Some(1) })),
                Expr::new(ExprKind::Literal(Literal::Int32 { value: Some(2) })),
            ],
            named_arguments: vec![(
                Identifier::from("distinct"),
                Expr::new(ExprKind::Literal(Literal::Boolean { value: Some(true) })),
            )],
            is_distinct: true,
            is_user_defined_function: false,
            is_internal: Some(false),
            ignore_nulls: Some(true),
            filter: Some(Box::new(Expr::new(ExprKind::Literal(Literal::Boolean {
                value: Some(true),
            })))),
            order_by: Some(vec![SortOrder {
                child: Box::new(Expr::new(ExprKind::Literal(Literal::Int32 {
                    value: Some(1),
                }))),
                direction: SortDirection::Ascending,
                null_ordering: NullOrdering::NullsFirst,
            }]),
        };

        assert_eq!(func.function_name, ObjectName::bare("sum"));
        assert_eq!(func.arguments.len(), 2);
        assert_eq!(func.named_arguments.len(), 1);
        assert!(func.is_distinct);
        assert!(!func.is_user_defined_function);
        assert_eq!(func.is_internal, Some(false));
        assert_eq!(func.ignore_nulls, Some(true));
        assert!(func.filter.is_some());
        assert!(func.order_by.is_some());
    }

    #[test]
    fn test_window_frame_types() {
        let frame_types = vec![WindowFrameType::Row, WindowFrameType::Range];

        for frame_type in frame_types {
            // Test serialization
            let json = serde_json::to_string(&frame_type).expect("Failed to serialize");
            let deserialized: WindowFrameType =
                serde_json::from_str(&json).expect("Failed to deserialize");
            assert_eq!(frame_type, deserialized);
        }
    }

    #[test]
    fn test_window_frame_boundaries() {
        let boundaries = vec![
            WindowFrameBoundary::CurrentRow,
            WindowFrameBoundary::UnboundedPreceding,
            WindowFrameBoundary::UnboundedFollowing,
            WindowFrameBoundary::Preceding(Box::new(Expr::new(ExprKind::Literal(
                Literal::Int32 { value: Some(5) },
            )))),
            WindowFrameBoundary::Following(Box::new(Expr::new(ExprKind::Literal(
                Literal::Int32 { value: Some(3) },
            )))),
            WindowFrameBoundary::Value(Box::new(Expr::new(ExprKind::Literal(Literal::Int32 {
                value: Some(-2),
            })))),
        ];

        for boundary in boundaries {
            match boundary {
                WindowFrameBoundary::CurrentRow => {}
                WindowFrameBoundary::UnboundedPreceding => {}
                WindowFrameBoundary::UnboundedFollowing => {}
                WindowFrameBoundary::Preceding(_) => {}
                WindowFrameBoundary::Following(_) => {}
                WindowFrameBoundary::Value(_) => {}
            }
        }
    }

    #[test]
    fn test_window_variants() {
        let named_window: Window<NoMetadata> = Window::Named(Identifier::from("my_window"));
        let unnamed_window: Window<NoMetadata> = Window::Unnamed {
            cluster_by: vec![Expr::new(ExprKind::Literal(Literal::Utf8 {
                value: Some("cluster_col".to_string()),
            }))],
            partition_by: vec![Expr::new(ExprKind::Literal(Literal::Utf8 {
                value: Some("partition_col".to_string()),
            }))],
            order_by: vec![SortOrder {
                child: Box::new(Expr::new(ExprKind::Literal(Literal::Utf8 {
                    value: Some("order_col".to_string()),
                }))),
                direction: SortDirection::Descending,
                null_ordering: NullOrdering::NullsLast,
            }],
            frame: Some(WindowFrame {
                frame_type: WindowFrameType::Range,
                lower: WindowFrameBoundary::UnboundedPreceding,
                upper: WindowFrameBoundary::CurrentRow,
            }),
        };

        match named_window {
            Window::Named(name) => assert_eq!(name.as_ref(), "my_window"),
            _ => panic!("Expected named window"),
        }

        match unnamed_window {
            Window::Unnamed {
                cluster_by,
                partition_by,
                order_by,
                frame,
            } => {
                assert_eq!(cluster_by.len(), 1);
                assert_eq!(partition_by.len(), 1);
                assert_eq!(order_by.len(), 1);
                assert!(frame.is_some());
            }
            _ => panic!("Expected unnamed window"),
        }
    }

    #[test]
    fn test_wildcard_options() {
        let options = WildcardOptions {
            ilike_pattern: Some("test_*".to_string()),
            exclude_columns: Some(vec![Identifier::from("col1"), Identifier::from("col2")]),
            except_columns: Some(vec![Identifier::from("col3")]),
            replace_columns: Some(vec![WildcardReplaceColumn {
                expression: Box::new(Expr::new(ExprKind::Literal(Literal::Utf8 {
                    value: Some("replacement".to_string()),
                }))),
                column_name: Identifier::from("old_col"),
                as_keyword: true,
            }]),
            rename_columns: Some(vec![IdentifierWithAlias {
                identifier: Identifier::from("old_name"),
                alias: Identifier::from("new_name"),
            }]),
        };

        assert_eq!(options.ilike_pattern, Some("test_*".to_string()));
        assert_eq!(options.exclude_columns.as_ref().unwrap().len(), 2);
        assert_eq!(options.except_columns.as_ref().unwrap().len(), 1);
        assert_eq!(options.replace_columns.as_ref().unwrap().len(), 1);
        assert_eq!(options.rename_columns.as_ref().unwrap().len(), 1);
    }

    // --- Conversion Tests ---

    #[test]
    fn test_expr_conversion() {
        // Test basic literal conversion
        let literal_expr = Expr {
            kind: ExprKind::Literal(Literal::Int32 { value: Some(42) }),
            metadata: NoMetadata,
        };

        let serializable_expr: serializable::Expr = literal_expr.into();
        match serializable_expr {
            serializable::Expr::Literal(Literal::Int32 { value: Some(42) }) => {}
            _ => panic!("Expected literal conversion to work"),
        }
    }

    #[test]
    fn test_sort_order_conversion() {
        let sort_order = SortOrder {
            child: Box::new(Expr {
                kind: ExprKind::Literal(Literal::Utf8 {
                    value: Some("test".to_string()),
                }),
                metadata: NoMetadata,
            }),
            direction: SortDirection::Ascending,
            null_ordering: NullOrdering::NullsFirst,
        };

        let serializable_sort: serializable::SortOrder = sort_order.into();
        assert_eq!(serializable_sort.direction, SortDirection::Ascending);
        assert_eq!(serializable_sort.null_ordering, NullOrdering::NullsFirst);
    }

    #[test]
    fn test_unresolved_function_conversion() {
        let func = UnresolvedFunction {
            function_name: ObjectName::bare("test_func"),
            arguments: vec![Expr {
                kind: ExprKind::Literal(Literal::Int32 { value: Some(1) }),
                metadata: NoMetadata,
            }],
            named_arguments: vec![],
            is_distinct: false,
            is_user_defined_function: false,
            is_internal: None,
            ignore_nulls: None,
            filter: None,
            order_by: None,
        };

        let serializable_func: serializable::UnresolvedFunction = func.into();
        assert_eq!(
            serializable_func.function_name,
            ObjectName::bare("test_func")
        );
        assert_eq!(serializable_func.arguments.len(), 1);
        assert!(!serializable_func.is_distinct);
    }

    #[test]
    fn test_wildcard_options_conversion() {
        let options: WildcardOptions<NoMetadata> = WildcardOptions {
            ilike_pattern: Some("test*".to_string()),
            exclude_columns: Some(vec![Identifier::from("col1")]),
            except_columns: None,
            replace_columns: None,
            rename_columns: None,
        };

        let serializable_options: serializable::WildcardOptions = options.into();
        assert_eq!(
            serializable_options.ilike_pattern,
            Some("test*".to_string())
        );
        assert_eq!(serializable_options.exclude_columns.unwrap().len(), 1);
    }

    #[test]
    fn test_complex_expr_conversion() {
        // Test complex expression with nested structures
        let complex_expr = Expr {
            kind: ExprKind::Window {
                window_function: Box::new(Expr {
                    kind: ExprKind::UnresolvedFunction(UnresolvedFunction {
                        function_name: ObjectName::bare("row_number"),
                        arguments: vec![],
                        named_arguments: vec![],
                        is_distinct: false,
                        is_user_defined_function: false,
                        is_internal: None,
                        ignore_nulls: None,
                        filter: None,
                        order_by: None,
                    }),
                    metadata: NoMetadata,
                }),
                window: Window::Unnamed {
                    cluster_by: vec![],
                    partition_by: vec![Expr {
                        kind: ExprKind::UnresolvedAttribute {
                            name: ObjectName::bare("department"),
                            plan_id: None,
                            is_metadata_column: false,
                        },
                        metadata: NoMetadata,
                    }],
                    order_by: vec![SortOrder {
                        child: Box::new(Expr {
                            kind: ExprKind::UnresolvedAttribute {
                                name: ObjectName::bare("salary"),
                                plan_id: None,
                                is_metadata_column: false,
                            },
                            metadata: NoMetadata,
                        }),
                        direction: SortDirection::Descending,
                        null_ordering: NullOrdering::NullsLast,
                    }],
                    frame: None,
                },
            },
            metadata: NoMetadata,
        };

        let serializable_expr: serializable::Expr = complex_expr.into();
        match serializable_expr {
            serializable::Expr::Window {
                window_function,
                window,
            } => {
                match *window_function {
                    serializable::Expr::UnresolvedFunction(_) => {}
                    _ => panic!("Expected unresolved function in window"),
                }
                match window {
                    serializable::Window::Unnamed { partition_by, .. } => {
                        assert_eq!(partition_by.len(), 1);
                    }
                    _ => panic!("Expected unnamed window"),
                }
            }
            _ => panic!("Expected window expression"),
        }
    }

    #[test]
    fn test_expr_kinds_subquery() {
        use crate::logical_plan::{QueryNode, QueryPlan, Range};

        let subquery_plan = QueryPlan::new(QueryNode::Range(Range {
            start: Some(1),
            end: 10,
            step: 1,
            num_partitions: None,
        }));

        let scalar_subquery: ExprKind<NoMetadata> = ExprKind::ScalarSubquery {
            subquery: Box::new(subquery_plan.clone()),
        };

        let exists_expr: ExprKind<NoMetadata> = ExprKind::Exists {
            subquery: Box::new(subquery_plan.clone()),
            negated: false,
        };

        let in_subquery: ExprKind<NoMetadata> = ExprKind::InSubquery {
            expr: Box::new(Expr::new(ExprKind::Literal(Literal::Int32 {
                value: Some(5),
            }))),
            subquery: Box::new(subquery_plan),
            negated: true,
        };

        match scalar_subquery {
            ExprKind::ScalarSubquery { .. } => {}
            _ => panic!("Expected scalar subquery"),
        }

        match exists_expr {
            ExprKind::Exists { negated, .. } => assert!(!negated),
            _ => panic!("Expected exists expression"),
        }

        match in_subquery {
            ExprKind::InSubquery { negated, .. } => assert!(negated),
            _ => panic!("Expected in subquery"),
        }
    }

    #[test]
    fn test_expr_kinds_grouping() {
        let rollup_expr: ExprKind<NoMetadata> = ExprKind::Rollup(vec![
            Expr::new(ExprKind::Literal(Literal::Utf8 {
                value: Some("col1".to_string()),
            })),
            Expr::new(ExprKind::Literal(Literal::Utf8 {
                value: Some("col2".to_string()),
            })),
        ]);

        let cube_expr: ExprKind<NoMetadata> =
            ExprKind::Cube(vec![Expr::new(ExprKind::Literal(Literal::Utf8 {
                value: Some("col1".to_string()),
            }))]);

        let grouping_sets: ExprKind<NoMetadata> = ExprKind::GroupingSets(vec![
            vec![Expr::new(ExprKind::Literal(Literal::Utf8 {
                value: Some("col1".to_string()),
            }))],
            vec![Expr::new(ExprKind::Literal(Literal::Utf8 {
                value: Some("col2".to_string()),
            }))],
        ]);

        match rollup_expr {
            ExprKind::Rollup(exprs) => assert_eq!(exprs.len(), 2),
            _ => panic!("Expected rollup expression"),
        }

        match cube_expr {
            ExprKind::Cube(exprs) => assert_eq!(exprs.len(), 1),
            _ => panic!("Expected cube expression"),
        }

        match grouping_sets {
            ExprKind::GroupingSets(sets) => assert_eq!(sets.len(), 2),
            _ => panic!("Expected grouping sets expression"),
        }
    }
}
