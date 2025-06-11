//! Logical query plan definitions for Barks.
//!
//! Generic plan system supporting metadata attachment.
//! Based on Sail "https://github.com/lakehq/sail"

use crate::expression::{CommonInlineUserDefinedFunction, Expr, Identifier, ObjectName, SortOrder};
use crate::literal::Literal;
use crate::storage::StorageLevel;
use crate::types::{DataTypeMetadata, NoMetadata, Schema};

// --- Generic Plan Representation ---

/// Top-level plan structure, can be a query or a command.
#[derive(Debug, Clone, PartialEq)]
pub enum Plan<M: DataTypeMetadata = NoMetadata> {
    Query(QueryPlan<M>),
    Command(CommandPlan<M>),
}

/// A query plan node with attached metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryPlan<M: DataTypeMetadata = NoMetadata> {
    pub node: QueryNode<M>,
    pub plan_id: Option<i64>,
    pub metadata: M,
}

impl QueryPlan<NoMetadata> {
    pub fn new(node: QueryNode<NoMetadata>) -> Self {
        Self {
            node,
            plan_id: None,
            metadata: NoMetadata,
        }
    }
}

/// A command plan node with attached metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct CommandPlan<M: DataTypeMetadata = NoMetadata> {
    pub node: CommandNode<M>,
    pub plan_id: Option<i64>,
    pub metadata: M,
}

impl CommandPlan<NoMetadata> {
    pub fn new(node: CommandNode<NoMetadata>) -> Self {
        Self {
            node,
            plan_id: None,
            metadata: NoMetadata,
        }
    }
}

/// Core query node variants with optional metadata.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryNode<M: DataTypeMetadata = NoMetadata> {
    Read {
        read_type: ReadType<M>,
        is_streaming: bool,
    },
    Project {
        input: Option<Box<QueryPlan<M>>>,
        expressions: Vec<Expr<M>>,
    },
    Filter {
        input: Box<QueryPlan<M>>,
        condition: Expr<M>,
    },
    Join(Join<M>),
    SetOperation(SetOperation<M>),
    Sort {
        input: Box<QueryPlan<M>>,
        order: Vec<SortOrder<M>>,
        is_global: bool,
    },
    Limit {
        input: Box<QueryPlan<M>>,
        skip: Option<Expr<M>>,
        limit: Option<Expr<M>>,
    },
    Aggregate(Aggregate<M>),
    LocalRelation {
        data: Option<Vec<u8>>,
        schema: Option<Schema<M>>,
    },
    Sample(Sample<M>),
    Deduplicate(Deduplicate<M>),
    Range(Range),
    SubqueryAlias {
        input: Box<QueryPlan<M>>,
        alias: Identifier,
        qualifier: Vec<Identifier>,
    },
    Repartition {
        input: Box<QueryPlan<M>>,
        num_partitions: usize,
        shuffle: bool,
    },
    ToDf {
        input: Box<QueryPlan<M>>,
        column_names: Vec<Identifier>,
    },
    WithColumnsRenamed {
        input: Box<QueryPlan<M>>,
        rename_columns_map: Vec<(Identifier, Identifier)>,
    },
    Drop {
        input: Box<QueryPlan<M>>,
        columns: Vec<Expr<M>>,
        column_names: Vec<Identifier>,
    },
    Tail {
        input: Box<QueryPlan<M>>,
        limit: Expr<M>,
    },
    WithColumns {
        input: Box<QueryPlan<M>>,
        aliases: Vec<Expr<M>>,
    },
    Hint {
        input: Box<QueryPlan<M>>,
        name: String,
        parameters: Vec<Expr<M>>,
    },
    Pivot(Pivot<M>),
    Unpivot(Unpivot<M>),
    ToSchema {
        input: Box<QueryPlan<M>>,
        schema: Schema<M>,
    },
    RepartitionByExpression {
        input: Box<QueryPlan<M>>,
        partition_expressions: Vec<Expr<M>>,
        num_partitions: Option<usize>,
    },
    MapPartitions {
        input: Box<QueryPlan<M>>,
        function: CommonInlineUserDefinedFunction<M>,
        is_barrier: bool,
    },
    CollectMetrics {
        input: Box<QueryPlan<M>>,
        name: String,
        metrics: Vec<Expr<M>>,
    },
    Parse(Parse<M>),
    GroupMap(GroupMap<M>),
    CoGroupMap(CoGroupMap<M>),
    WithWatermark(WithWatermark<M>),
    ApplyInPandasWithState(ApplyInPandasWithState<M>),
    CachedLocalRelation {
        hash: String,
    },
    CachedRemoteRelation {
        relation_id: String,
    },
    CommonInlineUserDefinedTableFunction(
        crate::expression::CommonInlineUserDefinedTableFunction<M>,
    ),
    // NA operations
    FillNa {
        input: Box<QueryPlan<M>>,
        columns: Vec<Identifier>,
        values: Vec<Expr<M>>,
    },
    DropNa {
        input: Box<QueryPlan<M>>,
        columns: Vec<Identifier>,
        min_non_nulls: Option<usize>,
    },
    Replace {
        input: Box<QueryPlan<M>>,
        columns: Vec<Identifier>,
        replacements: Vec<Replacement>,
    },
    // stat operations
    StatSummary {
        input: Box<QueryPlan<M>>,
        statistics: Vec<String>,
    },
    StatDescribe {
        input: Box<QueryPlan<M>>,
        columns: Vec<Identifier>,
    },
    StatCrosstab {
        input: Box<QueryPlan<M>>,
        left_column: Identifier,
        right_column: Identifier,
    },
    StatCov {
        input: Box<QueryPlan<M>>,
        left_column: Identifier,
        right_column: Identifier,
    },
    StatCorr {
        input: Box<QueryPlan<M>>,
        left_column: Identifier,
        right_column: Identifier,
        method: String,
    },
    StatApproxQuantile {
        input: Box<QueryPlan<M>>,
        columns: Vec<Identifier>,
        probabilities: Vec<f64>,
        relative_error: f64,
    },
    StatFreqItems {
        input: Box<QueryPlan<M>>,
        columns: Vec<Identifier>,
        support: Option<f64>,
    },
    StatSampleBy {
        input: Box<QueryPlan<M>>,
        column: Expr<M>,
        fractions: Vec<Fraction>,
        seed: Option<i64>,
    },
    // extensions
    Empty {
        produce_one_row: bool,
    },
    WithParameters {
        input: Box<QueryPlan<M>>,
        positional_arguments: Vec<Expr<M>>,
        named_arguments: Vec<(String, Expr<M>)>,
    },
    Values(Vec<Vec<Expr<M>>>),
    TableAlias {
        input: Box<QueryPlan<M>>,
        name: Identifier,
        columns: Vec<Identifier>,
    },
    WithCtes {
        input: Box<QueryPlan<M>>,
        recursive: bool,
        ctes: Vec<(Identifier, QueryPlan<M>)>,
    },
    LateralView {
        input: Option<Box<QueryPlan<M>>>,
        function: ObjectName,
        arguments: Vec<Expr<M>>,
        named_arguments: Vec<(Identifier, Expr<M>)>,
        table_alias: Option<ObjectName>,
        column_aliases: Option<Vec<Identifier>>,
        outer: bool,
    },
}

// --- Supporting Structs (Generic) ---

#[derive(Debug, Clone, PartialEq)]
pub struct Join<M: DataTypeMetadata = NoMetadata> {
    pub left: Box<QueryPlan<M>>,
    pub right: Box<QueryPlan<M>>,
    pub join_type: JoinType,
    pub join_criteria: Option<JoinCriteria<M>>,
    pub join_data_type: Option<JoinDataType>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinCriteria<M: DataTypeMetadata = NoMetadata> {
    Natural,
    On(Box<Expr<M>>),
    Using(Vec<Identifier>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Aggregate<M: DataTypeMetadata = NoMetadata> {
    pub input: Box<QueryPlan<M>>,
    pub grouping: Vec<Expr<M>>,
    pub aggregate: Vec<Expr<M>>,
    pub having: Option<Expr<M>>,
    /// Whether the grouping expressions should be added to the projection.
    pub with_grouping_expressions: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Write<M: DataTypeMetadata = NoMetadata> {
    pub input: Box<QueryPlan<M>>,
    pub source: Option<String>,
    pub save_type: SaveType,
    pub mode: SaveMode,
    pub sort_column_names: Vec<String>,
    pub partitioning_columns: Vec<String>,
    pub bucket_by: Option<BucketBy>,
    pub options: Vec<(String, String)>,
    pub clustering_columns: Vec<String>,
    pub overwrite_condition: Option<Expr<M>>,
}

// --- Concrete Supporting Enums & Structs ---
// These do not hold expressions or plans, so they can remain concrete.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum JoinType {
    Inner,
    FullOuter,
    LeftOuter,
    RightOuter,
    LeftAnti,
    LeftSemi,
    RightSemi,
    RightAnti,
    Cross,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct JoinDataType {
    pub is_asof_join: bool,
    pub using_columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum SaveType {
    Append,
    Overwrite,
    ErrorIfExists,
    Ignore,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum SaveMode {
    Append,
    Overwrite,
    ErrorIfExists,
    Ignore,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct BucketBy {
    pub bucket_column_names: Vec<String>,
    pub num_buckets: i32,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Fraction {
    pub key: Literal,
    pub fraction: f64,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Replacement {
    pub old_value: Literal,
    pub new_value: Literal,
}

// --- More Generic Supporting Structs ---

#[derive(Debug, Clone, PartialEq)]
pub struct SetOperation<M: DataTypeMetadata = NoMetadata> {
    pub left: Box<QueryPlan<M>>,
    pub right: Box<QueryPlan<M>>,
    pub set_op_type: SetOpType,
    pub is_all: bool,
    pub by_name: bool,
    pub allow_missing_columns: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Sample<M: DataTypeMetadata = NoMetadata> {
    pub input: Box<QueryPlan<M>>,
    pub lower_bound: f64,
    pub upper_bound: f64,
    pub with_replacement: bool,
    pub seed: Option<i64>,
    pub deterministic_order: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Deduplicate<M: DataTypeMetadata = NoMetadata> {
    pub input: Box<QueryPlan<M>>,
    pub column_names: Vec<Identifier>,
    pub all_columns_as_keys: bool,
    pub within_watermark: bool,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Range {
    pub start: Option<i64>,
    pub end: i64,
    pub step: i64,
    pub num_partitions: Option<usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Pivot<M: DataTypeMetadata = NoMetadata> {
    pub input: Box<QueryPlan<M>>,
    /// The group-by columns for the pivot operation (only supported in the DataFrame API).
    /// When the list is empty (for SQL statements), all the remaining columns are included.
    pub grouping: Vec<Expr<M>>,
    pub aggregate: Vec<Expr<M>>,
    pub columns: Vec<Expr<M>>,
    pub values: Vec<PivotValue>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PivotValue {
    pub values: Vec<Literal>,
    pub alias: Option<Identifier>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Unpivot<M: DataTypeMetadata = NoMetadata> {
    pub input: Box<QueryPlan<M>>,
    /// When `ids` is [None] (for SQL statements), all remaining columns are included.
    /// When `ids` is [Some] (for the DataFrame API), only the specified columns are included.
    pub ids: Option<Vec<Expr<M>>>,
    pub values: Vec<UnpivotValue<M>>,
    pub variable_column_name: Identifier,
    pub value_column_names: Vec<Identifier>,
    pub include_nulls: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UnpivotValue<M: DataTypeMetadata = NoMetadata> {
    pub columns: Vec<Expr<M>>,
    pub alias: Option<Identifier>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Parse<M: DataTypeMetadata = NoMetadata> {
    pub input: Box<QueryPlan<M>>,
    pub format: ParseFormat,
    pub schema: Option<Schema<M>>,
    pub options: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GroupMap<M: DataTypeMetadata = NoMetadata> {
    pub input: Box<QueryPlan<M>>,
    pub grouping_expressions: Vec<Expr<M>>,
    pub function: CommonInlineUserDefinedFunction<M>,
    pub sorting_expressions: Vec<Expr<M>>,
    pub initial_input: Option<Box<QueryPlan<M>>>,
    pub initial_grouping_expressions: Vec<Expr<M>>,
    pub is_map_groups_with_state: Option<bool>,
    pub output_mode: Option<String>,
    pub timeout_conf: Option<String>,
    pub state_schema: Option<Schema<M>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CoGroupMap<M: DataTypeMetadata = NoMetadata> {
    pub input: Box<QueryPlan<M>>,
    pub input_grouping_expressions: Vec<Expr<M>>,
    pub other: Box<QueryPlan<M>>,
    pub other_grouping_expressions: Vec<Expr<M>>,
    pub function: CommonInlineUserDefinedFunction<M>,
    pub input_sorting_expressions: Vec<Expr<M>>,
    pub other_sorting_expressions: Vec<Expr<M>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WithWatermark<M: DataTypeMetadata = NoMetadata> {
    pub input: Box<QueryPlan<M>>,
    pub event_time: String,
    pub delay_threshold: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ApplyInPandasWithState<M: DataTypeMetadata = NoMetadata> {
    pub input: Box<QueryPlan<M>>,
    pub grouping_expressions: Vec<Expr<M>>,
    pub function: CommonInlineUserDefinedFunction<M>,
    pub output_schema: Schema<M>,
    pub state_schema: Schema<M>,
    pub output_mode: String,
    pub timeout_conf: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ShowString {
    pub num_rows: usize,
    pub truncate: usize,
    pub vertical: bool,
}

// --- Concrete Supporting Enums ---

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum SetOpType {
    Intersect,
    Union,
    Except,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum ParseFormat {
    Unspecified,
    Csv,
    Json,
}

// --- ReadType Definition ---

#[derive(Debug, Clone, PartialEq)]
pub enum ReadType<M: DataTypeMetadata = NoMetadata> {
    NamedTable {
        unparsed_identifier: String,
        options: Vec<(String, String)>,
    },
    DataSource {
        format: Option<String>,
        schema: Option<Schema<M>>,
        options: Vec<(String, String)>,
        paths: Vec<String>,
        predicates: Vec<Expr<M>>,
    },
}

// --- Command Node Definition ---

/// Core command node variants with optional metadata.
#[derive(Debug, Clone, PartialEq)]
pub enum CommandNode<M: DataTypeMetadata = NoMetadata> {
    ShowString(ShowString),
    HtmlString {
        input: Box<QueryPlan<M>>,
        num_rows: usize,
        truncate: usize,
    },
    // catalog operations
    CurrentDatabase,
    SetCurrentDatabase {
        database_name: Identifier,
    },
    ListDatabases {
        catalog: Option<Identifier>,
        database_pattern: Option<String>,
    },
    ListTables {
        database: Option<ObjectName>,
        table_pattern: Option<String>,
    },
    ListFunctions {
        database: Option<ObjectName>,
        function_pattern: Option<String>,
    },
    ListColumns {
        table: ObjectName,
    },
    GetDatabase {
        database: ObjectName,
    },
    GetTable {
        table: ObjectName,
    },
    GetFunction {
        function: ObjectName,
    },
    DatabaseExists {
        database: ObjectName,
    },
    TableExists {
        table: ObjectName,
    },
    FunctionExists {
        function: ObjectName,
    },
    CreateTable {
        table: ObjectName,
        definition: TableDefinition<M>,
    },
    RecoverPartitions {
        table: ObjectName,
    },
    IsCached {
        table: ObjectName,
    },
    CacheTable {
        table: ObjectName,
        lazy: bool,
        storage_level: Option<StorageLevel>,
        query: Option<Box<QueryPlan<M>>>,
    },
    UncacheTable {
        table: ObjectName,
        if_exists: bool,
    },
    ClearCache,
    RefreshTable {
        table: ObjectName,
    },
    RefreshByPath {
        path: String,
    },
    CurrentCatalog,
    SetCurrentCatalog {
        catalog_name: Identifier,
    },
    ListCatalogs {
        catalog_pattern: Option<String>,
    },
    CreateCatalog {
        catalog: Identifier,
        definition: CatalogDefinition,
    },
    // commands
    CreateDatabase {
        database: ObjectName,
        definition: DatabaseDefinition,
    },
    DropDatabase {
        database: ObjectName,
        if_exists: bool,
        cascade: bool,
    },
    RegisterFunction(CommonInlineUserDefinedFunction<M>),
    RegisterTableFunction(crate::expression::CommonInlineUserDefinedTableFunction<M>),
    RefreshFunction {
        function: ObjectName,
    },
    DropFunction {
        function: ObjectName,
        if_exists: bool,
        is_temporary: bool,
    },
    DropTable {
        table: ObjectName,
        if_exists: bool,
        purge: bool,
    },
    CreateView {
        view: ObjectName,
        definition: ViewDefinition<M>,
    },
    DropView {
        view: ObjectName,
        /// An optional view kind to match the view name.
        kind: Option<ViewKind>,
        if_exists: bool,
    },
    Write(Write<M>),
    Explain {
        mode: ExplainMode,
        input: Box<QueryPlan<M>>,
    },
    InsertInto {
        input: Box<QueryPlan<M>>,
        table: ObjectName,
        columns: Vec<Identifier>,
        partition_spec: Vec<(Identifier, Option<Expr<M>>)>,
        replace: Option<Expr<M>>,
        if_not_exists: bool,
        overwrite: bool,
    },
    InsertOverwriteDirectory {
        input: Box<QueryPlan<M>>,
        local: bool,
        location: Option<String>,
        file_format: Option<TableFileFormat>,
        row_format: Option<TableRowFormat>,
        options: Vec<(String, String)>,
    },
    MergeInto {
        target: ObjectName,
        with_schema_evolution: bool,
        // TODO: add other fields
    },
    SetVariable {
        variable: String,
        value: String,
    },
    ResetVariable {
        variable: Option<String>,
    },
    AlterTable {
        table: ObjectName,
        operation: AlterTableOperation,
    },
    AlterView {
        view: ObjectName,
        operation: AlterViewOperation,
    },
}

// --- Supporting Definitions ---

#[derive(Debug, Clone, PartialEq)]
pub struct TableDefinition<M: DataTypeMetadata = NoMetadata> {
    pub schema: Schema<M>,
    pub comment: Option<String>,
    pub column_defaults: Vec<(Identifier, Expr<M>)>,
    pub constraints: Vec<TableConstraint>,
    pub location: Option<String>,
    pub file_format: Option<TableFileFormat>,
    pub row_format: Option<TableRowFormat>,
    pub table_partition_cols: Vec<Identifier>,
    pub file_sort_order: Vec<Vec<SortOrder<M>>>,
    pub if_not_exists: bool,
    pub or_replace: bool,
    pub unbounded: bool,
    pub options: Vec<(String, String)>,
    /// The query for `CREATE TABLE ... AS SELECT ...` (CTAS) statements.
    pub query: Option<Box<QueryPlan<M>>>,
    pub definition: Option<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CatalogDefinition {
    pub comment: Option<String>,
    pub options: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DatabaseDefinition {
    pub comment: Option<String>,
    pub location: Option<String>,
    pub options: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ViewKind {
    /// A view that is stored in the catalog.
    Default,
    /// A temporary view that is tied to the session.
    Temporary,
    /// A global temporary view that is tied to the session.
    GlobalTemporary,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ViewDefinition<M: DataTypeMetadata = NoMetadata> {
    pub input: Box<QueryPlan<M>>,
    pub columns: Option<Vec<Identifier>>,
    pub kind: ViewKind,
    pub replace: bool,
    pub definition: Option<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ExplainMode {
    Unspecified,
    Simple,
    Analyze,
    Codegen,
    Cost,
    Formatted,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum TableFileFormat {
    General {
        format: String,
    },
    Hive {
        input_format: String,
        output_format: String,
    },
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum TableRowFormat {
    Serde {
        name: String,
        properties: Vec<(String, String)>,
    },
    Delimited {
        fields_terminated_by: Option<String>,
        escaped_by: Option<String>,
        collection_items_terminated_by: Option<String>,
        map_keys_terminated_by: Option<String>,
        lines_terminated_by: Option<String>,
        null_defined_as: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum TableConstraint {
    Unique {
        name: Option<Identifier>,
        columns: Vec<Identifier>,
    },
    PrimaryKey {
        name: Option<Identifier>,
        columns: Vec<Identifier>,
    },
    ForeignKey {
        name: Option<Identifier>,
        columns: Vec<Identifier>,
        foreign_table: ObjectName,
        referred_columns: Vec<Identifier>,
    },
    Check {
        name: Option<Identifier>,
        expr: String, // Simplified for now - would need full expression serialization
    },
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum AlterTableOperation {
    Unknown,
    // TODO: add all the alter table operations
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum AlterViewOperation {
    Unknown,
    // TODO: add all the alter view operations
}

// --- Serializable Plan Representation ---

/// Serializable versions of plan types for JSON/binary serialization.
pub mod serializable {
    use super::{
        BucketBy, Fraction, JoinDataType, JoinType, Range, Replacement, SaveMode, SaveType,
    };
    use crate::expression::serializable::{CommonInlineUserDefinedTableFunction, Expr, SortOrder};
    use crate::expression::{Identifier, ObjectName};
    use crate::types::serializable::Schema;
    use serde::{Deserialize, Serialize};

    /// Serializable version of Plan with NoMetadata
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
    pub enum Plan {
        Query(QueryPlan),
        Command(CommandPlan),
    }

    /// Serializable version of QueryPlan with NoMetadata
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct QueryPlan {
        #[serde(flatten)]
        pub node: QueryNode,
        pub plan_id: Option<i64>,
    }

    /// Serializable version of CommandPlan with NoMetadata
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct CommandPlan {
        #[serde(flatten)]
        pub node: CommandNode,
        pub plan_id: Option<i64>,
    }

    /// Serializable version of QueryNode with NoMetadata
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
    pub enum QueryNode {
        Read {
            read_type: ReadType,
            is_streaming: bool,
        },
        Project {
            input: Option<Box<QueryPlan>>,
            expressions: Vec<Expr>,
        },
        Filter {
            input: Box<QueryPlan>,
            condition: Expr,
        },
        Join(Join),
        SetOperation(SetOperation),
        Sort {
            input: Box<QueryPlan>,
            order: Vec<SortOrder>,
            is_global: bool,
        },
        Limit {
            input: Box<QueryPlan>,
            skip: Option<Expr>,
            limit: Option<Expr>,
        },
        Aggregate(Aggregate),
        LocalRelation {
            data: Option<Vec<u8>>,
            schema: Option<Schema>,
        },
        Sample(Sample),
        Deduplicate(Deduplicate),
        Range(Range),
        SubqueryAlias {
            input: Box<QueryPlan>,
            alias: Identifier,
            qualifier: Vec<Identifier>,
        },
        Repartition {
            input: Box<QueryPlan>,
            num_partitions: usize,
            shuffle: bool,
        },
        ToDf {
            input: Box<QueryPlan>,
            column_names: Vec<Identifier>,
        },
        WithColumnsRenamed {
            input: Box<QueryPlan>,
            rename_columns_map: Vec<(Identifier, Identifier)>,
        },
        Drop {
            input: Box<QueryPlan>,
            columns: Vec<Expr>,
            column_names: Vec<Identifier>,
        },
        Tail {
            input: Box<QueryPlan>,
            limit: Expr,
        },
        WithColumns {
            input: Box<QueryPlan>,
            aliases: Vec<Expr>,
        },
        Hint {
            input: Box<QueryPlan>,
            name: String,
            parameters: Vec<Expr>,
        },
        Pivot(Pivot),
        Unpivot(Unpivot),
        ToSchema {
            input: Box<QueryPlan>,
            schema: Schema,
        },
        RepartitionByExpression {
            input: Box<QueryPlan>,
            partition_expressions: Vec<Expr>,
            num_partitions: Option<usize>,
        },
        MapPartitions {
            input: Box<QueryPlan>,
            function: crate::expression::serializable::CommonInlineUserDefinedFunction,
            is_barrier: bool,
        },
        CollectMetrics {
            input: Box<QueryPlan>,
            name: String,
            metrics: Vec<Expr>,
        },
        Parse(Parse),
        GroupMap(GroupMap),
        CoGroupMap(CoGroupMap),
        WithWatermark(WithWatermark),
        ApplyInPandasWithState(ApplyInPandasWithState),
        CachedLocalRelation {
            hash: String,
        },
        CachedRemoteRelation {
            relation_id: String,
        },
        CommonInlineUserDefinedTableFunction(CommonInlineUserDefinedTableFunction),
        // NA operations
        FillNa {
            input: Box<QueryPlan>,
            columns: Vec<Identifier>,
            values: Vec<Expr>,
        },
        DropNa {
            input: Box<QueryPlan>,
            columns: Vec<Identifier>,
            min_non_nulls: Option<usize>,
        },
        Replace {
            input: Box<QueryPlan>,
            columns: Vec<Identifier>,
            replacements: Vec<Replacement>,
        },
        // stat operations
        StatSummary {
            input: Box<QueryPlan>,
            statistics: Vec<String>,
        },
        StatDescribe {
            input: Box<QueryPlan>,
            columns: Vec<Identifier>,
        },
        StatCrosstab {
            input: Box<QueryPlan>,
            left_column: Identifier,
            right_column: Identifier,
        },
        StatCov {
            input: Box<QueryPlan>,
            left_column: Identifier,
            right_column: Identifier,
        },
        StatCorr {
            input: Box<QueryPlan>,
            left_column: Identifier,
            right_column: Identifier,
            method: String,
        },
        StatApproxQuantile {
            input: Box<QueryPlan>,
            columns: Vec<Identifier>,
            probabilities: Vec<f64>,
            relative_error: f64,
        },
        StatFreqItems {
            input: Box<QueryPlan>,
            columns: Vec<Identifier>,
            support: Option<f64>,
        },
        StatSampleBy {
            input: Box<QueryPlan>,
            column: Expr,
            fractions: Vec<Fraction>,
            seed: Option<i64>,
        },
        // extensions
        Empty {
            produce_one_row: bool,
        },
        WithParameters {
            input: Box<QueryPlan>,
            positional_arguments: Vec<Expr>,
            named_arguments: Vec<(String, Expr)>,
        },
        Values(Vec<Vec<Expr>>),
        TableAlias {
            input: Box<QueryPlan>,
            name: Identifier,
            columns: Vec<Identifier>,
        },
        WithCtes {
            input: Box<QueryPlan>,
            recursive: bool,
            ctes: Vec<(Identifier, QueryPlan)>,
        },
        LateralView {
            input: Option<Box<QueryPlan>>,
            function: ObjectName,
            arguments: Vec<Expr>,
            named_arguments: Vec<(Identifier, Expr)>,
            table_alias: Option<ObjectName>,
            column_aliases: Option<Vec<Identifier>>,
            outer: bool,
        },
    }

    // Supporting serializable structs
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Join {
        pub left: Box<QueryPlan>,
        pub right: Box<QueryPlan>,
        pub join_type: JoinType,
        pub join_criteria: Option<JoinCriteria>,
        pub join_data_type: Option<JoinDataType>,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
    pub enum JoinCriteria {
        Natural,
        On(Box<Expr>),
        Using(Vec<Identifier>),
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Aggregate {
        pub input: Box<QueryPlan>,
        pub grouping: Vec<Expr>,
        pub aggregate: Vec<Expr>,
        pub having: Option<Expr>,
        pub with_grouping_expressions: bool,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Write {
        pub input: Box<QueryPlan>,
        pub source: Option<String>,
        pub save_type: SaveType,
        pub mode: SaveMode,
        pub sort_column_names: Vec<String>,
        pub partitioning_columns: Vec<String>,
        pub bucket_by: Option<BucketBy>,
        pub options: Vec<(String, String)>,
        pub clustering_columns: Vec<String>,
        pub overwrite_condition: Option<Expr>,
    }

    // More serializable supporting structs
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct SetOperation {
        pub left: Box<QueryPlan>,
        pub right: Box<QueryPlan>,
        pub set_op_type: super::SetOpType,
        pub is_all: bool,
        pub by_name: bool,
        pub allow_missing_columns: bool,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Sample {
        pub input: Box<QueryPlan>,
        pub lower_bound: f64,
        pub upper_bound: f64,
        pub with_replacement: bool,
        pub seed: Option<i64>,
        pub deterministic_order: bool,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Deduplicate {
        pub input: Box<QueryPlan>,
        pub column_names: Vec<Identifier>,
        pub all_columns_as_keys: bool,
        pub within_watermark: bool,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Pivot {
        pub input: Box<QueryPlan>,
        pub grouping: Vec<Expr>,
        pub aggregate: Vec<Expr>,
        pub columns: Vec<Expr>,
        pub values: Vec<super::PivotValue>,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Unpivot {
        pub input: Box<QueryPlan>,
        pub ids: Option<Vec<Expr>>,
        pub values: Vec<UnpivotValue>,
        pub variable_column_name: Identifier,
        pub value_column_names: Vec<Identifier>,
        pub include_nulls: bool,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct UnpivotValue {
        pub columns: Vec<Expr>,
        pub alias: Option<Identifier>,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Parse {
        pub input: Box<QueryPlan>,
        pub format: super::ParseFormat,
        pub schema: Option<Schema>,
        pub options: Vec<(String, String)>,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct GroupMap {
        pub input: Box<QueryPlan>,
        pub grouping_expressions: Vec<Expr>,
        pub function: crate::expression::serializable::CommonInlineUserDefinedFunction,
        pub sorting_expressions: Vec<Expr>,
        pub initial_input: Option<Box<QueryPlan>>,
        pub initial_grouping_expressions: Vec<Expr>,
        pub is_map_groups_with_state: Option<bool>,
        pub output_mode: Option<String>,
        pub timeout_conf: Option<String>,
        pub state_schema: Option<Schema>,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct CoGroupMap {
        pub input: Box<QueryPlan>,
        pub input_grouping_expressions: Vec<Expr>,
        pub other: Box<QueryPlan>,
        pub other_grouping_expressions: Vec<Expr>,
        pub function: crate::expression::serializable::CommonInlineUserDefinedFunction,
        pub input_sorting_expressions: Vec<Expr>,
        pub other_sorting_expressions: Vec<Expr>,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct WithWatermark {
        pub input: Box<QueryPlan>,
        pub event_time: String,
        pub delay_threshold: String,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct ApplyInPandasWithState {
        pub input: Box<QueryPlan>,
        pub grouping_expressions: Vec<Expr>,
        pub function: crate::expression::serializable::CommonInlineUserDefinedFunction,
        pub output_schema: Schema,
        pub state_schema: Schema,
        pub output_mode: String,
        pub timeout_conf: String,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
    pub enum ReadType {
        NamedTable {
            unparsed_identifier: String,
            options: Vec<(String, String)>,
        },
        DataSource {
            format: Option<String>,
            schema: Option<Schema>,
            options: Vec<(String, String)>,
            paths: Vec<String>,
            predicates: Vec<Expr>,
        },
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
    pub enum CommandNode {
        ShowString(super::ShowString),
        HtmlString {
            input: Box<QueryPlan>,
            num_rows: usize,
            truncate: usize,
        },
        // catalog operations
        CurrentDatabase,
        SetCurrentDatabase {
            database_name: Identifier,
        },
        ListDatabases {
            catalog: Option<Identifier>,
            database_pattern: Option<String>,
        },
        ListTables {
            database: Option<ObjectName>,
            table_pattern: Option<String>,
        },
        ListFunctions {
            database: Option<ObjectName>,
            function_pattern: Option<String>,
        },
        ListColumns {
            table: ObjectName,
        },
        GetDatabase {
            database: ObjectName,
        },
        GetTable {
            table: ObjectName,
        },
        GetFunction {
            function: ObjectName,
        },
        DatabaseExists {
            database: ObjectName,
        },
        TableExists {
            table: ObjectName,
        },
        FunctionExists {
            function: ObjectName,
        },
        CreateTable {
            table: ObjectName,
            definition: TableDefinition,
        },
        RecoverPartitions {
            table: ObjectName,
        },
        IsCached {
            table: ObjectName,
        },
        CacheTable {
            table: ObjectName,
            lazy: bool,
            storage_level: Option<super::StorageLevel>,
            query: Option<Box<QueryPlan>>,
        },
        UncacheTable {
            table: ObjectName,
            if_exists: bool,
        },
        ClearCache,
        RefreshTable {
            table: ObjectName,
        },
        RefreshByPath {
            path: String,
        },
        CurrentCatalog,
        SetCurrentCatalog {
            catalog_name: Identifier,
        },
        ListCatalogs {
            catalog_pattern: Option<String>,
        },
        CreateCatalog {
            catalog: Identifier,
            definition: super::CatalogDefinition,
        },
        // commands
        CreateDatabase {
            database: ObjectName,
            definition: super::DatabaseDefinition,
        },
        DropDatabase {
            database: ObjectName,
            if_exists: bool,
            cascade: bool,
        },
        RegisterFunction(crate::expression::serializable::CommonInlineUserDefinedFunction),
        RegisterTableFunction(CommonInlineUserDefinedTableFunction),
        RefreshFunction {
            function: ObjectName,
        },
        DropFunction {
            function: ObjectName,
            if_exists: bool,
            is_temporary: bool,
        },
        DropTable {
            table: ObjectName,
            if_exists: bool,
            purge: bool,
        },
        CreateView {
            view: ObjectName,
            definition: ViewDefinition,
        },
        DropView {
            view: ObjectName,
            kind: Option<super::ViewKind>,
            if_exists: bool,
        },
        Write(Write),
        Explain {
            mode: super::ExplainMode,
            input: Box<QueryPlan>,
        },
        InsertInto {
            input: Box<QueryPlan>,
            table: ObjectName,
            columns: Vec<Identifier>,
            partition_spec: Vec<(Identifier, Option<Expr>)>,
            replace: Option<Expr>,
            if_not_exists: bool,
            overwrite: bool,
        },
        InsertOverwriteDirectory {
            input: Box<QueryPlan>,
            local: bool,
            location: Option<String>,
            file_format: Option<super::TableFileFormat>,
            row_format: Option<super::TableRowFormat>,
            options: Vec<(String, String)>,
        },
        MergeInto {
            target: ObjectName,
            with_schema_evolution: bool,
        },
        SetVariable {
            variable: String,
            value: String,
        },
        ResetVariable {
            variable: Option<String>,
        },
        AlterTable {
            table: ObjectName,
            operation: super::AlterTableOperation,
        },
        AlterView {
            view: ObjectName,
            operation: super::AlterViewOperation,
        },
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct TableDefinition {
        pub schema: Schema,
        pub comment: Option<String>,
        pub column_defaults: Vec<(Identifier, Expr)>,
        pub constraints: Vec<super::TableConstraint>,
        pub location: Option<String>,
        pub file_format: Option<super::TableFileFormat>,
        pub row_format: Option<super::TableRowFormat>,
        pub table_partition_cols: Vec<Identifier>,
        pub file_sort_order: Vec<Vec<SortOrder>>,
        pub if_not_exists: bool,
        pub or_replace: bool,
        pub unbounded: bool,
        pub options: Vec<(String, String)>,
        pub query: Option<Box<QueryPlan>>,
        pub definition: Option<String>,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct ViewDefinition {
        pub input: Box<QueryPlan>,
        pub columns: Option<Vec<Identifier>>,
        pub kind: super::ViewKind,
        pub replace: bool,
        pub definition: Option<String>,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expression::{Expr, ExprKind, NullOrdering, SortDirection, SortOrder};
    use crate::literal::Literal;
    use crate::types::NoMetadata;
    use std::str::FromStr;

    // --- Core Plan Tests ---

    #[test]
    fn test_query_plan_creation() {
        let range_node = QueryNode::Range(Range {
            start: Some(0),
            end: 100,
            step: 1,
            num_partitions: Some(4),
        });

        let plan = QueryPlan::new(range_node);

        assert!(plan.plan_id.is_none());
        assert_eq!(plan.metadata, NoMetadata);

        match plan.node {
            QueryNode::Range(range) => {
                assert_eq!(range.start, Some(0));
                assert_eq!(range.end, 100);
                assert_eq!(range.step, 1);
                assert_eq!(range.num_partitions, Some(4));
            }
            _ => panic!("Expected Range node"),
        }
    }

    #[test]
    fn test_command_plan_creation() {
        let show_string = ShowString {
            num_rows: 20,
            truncate: 20,
            vertical: false,
        };

        let command_node = CommandNode::ShowString(show_string);

        let command_plan = CommandPlan::new(command_node);
        assert!(command_plan.plan_id.is_none());
        assert_eq!(command_plan.metadata, NoMetadata);
    }

    #[test]
    fn test_plan_enum() {
        let range_node = QueryNode::Range(Range {
            start: None,
            end: 10,
            step: 1,
            num_partitions: None,
        });

        let query_plan = QueryPlan::new(range_node);
        let plan = Plan::Query(query_plan);

        match plan {
            Plan::Query(qp) => match qp.node {
                QueryNode::Range(range) => {
                    assert_eq!(range.start, None);
                    assert_eq!(range.end, 10);
                }
                _ => panic!("Expected Range node"),
            },
            Plan::Command(_) => panic!("Expected Query plan"),
        }
    }

    #[test]
    fn test_query_node_project() {
        let project_node = QueryNode::Project {
            input: Some(Box::new(QueryPlan::new(QueryNode::Range(Range {
                start: Some(1),
                end: 10,
                step: 1,
                num_partitions: None,
            })))),
            expressions: vec![
                Expr::new(ExprKind::Literal(Literal::Int32 { value: Some(1) })),
                Expr::new(ExprKind::Literal(Literal::Utf8 {
                    value: Some("test".to_string()),
                })),
            ],
        };

        match project_node {
            QueryNode::Project { input, expressions } => {
                assert!(input.is_some());
                assert_eq!(expressions.len(), 2);
            }
            _ => panic!("Expected Project node"),
        }
    }

    #[test]
    fn test_query_node_filter() {
        let filter_node = QueryNode::Filter {
            input: Box::new(QueryPlan::new(QueryNode::Range(Range {
                start: Some(1),
                end: 10,
                step: 1,
                num_partitions: None,
            }))),
            condition: Expr::new(ExprKind::Literal(Literal::Boolean { value: Some(true) })),
        };

        match filter_node {
            QueryNode::Filter { input, condition } => {
                match input.node {
                    QueryNode::Range(_) => {}
                    _ => panic!("Expected Range input"),
                }
                match condition.kind {
                    ExprKind::Literal(Literal::Boolean { value: Some(true) }) => {}
                    _ => panic!("Expected boolean condition"),
                }
            }
            _ => panic!("Expected Filter node"),
        }
    }

    #[test]
    fn test_query_node_join() {
        let left_plan = QueryPlan::new(QueryNode::Range(Range {
            start: Some(1),
            end: 5,
            step: 1,
            num_partitions: None,
        }));

        let right_plan = QueryPlan::new(QueryNode::Range(Range {
            start: Some(6),
            end: 10,
            step: 1,
            num_partitions: None,
        }));

        let join = Join {
            left: Box::new(left_plan),
            right: Box::new(right_plan),
            join_type: JoinType::Inner,
            join_criteria: Some(JoinCriteria::On(Box::new(Expr::new(ExprKind::Literal(
                Literal::Boolean { value: Some(true) },
            ))))),
            join_data_type: Some(JoinDataType {
                is_asof_join: false,
                using_columns: vec!["id".to_string()],
            }),
        };

        let join_node = QueryNode::Join(join);

        match join_node {
            QueryNode::Join(j) => {
                assert_eq!(j.join_type, JoinType::Inner);
                assert!(j.join_criteria.is_some());
                assert!(j.join_data_type.is_some());
            }
            _ => panic!("Expected Join node"),
        }
    }

    #[test]
    fn test_query_node_aggregate() {
        let aggregate = Aggregate {
            input: Box::new(QueryPlan::new(QueryNode::Range(Range {
                start: Some(1),
                end: 10,
                step: 1,
                num_partitions: None,
            }))),
            grouping: vec![Expr::new(ExprKind::Literal(Literal::Utf8 {
                value: Some("department".to_string()),
            }))],
            aggregate: vec![Expr::new(ExprKind::Literal(Literal::Utf8 {
                value: Some("count(*)".to_string()),
            }))],
            having: Some(Expr::new(ExprKind::Literal(Literal::Boolean {
                value: Some(true),
            }))),
            with_grouping_expressions: true,
        };

        let agg_node = QueryNode::Aggregate(aggregate);

        match agg_node {
            QueryNode::Aggregate(agg) => {
                assert_eq!(agg.grouping.len(), 1);
                assert_eq!(agg.aggregate.len(), 1);
                assert!(agg.having.is_some());
                assert!(agg.with_grouping_expressions);
            }
            _ => panic!("Expected Aggregate node"),
        }
    }

    #[test]
    fn test_query_node_sort() {
        let sort_node = QueryNode::Sort {
            input: Box::new(QueryPlan::new(QueryNode::Range(Range {
                start: Some(1),
                end: 10,
                step: 1,
                num_partitions: None,
            }))),
            order: vec![
                SortOrder {
                    child: Box::new(Expr::new(ExprKind::Literal(Literal::Utf8 {
                        value: Some("col1".to_string()),
                    }))),
                    direction: SortDirection::Ascending,
                    null_ordering: NullOrdering::NullsFirst,
                },
                SortOrder {
                    child: Box::new(Expr::new(ExprKind::Literal(Literal::Utf8 {
                        value: Some("col2".to_string()),
                    }))),
                    direction: SortDirection::Descending,
                    null_ordering: NullOrdering::NullsLast,
                },
            ],
            is_global: true,
        };

        match sort_node {
            QueryNode::Sort {
                order, is_global, ..
            } => {
                assert_eq!(order.len(), 2);
                assert!(is_global);
                assert_eq!(order[0].direction, SortDirection::Ascending);
                assert_eq!(order[1].direction, SortDirection::Descending);
            }
            _ => panic!("Expected Sort node"),
        }
    }

    #[test]
    fn test_query_node_limit() {
        let limit_node = QueryNode::Limit {
            input: Box::new(QueryPlan::new(QueryNode::Range(Range {
                start: Some(1),
                end: 100,
                step: 1,
                num_partitions: None,
            }))),
            skip: Some(Expr::new(ExprKind::Literal(Literal::Int64 {
                value: Some(10),
            }))),
            limit: Some(Expr::new(ExprKind::Literal(Literal::Int64 {
                value: Some(20),
            }))),
        };

        match limit_node {
            QueryNode::Limit { skip, limit, .. } => {
                assert!(skip.is_some());
                assert!(limit.is_some());
            }
            _ => panic!("Expected Limit node"),
        }
    }

    #[test]
    fn test_serializable_plan() {
        let range = Range {
            start: Some(1),
            end: 5,
            step: 1,
            num_partitions: Some(2),
        };

        // Test that Range can be serialized/deserialized
        let json = serde_json::to_string(&range).expect("Failed to serialize Range");
        let deserialized: Range = serde_json::from_str(&json).expect("Failed to deserialize Range");

        assert_eq!(range, deserialized);
    }

    #[test]
    fn test_join_type_serialization() {
        let join_types = vec![
            JoinType::Inner,
            JoinType::FullOuter,
            JoinType::LeftOuter,
            JoinType::RightOuter,
            JoinType::LeftAnti,
            JoinType::LeftSemi,
            JoinType::RightSemi,
            JoinType::RightAnti,
            JoinType::Cross,
        ];

        for join_type in join_types {
            let json = serde_json::to_string(&join_type).expect("Failed to serialize JoinType");
            let deserialized: JoinType =
                serde_json::from_str(&json).expect("Failed to deserialize JoinType");
            assert_eq!(join_type, deserialized);
        }
    }

    #[test]
    fn test_storage_level_from_str() {
        let storage_level =
            StorageLevel::from_str("MEMORY_ONLY").expect("Failed to parse MEMORY_ONLY");

        assert!(!storage_level.use_disk);
        assert!(storage_level.use_memory);
        assert!(!storage_level.use_off_heap);
        assert!(storage_level.deserialized);
        assert_eq!(storage_level.replication, 1);

        let storage_level2 =
            StorageLevel::from_str("MEMORY_AND_DISK_2").expect("Failed to parse MEMORY_AND_DISK_2");

        assert!(storage_level2.use_disk);
        assert!(storage_level2.use_memory);
        assert!(!storage_level2.use_off_heap);
        assert!(storage_level2.deserialized);
        assert_eq!(storage_level2.replication, 2);
    }

    // --- Supporting Structure Tests ---

    #[test]
    fn test_join_criteria_variants() {
        let natural_criteria: JoinCriteria<NoMetadata> = JoinCriteria::Natural;
        let on_criteria: JoinCriteria<NoMetadata> =
            JoinCriteria::On(Box::new(Expr::new(ExprKind::Literal(Literal::Boolean {
                value: Some(true),
            }))));
        let using_criteria: JoinCriteria<NoMetadata> = JoinCriteria::Using(vec![
            crate::expression::Identifier::from("id"),
            crate::expression::Identifier::from("name"),
        ]);

        match natural_criteria {
            JoinCriteria::Natural => {}
            _ => panic!("Expected Natural criteria"),
        }

        match on_criteria {
            JoinCriteria::On(_) => {}
            _ => panic!("Expected On criteria"),
        }

        match using_criteria {
            JoinCriteria::Using(cols) => assert_eq!(cols.len(), 2),
            _ => panic!("Expected Using criteria"),
        }
    }

    #[test]
    fn test_set_operation_types() {
        let set_op_types = vec![SetOpType::Intersect, SetOpType::Union, SetOpType::Except];

        for set_op_type in set_op_types {
            let json = serde_json::to_string(&set_op_type).expect("Failed to serialize SetOpType");
            let deserialized: SetOpType =
                serde_json::from_str(&json).expect("Failed to deserialize SetOpType");
            assert_eq!(set_op_type, deserialized);
        }
    }

    #[test]
    fn test_save_modes() {
        let save_modes = vec![
            SaveMode::Append,
            SaveMode::Overwrite,
            SaveMode::ErrorIfExists,
            SaveMode::Ignore,
        ];

        for save_mode in save_modes {
            let json = serde_json::to_string(&save_mode).expect("Failed to serialize SaveMode");
            let deserialized: SaveMode =
                serde_json::from_str(&json).expect("Failed to deserialize SaveMode");
            assert_eq!(save_mode, deserialized);
        }
    }

    #[test]
    fn test_parse_formats() {
        let parse_formats = vec![
            ParseFormat::Unspecified,
            ParseFormat::Csv,
            ParseFormat::Json,
        ];

        for parse_format in parse_formats {
            let json =
                serde_json::to_string(&parse_format).expect("Failed to serialize ParseFormat");
            let deserialized: ParseFormat =
                serde_json::from_str(&json).expect("Failed to deserialize ParseFormat");
            assert_eq!(parse_format, deserialized);
        }
    }

    #[test]
    fn test_bucket_by_serialization() {
        let bucket_by = BucketBy {
            bucket_column_names: vec!["col1".to_string(), "col2".to_string()],
            num_buckets: 10,
        };

        let json = serde_json::to_string(&bucket_by).expect("Failed to serialize BucketBy");
        let deserialized: BucketBy =
            serde_json::from_str(&json).expect("Failed to deserialize BucketBy");

        assert_eq!(bucket_by, deserialized);
        assert_eq!(deserialized.bucket_column_names.len(), 2);
        assert_eq!(deserialized.num_buckets, 10);
    }

    #[test]
    fn test_fraction_serialization() {
        let fraction = Fraction {
            key: Literal::Utf8 {
                value: Some("key1".to_string()),
            },
            fraction: 0.5,
        };

        let json = serde_json::to_string(&fraction).expect("Failed to serialize Fraction");
        let deserialized: Fraction =
            serde_json::from_str(&json).expect("Failed to deserialize Fraction");

        assert_eq!(fraction, deserialized);
        assert_eq!(deserialized.fraction, 0.5);
    }

    #[test]
    fn test_replacement_serialization() {
        let replacement = Replacement {
            old_value: Literal::Int32 { value: Some(1) },
            new_value: Literal::Int32 { value: Some(2) },
        };

        let json = serde_json::to_string(&replacement).expect("Failed to serialize Replacement");
        let deserialized: Replacement =
            serde_json::from_str(&json).expect("Failed to deserialize Replacement");

        assert_eq!(replacement, deserialized);
    }

    #[test]
    fn test_query_node_local_relation() {
        let local_relation: QueryNode<NoMetadata> = QueryNode::LocalRelation {
            data: Some(vec![1, 2, 3, 4]),
            schema: None,
        };

        match local_relation {
            QueryNode::LocalRelation { data, schema } => {
                assert!(data.is_some());
                assert_eq!(data.unwrap().len(), 4);
                assert!(schema.is_none());
            }
            _ => panic!("Expected LocalRelation node"),
        }
    }

    #[test]
    fn test_query_node_sample() {
        let sample = Sample {
            input: Box::new(QueryPlan::new(QueryNode::Range(Range {
                start: Some(1),
                end: 100,
                step: 1,
                num_partitions: None,
            }))),
            lower_bound: 0.0,
            upper_bound: 0.1,
            with_replacement: false,
            seed: Some(42),
            deterministic_order: true,
        };

        let sample_node = QueryNode::Sample(sample);

        match sample_node {
            QueryNode::Sample(s) => {
                assert_eq!(s.lower_bound, 0.0);
                assert_eq!(s.upper_bound, 0.1);
                assert!(!s.with_replacement);
                assert_eq!(s.seed, Some(42));
                assert!(s.deterministic_order);
            }
            _ => panic!("Expected Sample node"),
        }
    }

    #[test]
    fn test_query_node_deduplicate() {
        let deduplicate = Deduplicate {
            input: Box::new(QueryPlan::new(QueryNode::Range(Range {
                start: Some(1),
                end: 100,
                step: 1,
                num_partitions: None,
            }))),
            column_names: vec![
                crate::expression::Identifier::from("col1"),
                crate::expression::Identifier::from("col2"),
            ],
            all_columns_as_keys: false,
            within_watermark: true,
        };

        let dedup_node = QueryNode::Deduplicate(deduplicate);

        match dedup_node {
            QueryNode::Deduplicate(d) => {
                assert_eq!(d.column_names.len(), 2);
                assert!(!d.all_columns_as_keys);
                assert!(d.within_watermark);
            }
            _ => panic!("Expected Deduplicate node"),
        }
    }

    #[test]
    fn test_query_node_repartition() {
        let repartition_node = QueryNode::Repartition {
            input: Box::new(QueryPlan::new(QueryNode::Range(Range {
                start: Some(1),
                end: 100,
                step: 1,
                num_partitions: None,
            }))),
            num_partitions: 8,
            shuffle: true,
        };

        match repartition_node {
            QueryNode::Repartition {
                num_partitions,
                shuffle,
                ..
            } => {
                assert_eq!(num_partitions, 8);
                assert!(shuffle);
            }
            _ => panic!("Expected Repartition node"),
        }
    }

    #[test]
    fn test_query_node_subquery_alias() {
        let subquery_alias = QueryNode::SubqueryAlias {
            input: Box::new(QueryPlan::new(QueryNode::Range(Range {
                start: Some(1),
                end: 10,
                step: 1,
                num_partitions: None,
            }))),
            alias: crate::expression::Identifier::from("sub"),
            qualifier: vec![
                crate::expression::Identifier::from("schema"),
                crate::expression::Identifier::from("table"),
            ],
        };

        match subquery_alias {
            QueryNode::SubqueryAlias {
                alias, qualifier, ..
            } => {
                assert_eq!(alias.as_ref(), "sub");
                assert_eq!(qualifier.len(), 2);
            }
            _ => panic!("Expected SubqueryAlias node"),
        }
    }

    #[test]
    fn test_query_node_empty() {
        let empty_node: QueryNode<NoMetadata> = QueryNode::Empty {
            produce_one_row: true,
        };

        match empty_node {
            QueryNode::Empty { produce_one_row } => {
                assert!(produce_one_row);
            }
            _ => panic!("Expected Empty node"),
        }
    }

    #[test]
    fn test_query_node_values() {
        let values_node = QueryNode::Values(vec![
            vec![
                Expr::new(ExprKind::Literal(Literal::Int32 { value: Some(1) })),
                Expr::new(ExprKind::Literal(Literal::Utf8 {
                    value: Some("a".to_string()),
                })),
            ],
            vec![
                Expr::new(ExprKind::Literal(Literal::Int32 { value: Some(2) })),
                Expr::new(ExprKind::Literal(Literal::Utf8 {
                    value: Some("b".to_string()),
                })),
            ],
        ]);

        match values_node {
            QueryNode::Values(rows) => {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0].len(), 2);
                assert_eq!(rows[1].len(), 2);
            }
            _ => panic!("Expected Values node"),
        }
    }
}

// --- Conversion Implementations ---

#[allow(clippy::items_after_test_module)]
/// Conversion from generic Plan<M> to serializable Plan
impl<M: DataTypeMetadata> From<Plan<M>> for serializable::Plan {
    fn from(plan: Plan<M>) -> Self {
        match plan {
            Plan::Query(query_plan) => serializable::Plan::Query(query_plan.into()),
            Plan::Command(command_plan) => serializable::Plan::Command(command_plan.into()),
        }
    }
}

/// Conversion from generic QueryPlan<M> to serializable QueryPlan
impl<M: DataTypeMetadata> From<QueryPlan<M>> for serializable::QueryPlan {
    fn from(plan: QueryPlan<M>) -> Self {
        Self {
            node: plan.node.into(),
            plan_id: plan.plan_id,
        }
    }
}

/// Conversion from generic CommandPlan<M> to serializable CommandPlan
impl<M: DataTypeMetadata> From<CommandPlan<M>> for serializable::CommandPlan {
    fn from(plan: CommandPlan<M>) -> Self {
        Self {
            node: plan.node.into(),
            plan_id: plan.plan_id,
        }
    }
}

// --- Helper Conversions ---

impl<M: DataTypeMetadata> From<ReadType<M>> for serializable::ReadType {
    fn from(read_type: ReadType<M>) -> Self {
        match read_type {
            ReadType::NamedTable {
                unparsed_identifier,
                options,
            } => serializable::ReadType::NamedTable {
                unparsed_identifier,
                options,
            },
            ReadType::DataSource {
                format,
                schema,
                options,
                paths,
                predicates,
            } => serializable::ReadType::DataSource {
                format,
                schema: schema.map(|s| s.into()),
                options,
                paths,
                predicates: predicates.into_iter().map(|e| e.into()).collect(),
            },
        }
    }
}

impl<M: DataTypeMetadata> From<Join<M>> for serializable::Join {
    fn from(join: Join<M>) -> Self {
        serializable::Join {
            left: Box::new((*join.left).into()),
            right: Box::new((*join.right).into()),
            join_type: join.join_type,
            join_criteria: join.join_criteria.map(|c| c.into()),
            join_data_type: join.join_data_type,
        }
    }
}

impl<M: DataTypeMetadata> From<JoinCriteria<M>> for serializable::JoinCriteria {
    fn from(criteria: JoinCriteria<M>) -> Self {
        match criteria {
            JoinCriteria::Natural => serializable::JoinCriteria::Natural,
            JoinCriteria::On(expr) => serializable::JoinCriteria::On(Box::new((*expr).into())),
            JoinCriteria::Using(ids) => serializable::JoinCriteria::Using(ids),
        }
    }
}

impl<M: DataTypeMetadata> From<SetOperation<M>> for serializable::SetOperation {
    fn from(op: SetOperation<M>) -> Self {
        serializable::SetOperation {
            left: Box::new((*op.left).into()),
            right: Box::new((*op.right).into()),
            set_op_type: op.set_op_type,
            is_all: op.is_all,
            by_name: op.by_name,
            allow_missing_columns: op.allow_missing_columns,
        }
    }
}

impl<M: DataTypeMetadata> From<Aggregate<M>> for serializable::Aggregate {
    fn from(agg: Aggregate<M>) -> Self {
        serializable::Aggregate {
            input: Box::new((*agg.input).into()),
            grouping: agg.grouping.into_iter().map(|e| e.into()).collect(),
            aggregate: agg.aggregate.into_iter().map(|e| e.into()).collect(),
            having: agg.having.map(|e| e.into()),
            with_grouping_expressions: agg.with_grouping_expressions,
        }
    }
}

impl<M: DataTypeMetadata> From<Sample<M>> for serializable::Sample {
    fn from(sample: Sample<M>) -> Self {
        serializable::Sample {
            input: Box::new((*sample.input).into()),
            lower_bound: sample.lower_bound,
            upper_bound: sample.upper_bound,
            with_replacement: sample.with_replacement,
            seed: sample.seed,
            deterministic_order: sample.deterministic_order,
        }
    }
}

impl<M: DataTypeMetadata> From<Deduplicate<M>> for serializable::Deduplicate {
    fn from(dedup: Deduplicate<M>) -> Self {
        serializable::Deduplicate {
            input: Box::new((*dedup.input).into()),
            column_names: dedup.column_names,
            all_columns_as_keys: dedup.all_columns_as_keys,
            within_watermark: dedup.within_watermark,
        }
    }
}

impl<M: DataTypeMetadata> From<Pivot<M>> for serializable::Pivot {
    fn from(pivot: Pivot<M>) -> Self {
        serializable::Pivot {
            input: Box::new((*pivot.input).into()),
            grouping: pivot.grouping.into_iter().map(|e| e.into()).collect(),
            aggregate: pivot.aggregate.into_iter().map(|e| e.into()).collect(),
            columns: pivot.columns.into_iter().map(|e| e.into()).collect(),
            values: pivot.values,
        }
    }
}

impl<M: DataTypeMetadata> From<Unpivot<M>> for serializable::Unpivot {
    fn from(unpivot: Unpivot<M>) -> Self {
        serializable::Unpivot {
            input: Box::new((*unpivot.input).into()),
            ids: unpivot
                .ids
                .map(|ids| ids.into_iter().map(|e| e.into()).collect()),
            values: unpivot.values.into_iter().map(|v| v.into()).collect(),
            variable_column_name: unpivot.variable_column_name,
            value_column_names: unpivot.value_column_names,
            include_nulls: unpivot.include_nulls,
        }
    }
}

impl<M: DataTypeMetadata> From<UnpivotValue<M>> for serializable::UnpivotValue {
    fn from(value: UnpivotValue<M>) -> Self {
        serializable::UnpivotValue {
            columns: value.columns.into_iter().map(|e| e.into()).collect(),
            alias: value.alias,
        }
    }
}

impl<M: DataTypeMetadata> From<Parse<M>> for serializable::Parse {
    fn from(parse: Parse<M>) -> Self {
        serializable::Parse {
            input: Box::new((*parse.input).into()),
            format: parse.format,
            schema: parse.schema.map(|s| s.into()),
            options: parse.options,
        }
    }
}

impl<M: DataTypeMetadata> From<GroupMap<M>> for serializable::GroupMap {
    fn from(map: GroupMap<M>) -> Self {
        serializable::GroupMap {
            input: Box::new((*map.input).into()),
            grouping_expressions: map
                .grouping_expressions
                .into_iter()
                .map(|e| e.into())
                .collect(),
            function: map.function.into(),
            sorting_expressions: map
                .sorting_expressions
                .into_iter()
                .map(|e| e.into())
                .collect(),
            initial_input: map.initial_input.map(|p| Box::new((*p).into())),
            initial_grouping_expressions: map
                .initial_grouping_expressions
                .into_iter()
                .map(|e| e.into())
                .collect(),
            is_map_groups_with_state: map.is_map_groups_with_state,
            output_mode: map.output_mode,
            timeout_conf: map.timeout_conf,
            state_schema: map.state_schema.map(|s| s.into()),
        }
    }
}

impl<M: DataTypeMetadata> From<CoGroupMap<M>> for serializable::CoGroupMap {
    fn from(map: CoGroupMap<M>) -> Self {
        serializable::CoGroupMap {
            input: Box::new((*map.input).into()),
            input_grouping_expressions: map
                .input_grouping_expressions
                .into_iter()
                .map(|e| e.into())
                .collect(),
            other: Box::new((*map.other).into()),
            other_grouping_expressions: map
                .other_grouping_expressions
                .into_iter()
                .map(|e| e.into())
                .collect(),
            function: map.function.into(),
            input_sorting_expressions: map
                .input_sorting_expressions
                .into_iter()
                .map(|e| e.into())
                .collect(),
            other_sorting_expressions: map
                .other_sorting_expressions
                .into_iter()
                .map(|e| e.into())
                .collect(),
        }
    }
}

impl<M: DataTypeMetadata> From<WithWatermark<M>> for serializable::WithWatermark {
    fn from(wm: WithWatermark<M>) -> Self {
        serializable::WithWatermark {
            input: Box::new((*wm.input).into()),
            event_time: wm.event_time,
            delay_threshold: wm.delay_threshold,
        }
    }
}

impl<M: DataTypeMetadata> From<ApplyInPandasWithState<M>> for serializable::ApplyInPandasWithState {
    fn from(apply: ApplyInPandasWithState<M>) -> Self {
        serializable::ApplyInPandasWithState {
            input: Box::new((*apply.input).into()),
            grouping_expressions: apply
                .grouping_expressions
                .into_iter()
                .map(|e| e.into())
                .collect(),
            function: apply.function.into(),
            output_schema: apply.output_schema.into(),
            state_schema: apply.state_schema.into(),
            output_mode: apply.output_mode,
            timeout_conf: apply.timeout_conf,
        }
    }
}

impl<M: DataTypeMetadata> From<Write<M>> for serializable::Write {
    fn from(write: Write<M>) -> Self {
        serializable::Write {
            input: Box::new((*write.input).into()),
            source: write.source,
            save_type: write.save_type,
            mode: write.mode,
            sort_column_names: write.sort_column_names,
            partitioning_columns: write.partitioning_columns,
            bucket_by: write.bucket_by,
            options: write.options,
            clustering_columns: write.clustering_columns,
            overwrite_condition: write.overwrite_condition.map(|e| e.into()),
        }
    }
}

impl<M: DataTypeMetadata> From<TableDefinition<M>> for serializable::TableDefinition {
    fn from(def: TableDefinition<M>) -> Self {
        serializable::TableDefinition {
            schema: def.schema.into(),
            comment: def.comment,
            column_defaults: def
                .column_defaults
                .into_iter()
                .map(|(id, e)| (id, e.into()))
                .collect(),
            constraints: def.constraints,
            location: def.location,
            file_format: def.file_format,
            row_format: def.row_format,
            table_partition_cols: def.table_partition_cols,
            file_sort_order: def
                .file_sort_order
                .into_iter()
                .map(|v| v.into_iter().map(|s| s.into()).collect())
                .collect(),
            if_not_exists: def.if_not_exists,
            or_replace: def.or_replace,
            unbounded: def.unbounded,
            options: def.options,
            query: def.query.map(|p| Box::new((*p).into())),
            definition: def.definition,
        }
    }
}

impl<M: DataTypeMetadata> From<ViewDefinition<M>> for serializable::ViewDefinition {
    fn from(def: ViewDefinition<M>) -> Self {
        serializable::ViewDefinition {
            input: Box::new((*def.input).into()),
            columns: def.columns,
            kind: def.kind,
            replace: def.replace,
            definition: def.definition,
        }
    }
}

/// Conversion from generic QueryNode<M> to serializable QueryNode
impl<M: DataTypeMetadata> From<QueryNode<M>> for serializable::QueryNode {
    fn from(node: QueryNode<M>) -> Self {
        match node {
            QueryNode::Read {
                read_type,
                is_streaming,
            } => serializable::QueryNode::Read {
                read_type: read_type.into(),
                is_streaming,
            },
            QueryNode::Project { input, expressions } => serializable::QueryNode::Project {
                input: input.map(|p| Box::new((*p).into())),
                expressions: expressions.into_iter().map(Into::into).collect(),
            },
            QueryNode::Filter { input, condition } => serializable::QueryNode::Filter {
                input: Box::new((*input).into()),
                condition: condition.into(),
            },
            QueryNode::Join(join) => serializable::QueryNode::Join(join.into()),
            QueryNode::SetOperation(op) => serializable::QueryNode::SetOperation(op.into()),
            QueryNode::Sort {
                input,
                order,
                is_global,
            } => serializable::QueryNode::Sort {
                input: Box::new((*input).into()),
                order: order.into_iter().map(Into::into).collect(),
                is_global,
            },
            QueryNode::Limit { input, skip, limit } => serializable::QueryNode::Limit {
                input: Box::new((*input).into()),
                skip: skip.map(Into::into),
                limit: limit.map(Into::into),
            },
            QueryNode::Aggregate(agg) => serializable::QueryNode::Aggregate(agg.into()),
            QueryNode::LocalRelation { data, schema } => serializable::QueryNode::LocalRelation {
                data,
                schema: schema.map(|s| s.into()),
            },
            QueryNode::Sample(sample) => serializable::QueryNode::Sample(sample.into()),
            QueryNode::Deduplicate(dedup) => serializable::QueryNode::Deduplicate(dedup.into()),
            QueryNode::Range(range) => serializable::QueryNode::Range(range),
            QueryNode::SubqueryAlias {
                input,
                alias,
                qualifier,
            } => serializable::QueryNode::SubqueryAlias {
                input: Box::new((*input).into()),
                alias,
                qualifier,
            },
            QueryNode::Repartition {
                input,
                num_partitions,
                shuffle,
            } => serializable::QueryNode::Repartition {
                input: Box::new((*input).into()),
                num_partitions,
                shuffle,
            },
            QueryNode::ToDf {
                input,
                column_names,
            } => serializable::QueryNode::ToDf {
                input: Box::new((*input).into()),
                column_names,
            },
            QueryNode::WithColumnsRenamed {
                input,
                rename_columns_map,
            } => serializable::QueryNode::WithColumnsRenamed {
                input: Box::new((*input).into()),
                rename_columns_map,
            },
            QueryNode::Drop {
                input,
                columns,
                column_names,
            } => serializable::QueryNode::Drop {
                input: Box::new((*input).into()),
                columns: columns.into_iter().map(Into::into).collect(),
                column_names,
            },
            QueryNode::Tail { input, limit } => serializable::QueryNode::Tail {
                input: Box::new((*input).into()),
                limit: limit.into(),
            },
            QueryNode::WithColumns { input, aliases } => serializable::QueryNode::WithColumns {
                input: Box::new((*input).into()),
                aliases: aliases.into_iter().map(Into::into).collect(),
            },
            QueryNode::Hint {
                input,
                name,
                parameters,
            } => serializable::QueryNode::Hint {
                input: Box::new((*input).into()),
                name,
                parameters: parameters.into_iter().map(Into::into).collect(),
            },
            QueryNode::Pivot(pivot) => serializable::QueryNode::Pivot(pivot.into()),
            QueryNode::Unpivot(unpivot) => serializable::QueryNode::Unpivot(unpivot.into()),
            QueryNode::ToSchema { input, schema } => serializable::QueryNode::ToSchema {
                input: Box::new((*input).into()),
                schema: schema.into(),
            },
            QueryNode::RepartitionByExpression {
                input,
                partition_expressions,
                num_partitions,
            } => serializable::QueryNode::RepartitionByExpression {
                input: Box::new((*input).into()),
                partition_expressions: partition_expressions.into_iter().map(Into::into).collect(),
                num_partitions,
            },
            QueryNode::MapPartitions {
                input,
                function,
                is_barrier,
            } => serializable::QueryNode::MapPartitions {
                input: Box::new((*input).into()),
                function: function.into(),
                is_barrier,
            },
            QueryNode::CollectMetrics {
                input,
                name,
                metrics,
            } => serializable::QueryNode::CollectMetrics {
                input: Box::new((*input).into()),
                name,
                metrics: metrics.into_iter().map(Into::into).collect(),
            },
            QueryNode::Parse(parse) => serializable::QueryNode::Parse(parse.into()),
            QueryNode::GroupMap(map) => serializable::QueryNode::GroupMap(map.into()),
            QueryNode::CoGroupMap(map) => serializable::QueryNode::CoGroupMap(map.into()),
            QueryNode::WithWatermark(wm) => serializable::QueryNode::WithWatermark(wm.into()),
            QueryNode::ApplyInPandasWithState(apply) => {
                serializable::QueryNode::ApplyInPandasWithState(apply.into())
            }
            QueryNode::CachedLocalRelation { hash } => {
                serializable::QueryNode::CachedLocalRelation { hash }
            }
            QueryNode::CachedRemoteRelation { relation_id } => {
                serializable::QueryNode::CachedRemoteRelation { relation_id }
            }
            QueryNode::CommonInlineUserDefinedTableFunction(func) => {
                serializable::QueryNode::CommonInlineUserDefinedTableFunction(func.into())
            }
            QueryNode::FillNa {
                input,
                columns,
                values,
            } => serializable::QueryNode::FillNa {
                input: Box::new((*input).into()),
                columns,
                values: values.into_iter().map(Into::into).collect(),
            },
            QueryNode::DropNa {
                input,
                columns,
                min_non_nulls,
            } => serializable::QueryNode::DropNa {
                input: Box::new((*input).into()),
                columns,
                min_non_nulls,
            },
            QueryNode::Replace {
                input,
                columns,
                replacements,
            } => serializable::QueryNode::Replace {
                input: Box::new((*input).into()),
                columns,
                replacements,
            },
            QueryNode::StatSummary { input, statistics } => serializable::QueryNode::StatSummary {
                input: Box::new((*input).into()),
                statistics,
            },
            QueryNode::StatDescribe { input, columns } => serializable::QueryNode::StatDescribe {
                input: Box::new((*input).into()),
                columns,
            },
            QueryNode::StatCrosstab {
                input,
                left_column,
                right_column,
            } => serializable::QueryNode::StatCrosstab {
                input: Box::new((*input).into()),
                left_column,
                right_column,
            },
            QueryNode::StatCov {
                input,
                left_column,
                right_column,
            } => serializable::QueryNode::StatCov {
                input: Box::new((*input).into()),
                left_column,
                right_column,
            },
            QueryNode::StatCorr {
                input,
                left_column,
                right_column,
                method,
            } => serializable::QueryNode::StatCorr {
                input: Box::new((*input).into()),
                left_column,
                right_column,
                method,
            },
            QueryNode::StatApproxQuantile {
                input,
                columns,
                probabilities,
                relative_error,
            } => serializable::QueryNode::StatApproxQuantile {
                input: Box::new((*input).into()),
                columns,
                probabilities,
                relative_error,
            },
            QueryNode::StatFreqItems {
                input,
                columns,
                support,
            } => serializable::QueryNode::StatFreqItems {
                input: Box::new((*input).into()),
                columns,
                support,
            },
            QueryNode::StatSampleBy {
                input,
                column,
                fractions,
                seed,
            } => serializable::QueryNode::StatSampleBy {
                input: Box::new((*input).into()),
                column: column.into(),
                fractions,
                seed,
            },
            QueryNode::Empty { produce_one_row } => {
                serializable::QueryNode::Empty { produce_one_row }
            }
            QueryNode::WithParameters {
                input,
                positional_arguments,
                named_arguments,
            } => serializable::QueryNode::WithParameters {
                input: Box::new((*input).into()),
                positional_arguments: positional_arguments.into_iter().map(Into::into).collect(),
                named_arguments: named_arguments
                    .into_iter()
                    .map(|(k, v)| (k, v.into()))
                    .collect(),
            },
            QueryNode::Values(values) => serializable::QueryNode::Values(
                values
                    .into_iter()
                    .map(|row| row.into_iter().map(Into::into).collect())
                    .collect(),
            ),
            QueryNode::TableAlias {
                input,
                name,
                columns,
            } => serializable::QueryNode::TableAlias {
                input: Box::new((*input).into()),
                name,
                columns,
            },
            QueryNode::WithCtes {
                input,
                recursive,
                ctes,
            } => serializable::QueryNode::WithCtes {
                input: Box::new((*input).into()),
                recursive,
                ctes: ctes.into_iter().map(|(id, p)| (id, p.into())).collect(),
            },
            QueryNode::LateralView {
                input,
                function,
                arguments,
                named_arguments,
                table_alias,
                column_aliases,
                outer,
            } => serializable::QueryNode::LateralView {
                input: input.map(|p| Box::new((*p).into())),
                function,
                arguments: arguments.into_iter().map(Into::into).collect(),
                named_arguments: named_arguments
                    .into_iter()
                    .map(|(id, e)| (id, e.into()))
                    .collect(),
                table_alias,
                column_aliases,
                outer,
            },
        }
    }
}

/// Conversion from generic CommandNode<M> to serializable CommandNode
impl<M: DataTypeMetadata> From<CommandNode<M>> for serializable::CommandNode {
    fn from(node: CommandNode<M>) -> Self {
        match node {
            CommandNode::ShowString(s) => serializable::CommandNode::ShowString(s),
            CommandNode::HtmlString {
                input,
                num_rows,
                truncate,
            } => serializable::CommandNode::HtmlString {
                input: Box::new((*input).into()),
                num_rows,
                truncate,
            },
            CommandNode::CurrentDatabase => serializable::CommandNode::CurrentDatabase,
            CommandNode::SetCurrentDatabase { database_name } => {
                serializable::CommandNode::SetCurrentDatabase { database_name }
            }
            CommandNode::ListDatabases {
                catalog,
                database_pattern,
            } => serializable::CommandNode::ListDatabases {
                catalog,
                database_pattern,
            },
            CommandNode::ListTables {
                database,
                table_pattern,
            } => serializable::CommandNode::ListTables {
                database,
                table_pattern,
            },
            CommandNode::ListFunctions {
                database,
                function_pattern,
            } => serializable::CommandNode::ListFunctions {
                database,
                function_pattern,
            },
            CommandNode::ListColumns { table } => serializable::CommandNode::ListColumns { table },
            CommandNode::GetDatabase { database } => {
                serializable::CommandNode::GetDatabase { database }
            }
            CommandNode::GetTable { table } => serializable::CommandNode::GetTable { table },
            CommandNode::GetFunction { function } => {
                serializable::CommandNode::GetFunction { function }
            }
            CommandNode::DatabaseExists { database } => {
                serializable::CommandNode::DatabaseExists { database }
            }
            CommandNode::TableExists { table } => serializable::CommandNode::TableExists { table },
            CommandNode::FunctionExists { function } => {
                serializable::CommandNode::FunctionExists { function }
            }
            CommandNode::CreateTable { table, definition } => {
                serializable::CommandNode::CreateTable {
                    table,
                    definition: definition.into(),
                }
            }
            CommandNode::RecoverPartitions { table } => {
                serializable::CommandNode::RecoverPartitions { table }
            }
            CommandNode::IsCached { table } => serializable::CommandNode::IsCached { table },
            CommandNode::CacheTable {
                table,
                lazy,
                storage_level,
                query,
            } => serializable::CommandNode::CacheTable {
                table,
                lazy,
                storage_level,
                query: query.map(|p| Box::new((*p).into())),
            },
            CommandNode::UncacheTable { table, if_exists } => {
                serializable::CommandNode::UncacheTable { table, if_exists }
            }
            CommandNode::ClearCache => serializable::CommandNode::ClearCache,
            CommandNode::RefreshTable { table } => {
                serializable::CommandNode::RefreshTable { table }
            }
            CommandNode::RefreshByPath { path } => {
                serializable::CommandNode::RefreshByPath { path }
            }
            CommandNode::CurrentCatalog => serializable::CommandNode::CurrentCatalog,
            CommandNode::SetCurrentCatalog { catalog_name } => {
                serializable::CommandNode::SetCurrentCatalog { catalog_name }
            }
            CommandNode::ListCatalogs { catalog_pattern } => {
                serializable::CommandNode::ListCatalogs { catalog_pattern }
            }
            CommandNode::CreateCatalog {
                catalog,
                definition,
            } => serializable::CommandNode::CreateCatalog {
                catalog,
                definition,
            },
            CommandNode::CreateDatabase {
                database,
                definition,
            } => serializable::CommandNode::CreateDatabase {
                database,
                definition,
            },
            CommandNode::DropDatabase {
                database,
                if_exists,
                cascade,
            } => serializable::CommandNode::DropDatabase {
                database,
                if_exists,
                cascade,
            },
            CommandNode::RegisterFunction(f) => {
                serializable::CommandNode::RegisterFunction(f.into())
            }
            CommandNode::RegisterTableFunction(f) => {
                serializable::CommandNode::RegisterTableFunction(f.into())
            }
            CommandNode::RefreshFunction { function } => {
                serializable::CommandNode::RefreshFunction { function }
            }
            CommandNode::DropFunction {
                function,
                if_exists,
                is_temporary,
            } => serializable::CommandNode::DropFunction {
                function,
                if_exists,
                is_temporary,
            },
            CommandNode::DropTable {
                table,
                if_exists,
                purge,
            } => serializable::CommandNode::DropTable {
                table,
                if_exists,
                purge,
            },
            CommandNode::CreateView { view, definition } => serializable::CommandNode::CreateView {
                view,
                definition: definition.into(),
            },
            CommandNode::DropView {
                view,
                kind,
                if_exists,
            } => serializable::CommandNode::DropView {
                view,
                kind,
                if_exists,
            },
            CommandNode::Write(w) => serializable::CommandNode::Write(w.into()),
            CommandNode::Explain { mode, input } => serializable::CommandNode::Explain {
                mode,
                input: Box::new((*input).into()),
            },
            CommandNode::InsertInto {
                input,
                table,
                columns,
                partition_spec,
                replace,
                if_not_exists,
                overwrite,
            } => serializable::CommandNode::InsertInto {
                input: Box::new((*input).into()),
                table,
                columns,
                partition_spec: partition_spec
                    .into_iter()
                    .map(|(id, e)| (id, e.map(Into::into)))
                    .collect(),
                replace: replace.map(Into::into),
                if_not_exists,
                overwrite,
            },
            CommandNode::InsertOverwriteDirectory {
                input,
                local,
                location,
                file_format,
                row_format,
                options,
            } => serializable::CommandNode::InsertOverwriteDirectory {
                input: Box::new((*input).into()),
                local,
                location,
                file_format,
                row_format,
                options,
            },
            CommandNode::MergeInto {
                target,
                with_schema_evolution,
            } => serializable::CommandNode::MergeInto {
                target,
                with_schema_evolution,
            },
            CommandNode::SetVariable { variable, value } => {
                serializable::CommandNode::SetVariable { variable, value }
            }
            CommandNode::ResetVariable { variable } => {
                serializable::CommandNode::ResetVariable { variable }
            }
            CommandNode::AlterTable { table, operation } => {
                serializable::CommandNode::AlterTable { table, operation }
            }
            CommandNode::AlterView { view, operation } => {
                serializable::CommandNode::AlterView { view, operation }
            }
        }
    }
}
