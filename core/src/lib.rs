//! Barks Core - Apache Spark implementation in Rust
//!
//! This is the core module of the Barks project, providing the fundamental
//! distributed computing capabilities similar to Apache Spark.

pub mod context;
pub mod expression;
pub mod literal;
pub mod logical_plan;
pub mod planner;
pub mod rdd;
pub mod storage;
pub mod types;

pub use context::BarksContext;
pub use expression::{Expr, ExprKind};
pub use literal::Literal;
pub use logical_plan::{CommandNode, CommandPlan, Plan, QueryNode, QueryPlan};
pub use rdd::DistributedDataset;
pub use storage::StorageLevel;
pub use types::{DataType, DataTypeKind, DataTypeMetadata, Field, Fields, NoMetadata, Schema};
