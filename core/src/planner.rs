//! Translates Barks logical plans to DataFusion logical plans.

use crate::expression::{Expr as BarksExpr, ExprKind as BarksExprKind};
use crate::literal::Literal as BarksLiteral;
use crate::logical_plan::{QueryNode, QueryPlan};
use crate::types::NoMetadata;
use barks_common::error::{CommonError, Result};
use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::{MemTable, provider_as_source};
use datafusion::logical_expr::{self as df, LogicalPlan as DfLogicalPlan, LogicalPlanBuilder};
use datafusion::scalar::ScalarValue;
use std::sync::Arc;

/// Translates a Barks `QueryPlan` into a DataFusion `LogicalPlan`.
pub fn to_df_logical_plan(plan: &QueryPlan<NoMetadata>) -> Result<DfLogicalPlan> {
    match &plan.node {
        QueryNode::Project { input, expressions } => {
            let input_plan = input
                .as_ref()
                .ok_or_else(|| CommonError::internal_error("Project must have an input"))?;
            let df_input_plan = to_df_logical_plan(input_plan)?;
            let df_exprs = expressions
                .iter()
                .map(to_df_expr)
                .collect::<Result<Vec<_>>>()?;
            Ok(LogicalPlanBuilder::from(df_input_plan)
                .project(df_exprs)?
                .build()?)
        }
        QueryNode::Filter { input, condition } => {
            let df_input_plan = to_df_logical_plan(input)?;
            let df_condition = to_df_expr(condition)?;
            Ok(LogicalPlanBuilder::from(df_input_plan)
                .filter(df_condition)?
                .build()?)
        }
        QueryNode::Range(range) => {
            let start = range.start.unwrap_or(0);
            let end = range.end;
            let step = range.step;

            let values: Vec<i64> = (start..end).step_by(step as usize).collect();
            let array = Arc::new(Int64Array::from(values));
            let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
                "id",
                ArrowDataType::Int64,
                false,
            )]));
            let batch = RecordBatch::try_new(schema.clone(), vec![array]).map_err(|e| {
                CommonError::internal_error(format!("Failed to create RecordBatch: {}", e))
            })?;

            let table_provider = MemTable::try_new(schema, vec![vec![batch]]).map_err(|e| {
                CommonError::internal_error(format!("Failed to create MemTable: {}", e))
            })?;

            let table_source = provider_as_source(Arc::new(table_provider));
            let builder = LogicalPlanBuilder::scan("range", table_source, None)?;
            Ok(builder.build()?)
        }
        _ => Err(CommonError::internal_error(format!(
            "Translation for QueryNode::{:?} not implemented",
            plan.node.variant_name()
        ))),
    }
}

/// Translates a Barks `Expr` into a DataFusion `Expr`.
pub fn to_df_expr(expr: &BarksExpr<NoMetadata>) -> Result<df::Expr> {
    match &expr.kind {
        BarksExprKind::UnresolvedAttribute { name, .. } => {
            let parts: Vec<String> = name.clone().into();
            if parts.len() == 1 {
                Ok(df::col(parts[0].clone()))
            } else {
                Err(CommonError::internal_error(format!(
                    "Unsupported multi-part identifier in expression translation: {:?}",
                    name
                )))
            }
        }
        BarksExprKind::Literal(l) => to_df_scalar(l).map(df::lit),
        BarksExprKind::CallFunction {
            function_name,
            arguments,
        } => {
            let op = match function_name.parts()[0].as_ref() {
                "eq" => Ok(df::Operator::Eq),
                "neq" => Ok(df::Operator::NotEq),
                "lt" => Ok(df::Operator::Lt),
                "lteq" => Ok(df::Operator::LtEq),
                "gt" => Ok(df::Operator::Gt),
                "gteq" => Ok(df::Operator::GtEq),
                "add" => Ok(df::Operator::Plus),
                "subtract" => Ok(df::Operator::Minus),
                "multiply" => Ok(df::Operator::Multiply),
                "divide" => Ok(df::Operator::Divide),
                "and" => Ok(df::Operator::And),
                "or" => Ok(df::Operator::Or),
                other => Err(CommonError::internal_error(format!(
                    "Unsupported function call {}",
                    other
                ))),
            }?;
            if arguments.len() != 2 {
                return Err(CommonError::internal_error(
                    "Binary operator expects 2 arguments",
                ));
            }
            let left = to_df_expr(&arguments[0])?;
            let right = to_df_expr(&arguments[1])?;
            Ok(df::binary_expr(left, op, right))
        }
        _ => Err(CommonError::internal_error(format!(
            "Translation for expression {:?} not implemented",
            expr.kind
        ))),
    }
}

/// Translates a Barks `Literal` into a DataFusion `ScalarValue`.
fn to_df_scalar(literal: &BarksLiteral) -> Result<ScalarValue> {
    match literal {
        BarksLiteral::Boolean { value } => Ok(ScalarValue::Boolean(*value)),
        BarksLiteral::Int8 { value } => Ok(ScalarValue::Int8(*value)),
        BarksLiteral::Int16 { value } => Ok(ScalarValue::Int16(*value)),
        BarksLiteral::Int32 { value } => Ok(ScalarValue::Int32(*value)),
        BarksLiteral::Int64 { value } => Ok(ScalarValue::Int64(*value)),
        BarksLiteral::UInt8 { value } => Ok(ScalarValue::UInt8(*value)),
        BarksLiteral::UInt16 { value } => Ok(ScalarValue::UInt16(*value)),
        BarksLiteral::UInt32 { value } => Ok(ScalarValue::UInt32(*value)),
        BarksLiteral::UInt64 { value } => Ok(ScalarValue::UInt64(*value)),
        BarksLiteral::Float32 { value } => Ok(ScalarValue::Float32(*value)),
        BarksLiteral::Float64 { value } => Ok(ScalarValue::Float64(*value)),
        BarksLiteral::Utf8 { value } => Ok(ScalarValue::Utf8(value.clone())),
        _ => Err(CommonError::internal_error(format!(
            "Unsupported literal for expression translation: {:?}",
            literal
        ))),
    }
}

impl QueryNode<NoMetadata> {
    fn variant_name(&self) -> &'static str {
        match self {
            QueryNode::Read { .. } => "Read",
            QueryNode::Project { .. } => "Project",
            QueryNode::Filter { .. } => "Filter",
            QueryNode::Join(_) => "Join",
            QueryNode::SetOperation(_) => "SetOperation",
            QueryNode::Sort { .. } => "Sort",
            QueryNode::Limit { .. } => "Limit",
            QueryNode::Aggregate(_) => "Aggregate",
            QueryNode::LocalRelation { .. } => "LocalRelation",
            QueryNode::Sample(_) => "Sample",
            QueryNode::Deduplicate(_) => "Deduplicate",
            QueryNode::Range(_) => "Range",
            QueryNode::SubqueryAlias { .. } => "SubqueryAlias",
            _ => "Unknown",
        }
    }
}
