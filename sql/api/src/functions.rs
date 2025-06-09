//! SQL functions for Barks
//!
//! This module provides SQL function definitions and utilities.

use datafusion::logical_expr::Expr;

/// String functions
pub mod string {
    use super::*;

    /// Concatenate strings
    pub fn concat(exprs: Vec<Expr>) -> Expr {
        datafusion::functions::expr_fn::concat(exprs)
    }

    /// Get string length
    pub fn length(expr: Expr) -> Expr {
        datafusion::functions::expr_fn::length(expr)
    }

    /// Convert to uppercase
    pub fn upper(expr: Expr) -> Expr {
        datafusion::functions::expr_fn::upper(expr)
    }

    /// Convert to lowercase
    pub fn lower(expr: Expr) -> Expr {
        datafusion::functions::expr_fn::lower(expr)
    }
}

/// Math functions
pub mod math {
    use super::*;

    /// Absolute value
    pub fn abs(expr: Expr) -> Expr {
        datafusion::functions::expr_fn::abs(expr)
    }

    /// Round to specified decimal places
    pub fn round(expr: Expr, decimals: Option<i64>) -> Expr {
        match decimals {
            Some(d) => {
                datafusion::functions::expr_fn::round(vec![expr, datafusion::logical_expr::lit(d)])
            }
            None => datafusion::functions::expr_fn::round(vec![expr]),
        }
    }

    /// Square root
    pub fn sqrt(expr: Expr) -> Expr {
        datafusion::functions::expr_fn::sqrt(expr)
    }
}

/// Date/time functions
pub mod datetime {
    use super::*;

    /// Extract year from date
    pub fn year(expr: Expr) -> Expr {
        datafusion::functions::expr_fn::date_part(datafusion::logical_expr::lit("year"), expr)
    }

    /// Extract month from date
    pub fn month(expr: Expr) -> Expr {
        datafusion::functions::expr_fn::date_part(datafusion::logical_expr::lit("month"), expr)
    }

    /// Extract day from date
    pub fn day(expr: Expr) -> Expr {
        datafusion::functions::expr_fn::date_part(datafusion::logical_expr::lit("day"), expr)
    }
}
