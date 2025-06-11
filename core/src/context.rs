//! The main context for a Barks application.

use crate::logical_plan::{QueryNode, QueryPlan, Range};
use crate::rdd::{DefaultDistributedDataset, DistributedDataset};
use crate::types::NoMetadata;
use datafusion::execution::context::{SessionContext, SessionState};
use datafusion::prelude::SessionConfig;
use std::sync::Arc;

/// `BarksContext` is the main entry point for Barks functionality.
///
/// It acts as a wrapper around DataFusion's `SessionContext` and will be extended
/// to manage cluster resources, schedulers, and caching services.
#[derive(Clone)]
pub struct BarksContext {
    /// The underlying DataFusion session context, which provides query planning and execution capabilities.
    df_session_ctx: SessionContext,
    // In the future, this will hold references to:
    // - A cluster manager client
    // - A distributed cache manager
    // - A scheduler (e.g., Ballista)
}

impl BarksContext {
    /// Creates a new `BarksContext` with a default configuration.
    pub fn new() -> Self {
        Self {
            df_session_ctx: SessionContext::new(),
        }
    }

    /// Creates a new `BarksContext` with a specific DataFusion `SessionState`.
    pub fn new_with_state(state: SessionState) -> Self {
        Self {
            df_session_ctx: SessionContext::new_with_state(state),
        }
    }

    /// Creates a new `BarksContext` with a specific DataFusion `SessionConfig`.
    pub fn new_with_config(config: SessionConfig) -> Self {
        Self {
            df_session_ctx: SessionContext::new_with_config(config),
        }
    }

    /// Returns a clone of the underlying DataFusion `SessionContext`.
    pub fn df_session_ctx(&self) -> SessionContext {
        self.df_session_ctx.clone()
    }

    /// Creates a `DistributedDataset` from a range of numbers.
    pub fn range(
        &self,
        start: i64,
        end: i64,
        step: i64,
        num_partitions: Option<usize>,
    ) -> Arc<dyn DistributedDataset> {
        let plan = QueryPlan {
            node: QueryNode::Range(Range {
                start: Some(start),
                end,
                step,
                num_partitions,
            }),
            plan_id: None,
            metadata: NoMetadata,
        };
        Arc::new(DefaultDistributedDataset::new(
            Arc::new(self.clone()),
            Arc::new(plan),
        ))
    }
}

impl Default for BarksContext {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_range_dataset() {
        let ctx = BarksContext::new();
        let dataset = ctx.range(0, 10, 1, Some(2));

        // Test that we can collect the data
        let results = dataset.collect().await.unwrap();
        assert!(!results.is_empty());

        // Test that we can count the data
        let count = dataset.count().await.unwrap();
        assert_eq!(count, 10);
    }

    #[tokio::test]
    async fn test_range_dataset_with_step() {
        let ctx = BarksContext::new();
        let dataset = ctx.range(0, 10, 2, Some(1));

        let count = dataset.count().await.unwrap();
        assert_eq!(count, 5); // 0, 2, 4, 6, 8
    }
}
