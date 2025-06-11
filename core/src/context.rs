//! The main context for a Barks application.

use datafusion::execution::context::{SessionContext, SessionState};
use datafusion::prelude::SessionConfig;

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
}

impl Default for BarksContext {
    fn default() -> Self {
        Self::new()
    }
}
