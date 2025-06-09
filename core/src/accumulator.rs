//! Accumulator implementation
//!
//! Accumulators are variables that are only "added" to through an associative and commutative operation
//! and can therefore be efficiently supported in parallel. They can be used to implement counters
//! (as in MapReduce) or sums.

use crate::traits::Data;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

/// Unique identifier for an accumulator
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccumulatorId(pub String);

impl AccumulatorId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }
}

impl Default for AccumulatorId {
    fn default() -> Self {
        Self::new()
    }
}

/// Trait for accumulator operations
pub trait AccumulatorOp<T>: Send + Sync + std::fmt::Debug {
    /// Add a value to the accumulator
    fn add(&self, current: T, value: T) -> T;

    /// Get the zero/identity value for this accumulator
    fn zero(&self) -> T;
}

/// Simple sum accumulator for numeric types
#[derive(Debug, Clone)]
pub struct SumAccumulator<T> {
    _phantom: std::marker::PhantomData<T>,
}

impl<T> SumAccumulator<T> {
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T> Default for SumAccumulator<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> AccumulatorOp<T> for SumAccumulator<T>
where
    T: std::ops::Add<Output = T> + Default + Clone + Send + Sync + std::fmt::Debug,
{
    fn add(&self, current: T, value: T) -> T {
        current + value
    }

    fn zero(&self) -> T {
        T::default()
    }
}

/// Count accumulator for counting occurrences
#[derive(Debug, Clone)]
pub struct CountAccumulator;

impl AccumulatorOp<u64> for CountAccumulator {
    fn add(&self, current: u64, _value: u64) -> u64 {
        current + 1
    }

    fn zero(&self) -> u64 {
        0
    }
}

/// Accumulator variable that can be used in distributed computations
#[derive(Debug)]
pub struct Accumulator<T: Data> {
    /// Unique identifier for this accumulator
    pub id: AccumulatorId,
    /// Name of the accumulator (for debugging)
    pub name: String,
    /// Current value (only valid on driver)
    value: Arc<RwLock<T>>,
    /// Operation for combining values
    op: Arc<dyn AccumulatorOp<T>>,
}

impl<T: Data> Accumulator<T> {
    /// Create a new accumulator with the given operation
    pub fn new(name: String, op: Arc<dyn AccumulatorOp<T>>) -> Self {
        let zero_value = op.zero();
        Self {
            id: AccumulatorId::new(),
            name,
            value: Arc::new(RwLock::new(zero_value)),
            op,
        }
    }

    /// Get the current value of the accumulator (driver only)
    pub async fn value(&self) -> T {
        self.value.read().await.clone()
    }

    /// Add a value to the accumulator (used internally)
    pub async fn add(&self, value: T) {
        let mut current = self.value.write().await;
        *current = self.op.add(current.clone(), value);
    }

    /// Reset the accumulator to zero
    pub async fn reset(&self) {
        let mut current = self.value.write().await;
        *current = self.op.zero();
    }

    /// Get the accumulator ID
    pub fn id(&self) -> &AccumulatorId {
        &self.id
    }

    /// Get the accumulator name
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// A type-erased accumulator trait that allows storing accumulators of different
/// types in the AccumulatorManager and applying updates to them.
#[async_trait::async_trait]
pub trait ErasedAccumulator: Send + Sync + std::fmt::Debug {
    /// Get the accumulator ID.
    fn id(&self) -> &AccumulatorId;
    /// Get the accumulator name.
    fn name(&self) -> &str;
    /// Add a value from a byte slice. This method handles deserialization internally.
    async fn add_from_bytes(&self, value_bytes: &[u8]);
    /// Downcast to `Any` to allow inspection of the underlying type if needed.
    fn as_any(&self) -> &dyn Any;
}

#[async_trait::async_trait]
impl<T: Data> ErasedAccumulator for Accumulator<T> {
    fn id(&self) -> &AccumulatorId {
        &self.id
    }
    fn name(&self) -> &str {
        &self.name
    }
    async fn add_from_bytes(&self, value_bytes: &[u8]) {
        if let Ok((value, _)) =
            bincode::decode_from_slice::<T, _>(value_bytes, bincode::config::standard())
        {
            self.add(value).await;
        }
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Accumulator update sent from executor to driver
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccumulatorUpdate {
    /// Accumulator ID
    pub id: AccumulatorId,
    /// Accumulator name
    pub name: String,
    /// Serialized value to add
    pub value: Vec<u8>,
}

impl AccumulatorUpdate {
    /// Create an accumulator update
    pub fn new<T: Data>(id: AccumulatorId, name: String, value: &T) -> Result<Self, String> {
        let serialized_value = bincode::encode_to_vec(value, bincode::config::standard())
            .map_err(|e| format!("Failed to serialize accumulator value: {}", e))?;

        Ok(Self {
            id,
            name,
            value: serialized_value,
        })
    }

    /// Deserialize the value
    pub fn deserialize_value<T: Data>(&self) -> Result<T, String> {
        let (value, _): (T, _) =
            bincode::decode_from_slice(&self.value, bincode::config::standard())
                .map_err(|e| format!("Failed to deserialize accumulator value: {}", e))?;
        Ok(value)
    }
}

/// Manager for accumulators on the driver
#[derive(Debug, Default)]
pub struct AccumulatorManager {
    accumulators: Arc<RwLock<HashMap<AccumulatorId, Arc<dyn ErasedAccumulator>>>>,
}

/// Metadata about an accumulator
#[derive(Debug, Clone)]
pub struct AccumulatorMetadata {
    name: String,
    value_type: String,
}

impl AccumulatorMetadata {
    /// Get the accumulator name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the accumulator value type
    pub fn value_type(&self) -> &str {
        &self.value_type
    }
}

impl AccumulatorManager {
    /// Create a new accumulator manager
    pub fn new() -> Self {
        Self {
            accumulators: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a new accumulator
    pub async fn register<T: Data>(&self, accumulator: Arc<Accumulator<T>>) {
        let mut accumulators = self.accumulators.write().await;
        accumulators.insert(
            accumulator.id().clone(),
            accumulator as Arc<dyn ErasedAccumulator>,
        );
    }

    /// Process accumulator updates from executors
    pub async fn process_updates(&self, updates: &[AccumulatorUpdate]) {
        let accumulators = self.accumulators.read().await;
        for update in updates {
            if let Some(accumulator) = accumulators.get(&update.id) {
                accumulator.add_from_bytes(&update.value).await;
            }
        }
    }

    /// Get accumulator metadata
    pub async fn get_metadata(&self, _id: &AccumulatorId) -> Option<AccumulatorMetadata> {
        let _accumulators = self.accumulators.read().await;
        // The concept of AccumulatorMetadata is now somewhat redundant.
        // We could extract info from the ErasedAccumulator if needed. For now, this is unused.
        None
    }

    /// List all registered accumulators
    pub async fn list_accumulators(&self) -> Vec<(AccumulatorId, AccumulatorMetadata)> {
        let accumulators = self.accumulators.read().await;
        accumulators
            .iter()
            .map(|(id, acc)| {
                (
                    id.clone(),
                    // Reconstruct metadata for compatibility with tests if needed
                    AccumulatorMetadata {
                        name: acc.name().to_string(),
                        value_type: "".to_string(),
                    },
                )
            })
            .collect()
    }

    /// Remove an accumulator
    pub async fn remove_accumulator(&self, id: &AccumulatorId) -> bool {
        let mut accumulators = self.accumulators.write().await;
        accumulators.remove(id).is_some()
    }

    /// Clear all accumulators
    pub async fn clear(&self) {
        let mut accumulators = self.accumulators.write().await;
        accumulators.clear();
    }
}

/// Registry for accumulator updates on executors
#[derive(Debug, Default)]
pub struct AccumulatorRegistry {
    /// Pending updates to be sent to driver
    updates: Arc<RwLock<Vec<AccumulatorUpdate>>>,
}

impl AccumulatorRegistry {
    /// Create a new accumulator registry
    pub fn new() -> Self {
        Self {
            updates: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Add an accumulator update
    pub async fn add_update(&self, update: AccumulatorUpdate) {
        let mut updates = self.updates.write().await;
        updates.push(update);
    }

    /// Get and clear all pending updates
    pub async fn drain_updates(&self) -> Vec<AccumulatorUpdate> {
        let mut updates = self.updates.write().await;
        std::mem::take(&mut *updates)
    }

    /// Get the number of pending updates
    pub async fn pending_count(&self) -> usize {
        let updates = self.updates.read().await;
        updates.len()
    }

    /// Clear all pending updates
    pub async fn clear(&self) {
        let mut updates = self.updates.write().await;
        updates.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_sum_accumulator() {
        let op = Arc::new(SumAccumulator::<i32>::new());
        let acc = Accumulator::new("test_sum".to_string(), op);

        assert_eq!(acc.value().await, 0);

        acc.add(5).await;
        assert_eq!(acc.value().await, 5);

        acc.add(10).await;
        assert_eq!(acc.value().await, 15);

        acc.reset().await;
        assert_eq!(acc.value().await, 0);
    }

    #[tokio::test]
    async fn test_count_accumulator() {
        let op = Arc::new(CountAccumulator);
        let acc = Accumulator::new("test_count".to_string(), op);

        assert_eq!(acc.value().await, 0);

        acc.add(1).await; // Value doesn't matter for count
        assert_eq!(acc.value().await, 1);

        acc.add(1).await;
        assert_eq!(acc.value().await, 2);
    }

    #[tokio::test]
    async fn test_accumulator_update() {
        let value = 42i32;
        let update =
            AccumulatorUpdate::new(AccumulatorId::new(), "test".to_string(), &value).unwrap();

        let deserialized: i32 = update.deserialize_value().unwrap();
        assert_eq!(deserialized, value);
    }

    #[tokio::test]
    async fn test_accumulator_manager() {
        let manager = AccumulatorManager::new();
        let op = Arc::new(SumAccumulator::<i32>::new());
        let acc = Arc::new(Accumulator::new("test_acc".to_string(), op));

        // Register accumulator
        manager.register(acc.clone()).await;

        // Process updates
        let update = AccumulatorUpdate::new(acc.id.clone(), acc.name.clone(), &10i32).unwrap();
        manager.process_updates(&[update]).await;

        assert_eq!(acc.value().await, 10);

        // List accumulators
        let list = manager.list_accumulators().await;
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].0, acc.id);
    }

    #[tokio::test]
    async fn test_accumulator_registry() {
        let registry = AccumulatorRegistry::new();

        let update1 =
            AccumulatorUpdate::new(AccumulatorId::new(), "test1".to_string(), &5i32).unwrap();

        let update2 =
            AccumulatorUpdate::new(AccumulatorId::new(), "test2".to_string(), &10i32).unwrap();

        registry.add_update(update1).await;
        registry.add_update(update2).await;

        assert_eq!(registry.pending_count().await, 2);

        let updates = registry.drain_updates().await;
        assert_eq!(updates.len(), 2);
        assert_eq!(registry.pending_count().await, 0);
    }
}
