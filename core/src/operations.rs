//! Serializable operations for distributed RDD execution
//!
//! This module provides serializable operation types that can replace
//! closures in RDD transformations, enabling distributed execution.

use bumpalo::Bump;
use dyn_clone::DynClone;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::{any::Any, fmt::Debug};

/// A helper trait that associates data types with their serializable operations and predicates.
/// This is the key to implementing generic DistributedRdd<T> while preserving typetag extensibility.
pub trait RddDataType:
    Any
    + Send
    + Sync
    + Clone
    + Debug
    + Serialize
    + for<'de> Deserialize<'de>
    + bincode::Encode
    + bincode::Decode<()>
    + 'static
{
    /// The Map operation trait object type associated with this data type.
    type MapOperation: Send + Sync + Debug + DynClone + Clone;

    /// The Filter operation trait object type associated with this data type.
    type FilterPredicate: Send + Sync + Debug + DynClone + Clone;

    /// The serializable operation enum containing the above operations.
    type SerializableOperation: Send
        + Sync
        + Clone
        + Debug
        + Serialize
        + for<'de> Deserialize<'de>
        + From<Self::MapOperation>
        + From<Self::FilterPredicate>;

    /// Applies a serializable operation to a vector of data.
    /// The arena parameter provides efficient memory allocation for intermediate computations.
    fn apply_operation(
        op: &Self::SerializableOperation,
        data: Vec<Self>,
        arena: &Bump,
    ) -> Vec<Self>;

    /// Creates a chained task for distributed execution
    fn create_chained_task(
        serialized_partition_data: Vec<u8>,
        operations: Vec<Self::SerializableOperation>,
    ) -> crate::traits::RddResult<Box<dyn crate::distributed::task::Task>>;
}

/// Trait for serializable operations on i32 values
#[typetag::serde(tag = "type")]
pub trait I32Operation: Send + Sync + Debug + DynClone {
    /// Execute the operation on a single i32 item
    fn execute(&self, item: i32) -> i32;
}
dyn_clone::clone_trait_object!(I32Operation);

/// Trait for serializable predicates on i32 values
#[typetag::serde(tag = "type")]
pub trait I32Predicate: Send + Sync + Debug + DynClone {
    /// Execute the predicate on a single i32 item
    fn test(&self, item: &i32) -> bool;
}
dyn_clone::clone_trait_object!(I32Predicate);

/// Trait for serializable operations on String values
#[typetag::serde(tag = "type")]
pub trait StringOperation: Send + Sync + Debug + DynClone {
    /// Execute the operation on a single String item
    fn execute(&self, item: String) -> String;
}
dyn_clone::clone_trait_object!(StringOperation);

/// Trait for serializable predicates on String values
#[typetag::serde(tag = "type")]
pub trait StringPredicate: Send + Sync + Debug + DynClone {
    /// Execute the predicate on a single String item
    fn test(&self, item: &str) -> bool;
}
dyn_clone::clone_trait_object!(StringPredicate);

/// Enum to hold different kinds of serializable i32 operations.
/// This makes it easy to create a chain of different transformations.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SerializableI32Operation {
    Map(Box<dyn I32Operation>),
    Filter(Box<dyn I32Predicate>),
}

impl From<Box<dyn I32Operation>> for SerializableI32Operation {
    fn from(op: Box<dyn I32Operation>) -> Self {
        SerializableI32Operation::Map(op)
    }
}

impl From<Box<dyn I32Predicate>> for SerializableI32Operation {
    fn from(pred: Box<dyn I32Predicate>) -> Self {
        SerializableI32Operation::Filter(pred)
    }
}

/// Enum to hold different kinds of serializable String operations.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SerializableStringOperation {
    Map(Box<dyn StringOperation>),
    Filter(Box<dyn StringPredicate>),
}

impl From<Box<dyn StringOperation>> for SerializableStringOperation {
    fn from(op: Box<dyn StringOperation>) -> Self {
        SerializableStringOperation::Map(op)
    }
}

impl From<Box<dyn StringPredicate>> for SerializableStringOperation {
    fn from(pred: Box<dyn StringPredicate>) -> Self {
        SerializableStringOperation::Filter(pred)
    }
}

/// Implement RddDataType for i32
impl RddDataType for i32 {
    type MapOperation = Box<dyn I32Operation>;
    type FilterPredicate = Box<dyn I32Predicate>;
    type SerializableOperation = SerializableI32Operation;

    fn apply_operation(
        op: &Self::SerializableOperation,
        data: Vec<Self>,
        _arena: &Bump,
    ) -> Vec<Self> {
        // For primitive types like i32, we don't need to use the arena for intermediate allocations
        // since the data is small and the operations are simple. However, the arena is available
        // for more complex operations that might benefit from bump allocation.
        match op {
            SerializableI32Operation::Map(map_op) => {
                data.par_iter().map(|item| map_op.execute(*item)).collect()
            }
            SerializableI32Operation::Filter(filter_op) => data
                .par_iter()
                .filter(|&item| filter_op.test(item))
                .cloned()
                .collect(),
        }
    }

    fn create_chained_task(
        serialized_partition_data: Vec<u8>,
        operations: Vec<Self::SerializableOperation>,
    ) -> crate::traits::RddResult<Box<dyn crate::distributed::task::Task>> {
        Ok(Box::new(crate::distributed::task::ChainedTask::<i32>::new(
            serialized_partition_data,
            operations,
        )))
    }
}

/// Implement RddDataType for String
impl RddDataType for String {
    type MapOperation = Box<dyn StringOperation>;
    type FilterPredicate = Box<dyn StringPredicate>;
    type SerializableOperation = SerializableStringOperation;

    fn apply_operation(
        op: &Self::SerializableOperation,
        data: Vec<Self>,
        _arena: &Bump,
    ) -> Vec<Self> {
        // For String operations, we could potentially use the arena for intermediate string
        // allocations in more complex scenarios. For now, we use standard allocation.
        match op {
            SerializableStringOperation::Map(map_op) => data
                .par_iter()
                .map(|item| map_op.execute(item.clone()))
                .collect(),
            SerializableStringOperation::Filter(filter_op) => data
                .par_iter()
                .filter(|item| filter_op.test(item))
                .cloned()
                .collect(),
        }
    }

    fn create_chained_task(
        serialized_partition_data: Vec<u8>,
        operations: Vec<Self::SerializableOperation>,
    ) -> crate::traits::RddResult<Box<dyn crate::distributed::task::Task>> {
        Ok(Box::new(
            crate::distributed::task::ChainedTask::<String>::new(
                serialized_partition_data,
                operations,
            ),
        ))
    }
}

/// Map operation that doubles an integer
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DoubleOperation;

#[typetag::serde]
impl I32Operation for DoubleOperation {
    fn execute(&self, item: i32) -> i32 {
        item * 2
    }
}

/// Map operation that squares an integer
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SquareOperation;

#[typetag::serde]
impl I32Operation for SquareOperation {
    fn execute(&self, item: i32) -> i32 {
        item * item
    }
}

/// Map operation that adds a constant to an integer
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AddConstantOperation {
    pub constant: i32,
}

#[typetag::serde]
impl I32Operation for AddConstantOperation {
    fn execute(&self, item: i32) -> i32 {
        item + self.constant
    }
}

/// Filter predicate that checks if a number is even
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EvenPredicate;

#[typetag::serde]
impl I32Predicate for EvenPredicate {
    fn test(&self, item: &i32) -> bool {
        item % 2 == 0
    }
}

/// Filter predicate that checks if a number is greater than a threshold
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct GreaterThanPredicate {
    pub threshold: i32,
}

#[typetag::serde]
impl I32Predicate for GreaterThanPredicate {
    fn test(&self, item: &i32) -> bool {
        *item > self.threshold
    }
}

/// Generic map operation for string transformations
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ToUpperCaseOperation;

#[typetag::serde]
impl StringOperation for ToUpperCaseOperation {
    fn execute(&self, item: String) -> String {
        item.to_uppercase()
    }
}

/// Generic filter predicate for string length
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MinLengthPredicate {
    pub min_length: usize,
}

#[typetag::serde]
impl StringPredicate for MinLengthPredicate {
    fn test(&self, item: &str) -> bool {
        item.len() >= self.min_length
    }
}

/// Trait for serializable operations on (String, i32) tuple values
#[typetag::serde(tag = "type")]
pub trait StringI32TupleOperation: Send + Sync + Debug + DynClone {
    /// Execute the operation on a single tuple item
    fn execute(&self, item: (String, i32)) -> (String, i32);
}
dyn_clone::clone_trait_object!(StringI32TupleOperation);

/// Trait for serializable predicates on (String, i32) tuple values
#[typetag::serde(tag = "type")]
pub trait StringI32TuplePredicate: Send + Sync + Debug + DynClone {
    /// Execute the predicate on a single tuple item
    fn test(&self, item: &(String, i32)) -> bool;
}
dyn_clone::clone_trait_object!(StringI32TuplePredicate);

/// Enum to hold different kinds of serializable (String, i32) tuple operations.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SerializableStringI32TupleOperation {
    Map(Box<dyn StringI32TupleOperation>),
    Filter(Box<dyn StringI32TuplePredicate>),
}

impl From<Box<dyn StringI32TupleOperation>> for SerializableStringI32TupleOperation {
    fn from(op: Box<dyn StringI32TupleOperation>) -> Self {
        SerializableStringI32TupleOperation::Map(op)
    }
}

impl From<Box<dyn StringI32TuplePredicate>> for SerializableStringI32TupleOperation {
    fn from(pred: Box<dyn StringI32TuplePredicate>) -> Self {
        SerializableStringI32TupleOperation::Filter(pred)
    }
}

/// Implement RddDataType for (String, i32) tuple
impl RddDataType for (String, i32) {
    type MapOperation = Box<dyn StringI32TupleOperation>;
    type FilterPredicate = Box<dyn StringI32TuplePredicate>;
    type SerializableOperation = SerializableStringI32TupleOperation;

    fn apply_operation(
        op: &Self::SerializableOperation,
        data: Vec<Self>,
        _arena: &Bump,
    ) -> Vec<Self> {
        match op {
            SerializableStringI32TupleOperation::Map(map_op) => data
                .par_iter()
                .map(|item| map_op.execute(item.clone()))
                .collect(),
            SerializableStringI32TupleOperation::Filter(filter_op) => data
                .par_iter()
                .filter(|item| filter_op.test(item))
                .cloned()
                .collect(),
        }
    }

    fn create_chained_task(
        serialized_partition_data: Vec<u8>,
        operations: Vec<Self::SerializableOperation>,
    ) -> crate::traits::RddResult<Box<dyn crate::distributed::task::Task>> {
        Ok(Box::new(crate::distributed::task::ChainedTask::<(
            String,
            i32,
        )>::new(
            serialized_partition_data, operations
        )))
    }
}

/// Trait for serializable operations on (i32, String) tuple values
#[typetag::serde(tag = "type")]
pub trait I32StringTupleOperation: Send + Sync + Debug + DynClone {
    /// Execute the operation on a single tuple item
    fn execute(&self, item: (i32, String)) -> (i32, String);
}
dyn_clone::clone_trait_object!(I32StringTupleOperation);

/// Trait for serializable predicates on (i32, String) tuple values
#[typetag::serde(tag = "type")]
pub trait I32StringTuplePredicate: Send + Sync + Debug + DynClone {
    /// Execute the predicate on a single tuple item
    fn test(&self, item: &(i32, String)) -> bool;
}
dyn_clone::clone_trait_object!(I32StringTuplePredicate);

/// Enum to hold different kinds of serializable (i32, String) tuple operations.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SerializableI32StringTupleOperation {
    Map(Box<dyn I32StringTupleOperation>),
    Filter(Box<dyn I32StringTuplePredicate>),
}

impl From<Box<dyn I32StringTupleOperation>> for SerializableI32StringTupleOperation {
    fn from(op: Box<dyn I32StringTupleOperation>) -> Self {
        SerializableI32StringTupleOperation::Map(op)
    }
}

impl From<Box<dyn I32StringTuplePredicate>> for SerializableI32StringTupleOperation {
    fn from(pred: Box<dyn I32StringTuplePredicate>) -> Self {
        SerializableI32StringTupleOperation::Filter(pred)
    }
}

/// Implement RddDataType for (i32, String) tuple
impl RddDataType for (i32, String) {
    type MapOperation = Box<dyn I32StringTupleOperation>;
    type FilterPredicate = Box<dyn I32StringTuplePredicate>;
    type SerializableOperation = SerializableI32StringTupleOperation;

    fn apply_operation(
        op: &Self::SerializableOperation,
        data: Vec<Self>,
        _arena: &Bump,
    ) -> Vec<Self> {
        match op {
            SerializableI32StringTupleOperation::Map(map_op) => data
                .par_iter()
                .map(|item| map_op.execute(item.clone()))
                .collect(),
            SerializableI32StringTupleOperation::Filter(filter_op) => data
                .par_iter()
                .filter(|item| filter_op.test(item))
                .cloned()
                .collect(),
        }
    }

    fn create_chained_task(
        serialized_partition_data: Vec<u8>,
        operations: Vec<Self::SerializableOperation>,
    ) -> crate::traits::RddResult<Box<dyn crate::distributed::task::Task>> {
        Ok(Box::new(crate::distributed::task::ChainedTask::<(
            i32,
            String,
        )>::new(
            serialized_partition_data, operations
        )))
    }
}

// For simplicity in tests, implement RddDataType for (String, String) by reusing String operations
impl RddDataType for (String, String) {
    type MapOperation = Box<dyn StringOperation>;
    type FilterPredicate = Box<dyn StringPredicate>;
    type SerializableOperation = SerializableStringOperation;

    fn apply_operation(
        op: &Self::SerializableOperation,
        data: Vec<Self>,
        _arena: &Bump,
    ) -> Vec<Self> {
        // For simplicity, we'll just pass through the data unchanged for tuple operations
        // In a real implementation, you'd want proper tuple-specific operations
        match op {
            SerializableStringOperation::Map(_) => data, // Pass through unchanged
            SerializableStringOperation::Filter(_) => data, // Pass through unchanged
        }
    }

    fn create_chained_task(
        serialized_partition_data: Vec<u8>,
        operations: Vec<Self::SerializableOperation>,
    ) -> crate::traits::RddResult<Box<dyn crate::distributed::task::Task>> {
        // For simplicity, reuse the String task implementation
        Ok(Box::new(
            crate::distributed::task::ChainedTask::<String>::new(
                serialized_partition_data,
                operations,
            ),
        ))
    }
}

pub mod tests;
