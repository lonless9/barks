//! Serializable operations for distributed RDD execution
//!
//! This module provides serializable operation types that can replace
//! closures in RDD transformations, enabling distributed execution.

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
    fn apply_operation(op: &Self::SerializableOperation, data: Vec<Self>) -> Vec<Self>;
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
    fn test(&self, item: &String) -> bool;
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

    fn apply_operation(op: &Self::SerializableOperation, data: Vec<Self>) -> Vec<Self> {
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
}

/// Implement RddDataType for String
impl RddDataType for String {
    type MapOperation = Box<dyn StringOperation>;
    type FilterPredicate = Box<dyn StringPredicate>;
    type SerializableOperation = SerializableStringOperation;

    fn apply_operation(op: &Self::SerializableOperation, data: Vec<Self>) -> Vec<Self> {
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
    fn test(&self, item: &String) -> bool {
        item.len() >= self.min_length
    }
}

/// Identity operation for i32 that returns the item unchanged
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct I32IdentityOperation;

#[typetag::serde]
impl I32Operation for I32IdentityOperation {
    fn execute(&self, item: i32) -> i32 {
        item
    }
}

/// Always true predicate for i32
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct I32AlwaysTruePredicate;

#[typetag::serde]
impl I32Predicate for I32AlwaysTruePredicate {
    fn test(&self, _item: &i32) -> bool {
        true
    }
}

/// Identity operation for String that returns the item unchanged
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StringIdentityOperation;

#[typetag::serde]
impl StringOperation for StringIdentityOperation {
    fn execute(&self, item: String) -> String {
        item
    }
}

/// Always true predicate for String
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StringAlwaysTruePredicate;

#[typetag::serde]
impl StringPredicate for StringAlwaysTruePredicate {
    fn test(&self, _item: &String) -> bool {
        true
    }
}

pub mod tests;
