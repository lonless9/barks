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

    /// The FlatMap operation trait object type associated with this data type.
    type FlatMapOperation: Send + Sync + Debug + DynClone + Clone;

    /// The serializable operation enum containing the above operations.
    type SerializableOperation: Send
        + Sync
        + Clone
        + Debug
        + Serialize
        + for<'de> Deserialize<'de>
        + From<Self::MapOperation>
        + From<Self::FilterPredicate>
        + From<Self::FlatMapOperation>;

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

    /// Compute distinct elements from the given data
    /// This method handles the deduplication logic for types that support it
    fn compute_distinct(data: Vec<Self>) -> crate::traits::RddResult<Vec<Self>>;
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

/// Trait for serializable flatMap operations on i32 values
#[typetag::serde(tag = "type")]
pub trait I32FlatMapOperation: Send + Sync + Debug + DynClone {
    /// Execute the flatMap operation on a single i32 item, returning a vector of results
    fn execute(&self, item: i32) -> Vec<i32>;
}
dyn_clone::clone_trait_object!(I32FlatMapOperation);

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

/// Trait for serializable flatMap operations on String values
#[typetag::serde(tag = "type")]
pub trait StringFlatMapOperation: Send + Sync + Debug + DynClone {
    /// Execute the flatMap operation on a single String item, returning a vector of results
    fn execute(&self, item: String) -> Vec<String>;
}
dyn_clone::clone_trait_object!(StringFlatMapOperation);

/// Enum to hold different kinds of serializable i32 operations.
/// This makes it easy to create a chain of different transformations.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SerializableI32Operation {
    Map(Box<dyn I32Operation>),
    Filter(Box<dyn I32Predicate>),
    FlatMap(Box<dyn I32FlatMapOperation>),
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

impl From<Box<dyn I32FlatMapOperation>> for SerializableI32Operation {
    fn from(op: Box<dyn I32FlatMapOperation>) -> Self {
        SerializableI32Operation::FlatMap(op)
    }
}

/// Enum to hold different kinds of serializable String operations.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SerializableStringOperation {
    Map(Box<dyn StringOperation>),
    Filter(Box<dyn StringPredicate>),
    FlatMap(Box<dyn StringFlatMapOperation>),
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

impl From<Box<dyn StringFlatMapOperation>> for SerializableStringOperation {
    fn from(op: Box<dyn StringFlatMapOperation>) -> Self {
        SerializableStringOperation::FlatMap(op)
    }
}

/// Implement RddDataType for i32
impl RddDataType for i32 {
    type MapOperation = Box<dyn I32Operation>;
    type FilterPredicate = Box<dyn I32Predicate>;
    type FlatMapOperation = Box<dyn I32FlatMapOperation>;
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
            SerializableI32Operation::FlatMap(flatmap_op) => data
                .par_iter()
                .flat_map(|item| flatmap_op.execute(*item))
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

    fn compute_distinct(data: Vec<Self>) -> crate::traits::RddResult<Vec<Self>> {
        use std::collections::HashSet;
        let mut seen = HashSet::new();
        let mut result = Vec::new();

        for item in data {
            if seen.insert(item) {
                result.push(item);
            }
        }

        Ok(result)
    }
}

/// Implement RddDataType for String
impl RddDataType for String {
    type MapOperation = Box<dyn StringOperation>;
    type FilterPredicate = Box<dyn StringPredicate>;
    type FlatMapOperation = Box<dyn StringFlatMapOperation>;
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
            SerializableStringOperation::FlatMap(flatmap_op) => data
                .par_iter()
                .flat_map(|item| flatmap_op.execute(item.clone()))
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

    fn compute_distinct(data: Vec<Self>) -> crate::traits::RddResult<Vec<Self>> {
        use std::collections::HashSet;
        let mut seen = HashSet::new();
        let mut result = Vec::new();

        for item in data {
            if seen.insert(item.clone()) {
                result.push(item);
            }
        }

        Ok(result)
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

/// FlatMap operation that splits an integer into a range
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SplitToRangeOperation {
    pub factor: i32,
}

#[typetag::serde]
impl I32FlatMapOperation for SplitToRangeOperation {
    fn execute(&self, item: i32) -> Vec<i32> {
        if item <= 0 {
            vec![]
        } else {
            (0..item.min(self.factor)).collect()
        }
    }
}

/// FlatMap operation that splits a string into words
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SplitWordsOperation;

#[typetag::serde]
impl StringFlatMapOperation for SplitWordsOperation {
    fn execute(&self, item: String) -> Vec<String> {
        item.split_whitespace().map(|s| s.to_string()).collect()
    }
}

/// FlatMap operation that splits a string into characters
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SplitCharsOperation;

#[typetag::serde]
impl StringFlatMapOperation for SplitCharsOperation {
    fn execute(&self, item: String) -> Vec<String> {
        item.chars().map(|c| c.to_string()).collect()
    }
}

/// FlatMap operation that duplicates a (String, i32) tuple based on the i32 value
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DuplicateTupleOperation;

#[typetag::serde]
impl StringI32TupleFlatMapOperation for DuplicateTupleOperation {
    fn execute(&self, item: (String, i32)) -> Vec<(String, i32)> {
        let (string_val, count) = item;
        if count <= 0 {
            vec![]
        } else {
            (0..count)
                .map(|i| (format!("{}-{}", string_val, i), i))
                .collect()
        }
    }
}

/// FlatMap operation that splits a (i32, String) tuple into multiple tuples based on string words
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SplitTupleWordsOperation;

#[typetag::serde]
impl I32StringTupleFlatMapOperation for SplitTupleWordsOperation {
    fn execute(&self, item: (i32, String)) -> Vec<(i32, String)> {
        let (num, text) = item;
        text.split_whitespace()
            .enumerate()
            .map(|(i, word)| (num + i as i32, word.to_string()))
            .collect()
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

/// Trait for serializable flatMap operations on (String, i32) tuple values
#[typetag::serde(tag = "type")]
pub trait StringI32TupleFlatMapOperation: Send + Sync + Debug + DynClone {
    /// Execute the flatMap operation on a single tuple item, returning a vector of results
    fn execute(&self, item: (String, i32)) -> Vec<(String, i32)>;
}
dyn_clone::clone_trait_object!(StringI32TupleFlatMapOperation);

/// Enum to hold different kinds of serializable (String, i32) tuple operations.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SerializableStringI32TupleOperation {
    Map(Box<dyn StringI32TupleOperation>),
    Filter(Box<dyn StringI32TuplePredicate>),
    FlatMap(Box<dyn StringI32TupleFlatMapOperation>),
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

impl From<Box<dyn StringI32TupleFlatMapOperation>> for SerializableStringI32TupleOperation {
    fn from(op: Box<dyn StringI32TupleFlatMapOperation>) -> Self {
        SerializableStringI32TupleOperation::FlatMap(op)
    }
}

/// Implement RddDataType for (String, i32) tuple
impl RddDataType for (String, i32) {
    type MapOperation = Box<dyn StringI32TupleOperation>;
    type FilterPredicate = Box<dyn StringI32TuplePredicate>;
    type FlatMapOperation = Box<dyn StringI32TupleFlatMapOperation>;
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
            SerializableStringI32TupleOperation::FlatMap(flatmap_op) => data
                .par_iter()
                .flat_map(|item| flatmap_op.execute(item.clone()))
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

    fn compute_distinct(data: Vec<Self>) -> crate::traits::RddResult<Vec<Self>> {
        use std::collections::HashSet;
        let mut seen = HashSet::new();
        let mut result = Vec::new();

        for item in data {
            if seen.insert(item.clone()) {
                result.push(item);
            }
        }

        Ok(result)
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

/// Trait for serializable flatMap operations on (i32, String) tuple values
#[typetag::serde(tag = "type")]
pub trait I32StringTupleFlatMapOperation: Send + Sync + Debug + DynClone {
    /// Execute the flatMap operation on a single tuple item, returning a vector of results
    fn execute(&self, item: (i32, String)) -> Vec<(i32, String)>;
}
dyn_clone::clone_trait_object!(I32StringTupleFlatMapOperation);

/// Enum to hold different kinds of serializable (i32, String) tuple operations.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SerializableI32StringTupleOperation {
    Map(Box<dyn I32StringTupleOperation>),
    Filter(Box<dyn I32StringTuplePredicate>),
    FlatMap(Box<dyn I32StringTupleFlatMapOperation>),
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

impl From<Box<dyn I32StringTupleFlatMapOperation>> for SerializableI32StringTupleOperation {
    fn from(op: Box<dyn I32StringTupleFlatMapOperation>) -> Self {
        SerializableI32StringTupleOperation::FlatMap(op)
    }
}

/// Implement RddDataType for (i32, String) tuple
impl RddDataType for (i32, String) {
    type MapOperation = Box<dyn I32StringTupleOperation>;
    type FilterPredicate = Box<dyn I32StringTuplePredicate>;
    type FlatMapOperation = Box<dyn I32StringTupleFlatMapOperation>;
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
            SerializableI32StringTupleOperation::FlatMap(flatmap_op) => data
                .par_iter()
                .flat_map(|item| flatmap_op.execute(item.clone()))
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

    fn compute_distinct(data: Vec<Self>) -> crate::traits::RddResult<Vec<Self>> {
        use std::collections::HashSet;
        let mut seen = HashSet::new();
        let mut result = Vec::new();

        for item in data {
            if seen.insert(item.clone()) {
                result.push(item);
            }
        }

        Ok(result)
    }
}

/// Trait for serializable operations on (String, String) tuple values
#[typetag::serde(tag = "type")]
pub trait StringStringTupleOperation: Send + Sync + Debug + DynClone {
    fn execute(&self, item: (String, String)) -> (String, String);
}
dyn_clone::clone_trait_object!(StringStringTupleOperation);

/// Trait for serializable predicates on (String, String) tuple values
#[typetag::serde(tag = "type")]
pub trait StringStringTuplePredicate: Send + Sync + Debug + DynClone {
    fn test(&self, item: &(String, String)) -> bool;
}
dyn_clone::clone_trait_object!(StringStringTuplePredicate);

/// Trait for serializable flatMap operations on (String, String) tuple values
#[typetag::serde(tag = "type")]
pub trait StringStringTupleFlatMapOperation: Send + Sync + Debug + DynClone {
    fn execute(&self, item: (String, String)) -> Vec<(String, String)>;
}
dyn_clone::clone_trait_object!(StringStringTupleFlatMapOperation);

/// Map operation that swaps the two strings in a tuple
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SwapStringTupleOperation;

#[typetag::serde]
impl StringStringTupleOperation for SwapStringTupleOperation {
    fn execute(&self, item: (String, String)) -> (String, String) {
        (item.1, item.0)
    }
}

/// Map operation that concatenates the two strings with a separator
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ConcatStringTupleOperation {
    pub separator: String,
}

#[typetag::serde]
impl StringStringTupleOperation for ConcatStringTupleOperation {
    fn execute(&self, item: (String, String)) -> (String, String) {
        let concatenated = format!("{}{}{}", item.0, self.separator, item.1);
        (concatenated, item.1)
    }
}

/// Predicate that filters tuples where the first string contains the second
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FirstContainsSecondPredicate;

#[typetag::serde]
impl StringStringTuplePredicate for FirstContainsSecondPredicate {
    fn test(&self, item: &(String, String)) -> bool {
        item.0.contains(&item.1)
    }
}

/// FlatMap operation that splits both strings by whitespace and creates all combinations
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SplitAndCombineOperation;

#[typetag::serde]
impl StringStringTupleFlatMapOperation for SplitAndCombineOperation {
    fn execute(&self, item: (String, String)) -> Vec<(String, String)> {
        let first_words: Vec<&str> = item.0.split_whitespace().collect();
        let second_words: Vec<&str> = item.1.split_whitespace().collect();

        let mut result = Vec::new();
        for first_word in first_words {
            for second_word in &second_words {
                result.push((first_word.to_string(), second_word.to_string()));
            }
        }
        result
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SerializableStringStringTupleOperation {
    Map(Box<dyn StringStringTupleOperation>),
    Filter(Box<dyn StringStringTuplePredicate>),
    FlatMap(Box<dyn StringStringTupleFlatMapOperation>),
}

impl From<Box<dyn StringStringTupleOperation>> for SerializableStringStringTupleOperation {
    fn from(op: Box<dyn StringStringTupleOperation>) -> Self {
        Self::Map(op)
    }
}

impl From<Box<dyn StringStringTuplePredicate>> for SerializableStringStringTupleOperation {
    fn from(pred: Box<dyn StringStringTuplePredicate>) -> Self {
        Self::Filter(pred)
    }
}

impl From<Box<dyn StringStringTupleFlatMapOperation>> for SerializableStringStringTupleOperation {
    fn from(op: Box<dyn StringStringTupleFlatMapOperation>) -> Self {
        Self::FlatMap(op)
    }
}

impl RddDataType for (String, String) {
    type MapOperation = Box<dyn StringStringTupleOperation>;
    type FilterPredicate = Box<dyn StringStringTuplePredicate>;
    type FlatMapOperation = Box<dyn StringStringTupleFlatMapOperation>;
    type SerializableOperation = SerializableStringStringTupleOperation;

    fn apply_operation(
        op: &Self::SerializableOperation,
        data: Vec<Self>,
        _arena: &Bump,
    ) -> Vec<Self> {
        match op {
            SerializableStringStringTupleOperation::Map(map_op) => data
                .par_iter()
                .map(|item| map_op.execute(item.clone()))
                .collect(),
            SerializableStringStringTupleOperation::Filter(filter_op) => data
                .par_iter()
                .filter(|item| filter_op.test(item))
                .cloned()
                .collect(),
            SerializableStringStringTupleOperation::FlatMap(flatmap_op) => data
                .par_iter()
                .flat_map(|item| flatmap_op.execute(item.clone()))
                .collect(),
        }
    }

    fn create_chained_task(
        serialized_partition_data: Vec<u8>,
        operations: Vec<Self::SerializableOperation>,
    ) -> crate::traits::RddResult<Box<dyn crate::distributed::task::Task>> {
        Ok(Box::new(crate::distributed::task::ChainedTask::<(
            String,
            String,
        )>::new(
            serialized_partition_data, operations
        )))
    }

    fn compute_distinct(data: Vec<Self>) -> crate::traits::RddResult<Vec<Self>> {
        use std::collections::HashSet;
        let mut seen = HashSet::new();
        let mut result = Vec::new();

        for item in data {
            if seen.insert(item.clone()) {
                result.push(item);
            }
        }

        Ok(result)
    }
}

pub mod tests;

#[cfg(test)]
mod flatmap_tests {
    use super::*;
    use crate::rdd::DistributedRdd;

    #[test]
    fn test_i32_flatmap_split_to_range() {
        let data = vec![3, 1, 4];
        let rdd = DistributedRdd::from_vec(data);

        let flatmap_op: Box<dyn I32FlatMapOperation> =
            Box::new(SplitToRangeOperation { factor: 10 });
        let result_rdd = rdd.flat_map(flatmap_op);

        let result = result_rdd.collect().unwrap();
        // 3 -> [0, 1, 2], 1 -> [0], 4 -> [0, 1, 2, 3]
        let expected = vec![0, 1, 2, 0, 0, 1, 2, 3];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_string_flatmap_split_words() {
        let data = vec!["hello world".to_string(), "rust programming".to_string()];
        let rdd = DistributedRdd::from_vec(data);

        let flatmap_op: Box<dyn StringFlatMapOperation> = Box::new(SplitWordsOperation);
        let result_rdd = rdd.flat_map(flatmap_op);

        let result = result_rdd.collect().unwrap();
        let expected = vec!["hello", "world", "rust", "programming"];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_string_flatmap_split_chars() {
        let data = vec!["hi".to_string(), "ok".to_string()];
        let rdd = DistributedRdd::from_vec(data);

        let flatmap_op: Box<dyn StringFlatMapOperation> = Box::new(SplitCharsOperation);
        let result_rdd = rdd.flat_map(flatmap_op);

        let result = result_rdd.collect().unwrap();
        let expected = vec!["h", "i", "o", "k"];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_string_i32_tuple_flatmap_duplicate() {
        let data = vec![("hello".to_string(), 3), ("world".to_string(), 2)];
        let rdd = DistributedRdd::from_vec(data);

        let flatmap_op: Box<dyn StringI32TupleFlatMapOperation> = Box::new(DuplicateTupleOperation);
        let result_rdd = rdd.flat_map(flatmap_op);

        let result = result_rdd.collect().unwrap();
        let expected = vec![
            ("hello-0".to_string(), 0),
            ("hello-1".to_string(), 1),
            ("hello-2".to_string(), 2),
            ("world-0".to_string(), 0),
            ("world-1".to_string(), 1),
        ];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_i32_string_tuple_flatmap_split_words() {
        let data = vec![
            (10, "hello world".to_string()),
            (20, "rust programming".to_string()),
        ];
        let rdd = DistributedRdd::from_vec(data);

        let flatmap_op: Box<dyn I32StringTupleFlatMapOperation> =
            Box::new(SplitTupleWordsOperation);
        let result_rdd = rdd.flat_map(flatmap_op);

        let result = result_rdd.collect().unwrap();
        let expected = vec![
            (10, "hello".to_string()),
            (11, "world".to_string()),
            (20, "rust".to_string()),
            (21, "programming".to_string()),
        ];
        assert_eq!(result, expected);
    }
}
