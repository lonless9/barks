//! Serializable operations for distributed RDD execution
//!
//! This module provides serializable operation types that can replace
//! closures in RDD transformations, enabling distributed execution.

use dyn_clone::DynClone;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

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

#[cfg(test)]
mod basic_tests {
    use super::*;

    #[test]
    fn test_double_operation() {
        let op = DoubleOperation;
        assert_eq!(op.execute(5), 10);
        assert_eq!(op.execute(-3), -6);
    }

    #[test]
    fn test_square_operation() {
        let op = SquareOperation;
        assert_eq!(op.execute(4), 16);
        assert_eq!(op.execute(-3), 9);
    }

    #[test]
    fn test_add_constant_operation() {
        let op = AddConstantOperation { constant: 10 };
        assert_eq!(op.execute(5), 15);
        assert_eq!(op.execute(-3), 7);
    }

    #[test]
    fn test_even_predicate() {
        let pred = EvenPredicate;
        assert!(pred.test(&4));
        assert!(!pred.test(&5));
        assert!(pred.test(&0));
        assert!(!pred.test(&-3));
    }

    #[test]
    fn test_greater_than_predicate() {
        let pred = GreaterThanPredicate { threshold: 10 };
        assert!(pred.test(&15));
        assert!(!pred.test(&5));
        assert!(!pred.test(&10));
    }

    #[test]
    fn test_string_operations() {
        let op = ToUpperCaseOperation;
        assert_eq!(op.execute("hello".to_string()), "HELLO");

        let pred = MinLengthPredicate { min_length: 3 };
        assert!(pred.test(&"hello".to_string()));
        assert!(!pred.test(&"hi".to_string()));
    }

    #[test]
    fn test_serialization() {
        let op: Box<dyn I32Operation> = Box::new(DoubleOperation);
        let serialized = serde_json::to_string(&op).unwrap();
        let deserialized: Box<dyn I32Operation> = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.execute(5), 10);

        let pred: Box<dyn I32Predicate> = Box::new(EvenPredicate);
        let serialized = serde_json::to_string(&pred).unwrap();
        let deserialized: Box<dyn I32Predicate> = serde_json::from_str(&serialized).unwrap();
        assert!(deserialized.test(&4));
        assert!(!deserialized.test(&5));
    }
}
