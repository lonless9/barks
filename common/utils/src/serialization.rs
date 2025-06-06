//! Serialization utilities for Barks
//!
//! This module provides utility functions for serialization and deserialization.

use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// Trait for types that can be serialized to bytes
pub trait ToBytes {
    type Error: Debug;

    /// Serialize to bytes using bincode
    fn to_bytes(&self) -> Result<Vec<u8>, Self::Error>;
}

/// Trait for types that can be deserialized from bytes
pub trait FromBytes: Sized {
    type Error: Debug;

    /// Deserialize from bytes using bincode
    fn from_bytes(data: &[u8]) -> Result<Self, Self::Error>;
}

/// Implement ToBytes for any type that implements Serialize and bincode::Encode
impl<T> ToBytes for T
where
    T: Serialize + bincode::Encode,
{
    type Error = bincode::error::EncodeError;

    fn to_bytes(&self) -> Result<Vec<u8>, Self::Error> {
        bincode::encode_to_vec(self, bincode::config::standard())
    }
}

/// Implement FromBytes for any type that implements Deserialize and bincode::Decode
impl<T> FromBytes for T
where
    T: for<'de> Deserialize<'de> + bincode::Decode<()>,
{
    type Error = bincode::error::DecodeError;

    fn from_bytes(data: &[u8]) -> Result<Self, Self::Error> {
        let (result, _) = bincode::decode_from_slice(data, bincode::config::standard())?;
        Ok(result)
    }
}

/// Utility functions for JSON serialization
pub mod json {
    use serde::{Deserialize, Serialize};
    use serde_json;

    /// Serialize to JSON string
    pub fn to_string<T: Serialize>(value: &T) -> Result<String, serde_json::Error> {
        serde_json::to_string(value)
    }

    /// Serialize to pretty JSON string
    pub fn to_string_pretty<T: Serialize>(value: &T) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(value)
    }

    /// Deserialize from JSON string
    pub fn from_string<T: for<'de> Deserialize<'de>>(s: &str) -> Result<T, serde_json::Error> {
        serde_json::from_str(s)
    }

    /// Serialize to JSON bytes
    pub fn to_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(value)
    }

    /// Deserialize from JSON bytes
    pub fn from_bytes<T: for<'de> Deserialize<'de>>(data: &[u8]) -> Result<T, serde_json::Error> {
        serde_json::from_slice(data)
    }
}

/// Utility functions for binary serialization using bincode
pub mod binary {
    use bincode;
    use serde::{Deserialize, Serialize};

    /// Serialize to binary using bincode
    pub fn to_bytes<T: Serialize + bincode::Encode>(
        value: &T,
    ) -> Result<Vec<u8>, bincode::error::EncodeError> {
        bincode::encode_to_vec(value, bincode::config::standard())
    }

    /// Deserialize from binary using bincode
    pub fn from_bytes<T: for<'de> Deserialize<'de> + bincode::Decode<()>>(
        data: &[u8],
    ) -> Result<T, bincode::error::DecodeError> {
        let (result, _) = bincode::decode_from_slice(data, bincode::config::standard())?;
        Ok(result)
    }

    /// Get the size of a serialized value without actually serializing it
    pub fn serialized_size<T: Serialize + bincode::Encode>(
        value: &T,
    ) -> Result<usize, bincode::error::EncodeError> {
        // Note: bincode 2.0 doesn't have a direct serialized_size function
        // So we serialize and return the length
        let bytes = to_bytes(value)?;
        Ok(bytes.len())
    }
}

/// Compression utilities
pub mod compression {
    use std::io;

    /// Compress data using a simple compression algorithm
    /// Note: This is a placeholder - in a real implementation you'd use
    /// a proper compression library like flate2
    pub fn compress(data: &[u8]) -> io::Result<Vec<u8>> {
        // For now, just return the data as-is
        // In a real implementation, you'd use something like:
        // use flate2::write::GzEncoder;
        // use flate2::Compression;
        Ok(data.to_vec())
    }

    /// Decompress data
    pub fn decompress(data: &[u8]) -> io::Result<Vec<u8>> {
        // For now, just return the data as-is
        // In a real implementation, you'd use something like:
        // use flate2::read::GzDecoder;
        Ok(data.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, PartialEq, bincode::Encode, bincode::Decode)]
    struct TestStruct {
        id: u32,
        name: String,
        values: Vec<i32>,
    }

    #[test]
    fn test_to_bytes_from_bytes() {
        let test_data = TestStruct {
            id: 42,
            name: "test".to_string(),
            values: vec![1, 2, 3, 4, 5],
        };

        let bytes = test_data.to_bytes().unwrap();
        let recovered: TestStruct = TestStruct::from_bytes(&bytes).unwrap();

        assert_eq!(test_data, recovered);
    }

    #[test]
    fn test_json_serialization() {
        let test_data = TestStruct {
            id: 42,
            name: "test".to_string(),
            values: vec![1, 2, 3, 4, 5],
        };

        let json_string = json::to_string(&test_data).unwrap();
        let recovered: TestStruct = json::from_string(&json_string).unwrap();

        assert_eq!(test_data, recovered);
    }

    #[test]
    fn test_binary_serialization() {
        let test_data = TestStruct {
            id: 42,
            name: "test".to_string(),
            values: vec![1, 2, 3, 4, 5],
        };

        let bytes = binary::to_bytes(&test_data).unwrap();
        let recovered: TestStruct = binary::from_bytes(&bytes).unwrap();

        assert_eq!(test_data, recovered);
    }
}
