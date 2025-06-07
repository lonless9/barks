//! Time utilities for Barks
//!
//! This module provides common time-related utility functions
//! used across different Barks modules.

use std::time::{SystemTime, UNIX_EPOCH};

/// Get current timestamp in seconds since UNIX_EPOCH
/// 
/// This function provides a consistent way to get timestamps
/// across the distributed system for logging, metrics, and
/// task tracking purposes.
/// 
/// # Returns
/// 
/// Returns the number of seconds since the Unix epoch (January 1, 1970 UTC)
/// as a u64. If the system time is before the Unix epoch, this function
/// will panic (which should never happen in practice).
/// 
/// # Examples
/// 
/// ```
/// use barks_utils::current_timestamp_secs;
/// 
/// let timestamp = current_timestamp_secs();
/// println!("Current timestamp: {}", timestamp);
/// ```
pub fn current_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// Get current timestamp in milliseconds since UNIX_EPOCH
/// 
/// This function provides higher precision timestamps for
/// performance measurements and fine-grained timing.
/// 
/// # Returns
/// 
/// Returns the number of milliseconds since the Unix epoch
/// as a u64.
/// 
/// # Examples
/// 
/// ```
/// use barks_utils::current_timestamp_millis;
/// 
/// let timestamp = current_timestamp_millis();
/// println!("Current timestamp (ms): {}", timestamp);
/// ```
pub fn current_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_current_timestamp_secs() {
        let timestamp1 = current_timestamp_secs();
        thread::sleep(Duration::from_millis(10));
        let timestamp2 = current_timestamp_secs();
        
        // Should be at least the same, possibly different by 1 second
        assert!(timestamp2 >= timestamp1);
        assert!(timestamp2 - timestamp1 <= 1);
    }

    #[test]
    fn test_current_timestamp_millis() {
        let timestamp1 = current_timestamp_millis();
        thread::sleep(Duration::from_millis(10));
        let timestamp2 = current_timestamp_millis();
        
        // Should be different and at least 10ms apart
        assert!(timestamp2 > timestamp1);
        assert!(timestamp2 - timestamp1 >= 10);
    }

    #[test]
    fn test_timestamp_consistency() {
        // Test that seconds and millis are consistent
        let secs = current_timestamp_secs();
        let millis = current_timestamp_millis();
        
        // Convert seconds to millis and check they're close
        let secs_as_millis = secs * 1000;
        let diff = if millis > secs_as_millis {
            millis - secs_as_millis
        } else {
            secs_as_millis - millis
        };
        
        // Should be within 1 second (1000ms)
        assert!(diff < 1000);
    }
}
