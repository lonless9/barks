//! Compression abstraction using trait-based design.
//!
//! This module provides a generic compression interface that abstracts over
//! different compression algorithms (zstd, lz4, snappy).

use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::{CommonError, Result};

/// Compression algorithm types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionAlgorithm {
    /// Zstd compression - high performance with excellent compression ratio.
    Zstd,
    /// LZ4 compression - extremely fast compression and decompression.
    Lz4,
    /// Snappy compression - balanced compression ratio and speed.
    Snappy,
}

impl CompressionAlgorithm {
    /// Get the default compression level for this algorithm.
    pub fn default_level(&self) -> i32 {
        match self {
            CompressionAlgorithm::Zstd => 3,   // Zstd default level
            CompressionAlgorithm::Lz4 => 0,    // LZ4 has no levels
            CompressionAlgorithm::Snappy => 0, // Snappy has no levels
        }
    }

    /// Check if this algorithm supports compression levels.
    pub fn supports_levels(&self) -> bool {
        matches!(self, CompressionAlgorithm::Zstd)
    }
}

/// Generic compressor interface.
///
/// This provides a unified interface for compression operations
/// without exposing the underlying implementation details.
pub trait Compressor: Debug + Send + Sync {
    /// Compress the input data and return compressed bytes.
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>>;

    /// Decompress the input data and return original bytes.
    fn decompress(&self, compressed_data: &[u8]) -> Result<Vec<u8>>;

    /// Get the compression algorithm used by this compressor.
    fn algorithm(&self) -> CompressionAlgorithm;

    /// Get compression statistics if available.
    fn stats(&self) -> CompressionStats;

    /// Reset statistics counters.
    fn reset_stats(&self);
}

/// Compression statistics information.
#[derive(Debug, Clone, Default)]
pub struct CompressionStats {
    pub total_compressions: u64,
    pub total_decompressions: u64,
    pub total_input_bytes: u64,
    pub total_compressed_bytes: u64,
    pub total_decompressed_bytes: u64,
}

impl CompressionStats {
    /// Calculate the average compression ratio.
    pub fn compression_ratio(&self) -> f64 {
        if self.total_input_bytes == 0 {
            0.0
        } else {
            self.total_compressed_bytes as f64 / self.total_input_bytes as f64
        }
    }

    /// Calculate the space savings percentage.
    pub fn space_savings(&self) -> f64 {
        let ratio = self.compression_ratio();
        if ratio == 0.0 {
            0.0
        } else {
            (1.0 - ratio) * 100.0
        }
    }
}

/// Configuration for compressor creation.
#[derive(Debug, Clone)]
pub struct CompressionConfig {
    /// The compression algorithm to use.
    pub algorithm: CompressionAlgorithm,
    /// Compression level (only used for algorithms that support it).
    pub level: Option<i32>,
    /// Whether to track compression statistics.
    pub track_stats: bool,
}

impl CompressionConfig {
    /// Create a new configuration with the specified algorithm.
    pub fn new(algorithm: CompressionAlgorithm) -> Self {
        Self {
            algorithm,
            level: Some(algorithm.default_level()),
            track_stats: true,
        }
    }

    /// Set the compression level.
    pub fn with_level(mut self, level: i32) -> Self {
        self.level = Some(level);
        self
    }

    /// Enable or disable statistics tracking.
    pub fn with_stats_tracking(mut self, track: bool) -> Self {
        self.track_stats = track;
        self
    }
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self::new(CompressionAlgorithm::Zstd)
    }
}

/// Builder for creating compressor instances.
pub struct CompressionBuilder {
    config: CompressionConfig,
}

impl CompressionBuilder {
    /// Create a new compression builder with default configuration (Zstd).
    pub fn new() -> Self {
        Self {
            config: CompressionConfig::default(),
        }
    }

    /// Create a new compression builder with the specified algorithm.
    pub fn with_algorithm(algorithm: CompressionAlgorithm) -> Self {
        Self {
            config: CompressionConfig::new(algorithm),
        }
    }

    /// Set the compression level.
    pub fn level(mut self, level: i32) -> Self {
        self.config.level = Some(level);
        self
    }

    /// Enable or disable statistics tracking.
    pub fn track_stats(mut self, track: bool) -> Self {
        self.config.track_stats = track;
        self
    }

    /// Build a compressor instance.
    pub fn build(self) -> Result<Box<dyn Compressor>> {
        match self.config.algorithm {
            CompressionAlgorithm::Zstd => {
                let compressor = ZstdCompressor::new(self.config)?;
                Ok(Box::new(compressor))
            }
            CompressionAlgorithm::Lz4 => {
                let compressor = Lz4Compressor::new(self.config)?;
                Ok(Box::new(compressor))
            }
            CompressionAlgorithm::Snappy => {
                let compressor = SnappyCompressor::new(self.config)?;
                Ok(Box::new(compressor))
            }
        }
    }
}

impl Default for CompressionBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Internal statistics tracker for compression operations.
#[derive(Debug, Default)]
struct InternalCompressionStats {
    compressions: AtomicU64,
    decompressions: AtomicU64,
    input_bytes: AtomicU64,
    compressed_bytes: AtomicU64,
    decompressed_bytes: AtomicU64,
}

impl InternalCompressionStats {
    fn record_compression(&self, input_size: usize, compressed_size: usize) {
        self.compressions.fetch_add(1, Ordering::AcqRel);
        self.input_bytes
            .fetch_add(input_size as u64, Ordering::AcqRel);
        self.compressed_bytes
            .fetch_add(compressed_size as u64, Ordering::AcqRel);
    }

    fn record_decompression(&self, decompressed_size: usize) {
        self.decompressions.fetch_add(1, Ordering::AcqRel);
        self.decompressed_bytes
            .fetch_add(decompressed_size as u64, Ordering::AcqRel);
    }

    fn reset(&self) {
        self.compressions.store(0, Ordering::Release);
        self.decompressions.store(0, Ordering::Release);
        self.input_bytes.store(0, Ordering::Release);
        self.compressed_bytes.store(0, Ordering::Release);
        self.decompressed_bytes.store(0, Ordering::Release);
    }

    fn get_stats(&self) -> CompressionStats {
        CompressionStats {
            total_compressions: self.compressions.load(Ordering::Acquire),
            total_decompressions: self.decompressions.load(Ordering::Acquire),
            total_input_bytes: self.input_bytes.load(Ordering::Acquire),
            total_compressed_bytes: self.compressed_bytes.load(Ordering::Acquire),
            total_decompressed_bytes: self.decompressed_bytes.load(Ordering::Acquire),
        }
    }
}

/// Zstd compressor implementation.
#[derive(Debug)]
struct ZstdCompressor {
    level: i32,
    stats: Option<Arc<InternalCompressionStats>>,
}

impl ZstdCompressor {
    fn new(config: CompressionConfig) -> Result<Self> {
        let level = config
            .level
            .unwrap_or(CompressionAlgorithm::Zstd.default_level());

        // Validate compression level for zstd (typically 1-22)
        if !(1..=22).contains(&level) {
            return Err(CommonError::configuration_error(format!(
                "Invalid zstd compression level: {}. Must be between 1 and 22",
                level
            )));
        }

        let stats = if config.track_stats {
            Some(Arc::new(InternalCompressionStats::default()))
        } else {
            None
        };

        Ok(Self { level, stats })
    }
}

impl Compressor for ZstdCompressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        let compressed = zstd::encode_all(data, self.level).map_err(|e| {
            CommonError::compression_error(format!("Zstd compression failed: {}", e))
        })?;

        if let Some(ref stats) = self.stats {
            stats.record_compression(data.len(), compressed.len());
        }

        Ok(compressed)
    }

    fn decompress(&self, compressed_data: &[u8]) -> Result<Vec<u8>> {
        let decompressed = zstd::decode_all(compressed_data).map_err(|e| {
            CommonError::decompression_error(format!("Zstd decompression failed: {}", e))
        })?;

        if let Some(ref stats) = self.stats {
            stats.record_decompression(decompressed.len());
        }

        Ok(decompressed)
    }

    fn algorithm(&self) -> CompressionAlgorithm {
        CompressionAlgorithm::Zstd
    }

    fn stats(&self) -> CompressionStats {
        if let Some(ref stats) = self.stats {
            stats.get_stats()
        } else {
            CompressionStats::default()
        }
    }

    fn reset_stats(&self) {
        if let Some(ref stats) = self.stats {
            stats.reset();
        }
    }
}

/// LZ4 compressor implementation.
#[derive(Debug)]
struct Lz4Compressor {
    stats: Option<Arc<InternalCompressionStats>>,
}

impl Lz4Compressor {
    fn new(config: CompressionConfig) -> Result<Self> {
        let stats = if config.track_stats {
            Some(Arc::new(InternalCompressionStats::default()))
        } else {
            None
        };

        Ok(Self { stats })
    }
}

impl Compressor for Lz4Compressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        let compressed = lz4_flex::compress_prepend_size(data);

        if let Some(ref stats) = self.stats {
            stats.record_compression(data.len(), compressed.len());
        }

        Ok(compressed)
    }

    fn decompress(&self, compressed_data: &[u8]) -> Result<Vec<u8>> {
        let decompressed = lz4_flex::decompress_size_prepended(compressed_data).map_err(|e| {
            CommonError::decompression_error(format!("LZ4 decompression failed: {}", e))
        })?;

        if let Some(ref stats) = self.stats {
            stats.record_decompression(decompressed.len());
        }

        Ok(decompressed)
    }

    fn algorithm(&self) -> CompressionAlgorithm {
        CompressionAlgorithm::Lz4
    }

    fn stats(&self) -> CompressionStats {
        if let Some(ref stats) = self.stats {
            stats.get_stats()
        } else {
            CompressionStats::default()
        }
    }

    fn reset_stats(&self) {
        if let Some(ref stats) = self.stats {
            stats.reset();
        }
    }
}

/// Snappy compressor implementation.
#[derive(Debug)]
struct SnappyCompressor {
    stats: Option<Arc<InternalCompressionStats>>,
}

impl SnappyCompressor {
    fn new(config: CompressionConfig) -> Result<Self> {
        let stats = if config.track_stats {
            Some(Arc::new(InternalCompressionStats::default()))
        } else {
            None
        };

        Ok(Self { stats })
    }
}

impl Compressor for SnappyCompressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut compressed = Vec::new();
        let mut encoder = snap::write::FrameEncoder::new(&mut compressed);

        std::io::copy(&mut std::io::Cursor::new(data), &mut encoder).map_err(|e| {
            CommonError::compression_error(format!("Snappy compression failed: {}", e))
        })?;

        encoder.into_inner().map_err(|e| {
            CommonError::compression_error(format!("Snappy compression finalization failed: {}", e))
        })?;

        if let Some(ref stats) = self.stats {
            stats.record_compression(data.len(), compressed.len());
        }

        Ok(compressed)
    }

    fn decompress(&self, compressed_data: &[u8]) -> Result<Vec<u8>> {
        let mut decompressed = Vec::new();
        let mut decoder = snap::read::FrameDecoder::new(compressed_data);

        std::io::copy(&mut decoder, &mut decompressed).map_err(|e| {
            CommonError::decompression_error(format!("Snappy decompression failed: {}", e))
        })?;

        if let Some(ref stats) = self.stats {
            stats.record_decompression(decompressed.len());
        }

        Ok(decompressed)
    }

    fn algorithm(&self) -> CompressionAlgorithm {
        CompressionAlgorithm::Snappy
    }

    fn stats(&self) -> CompressionStats {
        if let Some(ref stats) = self.stats {
            stats.get_stats()
        } else {
            CompressionStats::default()
        }
    }

    fn reset_stats(&self) {
        if let Some(ref stats) = self.stats {
            stats.reset();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_DATA: &[u8] = b"Hello, World! This is a test string for compression. It should be long enough to see some compression benefits. Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.";

    #[test]
    fn test_zstd_compression() {
        let compressor = CompressionBuilder::with_algorithm(CompressionAlgorithm::Zstd)
            .level(3)
            .build()
            .expect("Failed to create Zstd compressor");

        // Test compression
        let compressed = compressor.compress(TEST_DATA).expect("Compression failed");
        assert!(!compressed.is_empty());
        assert!(compressed.len() < TEST_DATA.len()); // Should be smaller

        // Test decompression
        let decompressed = compressor
            .decompress(&compressed)
            .expect("Decompression failed");
        assert_eq!(decompressed, TEST_DATA);

        // Test algorithm
        assert_eq!(compressor.algorithm(), CompressionAlgorithm::Zstd);
    }

    #[test]
    fn test_lz4_compression() {
        let compressor = CompressionBuilder::with_algorithm(CompressionAlgorithm::Lz4)
            .build()
            .expect("Failed to create LZ4 compressor");

        // Test compression
        let compressed = compressor.compress(TEST_DATA).expect("Compression failed");
        assert!(!compressed.is_empty());

        // Test decompression
        let decompressed = compressor
            .decompress(&compressed)
            .expect("Decompression failed");
        assert_eq!(decompressed, TEST_DATA);

        // Test algorithm
        assert_eq!(compressor.algorithm(), CompressionAlgorithm::Lz4);
    }

    #[test]
    fn test_snappy_compression() {
        let compressor = CompressionBuilder::with_algorithm(CompressionAlgorithm::Snappy)
            .build()
            .expect("Failed to create Snappy compressor");

        // Test compression
        let compressed = compressor.compress(TEST_DATA).expect("Compression failed");
        assert!(!compressed.is_empty());

        // Test decompression
        let decompressed = compressor
            .decompress(&compressed)
            .expect("Decompression failed");
        assert_eq!(decompressed, TEST_DATA);

        // Test algorithm
        assert_eq!(compressor.algorithm(), CompressionAlgorithm::Snappy);
    }

    #[test]
    fn test_compression_statistics() {
        let compressor = CompressionBuilder::with_algorithm(CompressionAlgorithm::Zstd)
            .track_stats(true)
            .build()
            .expect("Failed to create compressor");

        // Initial stats should be zero
        let initial_stats = compressor.stats();
        assert_eq!(initial_stats.total_compressions, 0);
        assert_eq!(initial_stats.total_decompressions, 0);

        // Perform compression
        let compressed = compressor.compress(TEST_DATA).expect("Compression failed");
        let stats_after_compression = compressor.stats();
        assert_eq!(stats_after_compression.total_compressions, 1);
        assert_eq!(
            stats_after_compression.total_input_bytes,
            TEST_DATA.len() as u64
        );
        assert_eq!(
            stats_after_compression.total_compressed_bytes,
            compressed.len() as u64
        );

        // Perform decompression
        let _decompressed = compressor
            .decompress(&compressed)
            .expect("Decompression failed");
        let stats_after_decompression = compressor.stats();
        assert_eq!(stats_after_decompression.total_decompressions, 1);
        assert_eq!(
            stats_after_decompression.total_decompressed_bytes,
            TEST_DATA.len() as u64
        );

        // Test compression ratio calculation
        let ratio = stats_after_decompression.compression_ratio();
        assert!(ratio > 0.0 && ratio < 1.0); // Should be compressed

        let savings = stats_after_decompression.space_savings();
        assert!(savings > 0.0 && savings < 100.0);
    }

    #[test]
    fn test_compression_without_stats() {
        let compressor = CompressionBuilder::with_algorithm(CompressionAlgorithm::Zstd)
            .track_stats(false)
            .build()
            .expect("Failed to create compressor");

        let _compressed = compressor.compress(TEST_DATA).expect("Compression failed");

        // Stats should be default (zeros) when tracking is disabled
        let stats = compressor.stats();
        assert_eq!(stats.total_compressions, 0);
        assert_eq!(stats.total_decompressions, 0);
    }

    #[test]
    fn test_compression_algorithm_properties() {
        // Test default levels
        assert_eq!(CompressionAlgorithm::Zstd.default_level(), 3);
        assert_eq!(CompressionAlgorithm::Lz4.default_level(), 0);
        assert_eq!(CompressionAlgorithm::Snappy.default_level(), 0);

        // Test level support
        assert!(CompressionAlgorithm::Zstd.supports_levels());
        assert!(!CompressionAlgorithm::Lz4.supports_levels());
        assert!(!CompressionAlgorithm::Snappy.supports_levels());
    }

    #[test]
    fn test_invalid_zstd_level() {
        let result = CompressionBuilder::with_algorithm(CompressionAlgorithm::Zstd)
            .level(100) // Invalid level
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn test_stats_reset() {
        let compressor = CompressionBuilder::with_algorithm(CompressionAlgorithm::Zstd)
            .track_stats(true)
            .build()
            .expect("Failed to create compressor");

        // Perform some operations
        let _compressed = compressor.compress(TEST_DATA).expect("Compression failed");
        let stats_before_reset = compressor.stats();
        assert!(stats_before_reset.total_compressions > 0);

        // Reset stats
        compressor.reset_stats();
        let stats_after_reset = compressor.stats();
        assert_eq!(stats_after_reset.total_compressions, 0);
        assert_eq!(stats_after_reset.total_decompressions, 0);
    }

    #[test]
    fn test_empty_data_compression() {
        let compressor = CompressionBuilder::with_algorithm(CompressionAlgorithm::Zstd)
            .build()
            .expect("Failed to create compressor");

        let empty_data = b"";
        let compressed = compressor.compress(empty_data).expect("Compression failed");
        let decompressed = compressor
            .decompress(&compressed)
            .expect("Decompression failed");

        assert_eq!(decompressed, empty_data);
    }

    #[test]
    fn test_compression_config() {
        let config = CompressionConfig::new(CompressionAlgorithm::Zstd)
            .with_level(5)
            .with_stats_tracking(false);

        assert_eq!(config.algorithm, CompressionAlgorithm::Zstd);
        assert_eq!(config.level, Some(5));
        assert!(!config.track_stats);
    }

    #[test]
    fn test_compression_config_default() {
        // Test Default implementation for CompressionConfig
        let config1 = CompressionConfig::default();
        let config2 = CompressionConfig::new(CompressionAlgorithm::Zstd);

        assert_eq!(config1.algorithm, config2.algorithm);
        assert_eq!(config1.algorithm, CompressionAlgorithm::Zstd);
    }

    #[test]
    fn test_compression_builder_default() {
        // Test Default implementation for CompressionBuilder
        let builder1 = CompressionBuilder::default();
        let builder2 = CompressionBuilder::new();

        let compressor1 = builder1
            .build()
            .expect("Failed to create compressor from default");
        let compressor2 = builder2
            .build()
            .expect("Failed to create compressor from new");

        // Both should use the same algorithm
        assert_eq!(compressor1.algorithm(), compressor2.algorithm());
        assert_eq!(compressor1.algorithm(), CompressionAlgorithm::Zstd);
    }

    #[test]
    fn test_compression_builder_new() {
        // Test CompressionBuilder::new() method
        let builder = CompressionBuilder::new();
        let compressor = builder.build().expect("Failed to create compressor");

        // Should default to Zstd
        assert_eq!(compressor.algorithm(), CompressionAlgorithm::Zstd);

        // Test compression works
        let compressed = compressor.compress(TEST_DATA).expect("Compression failed");
        let decompressed = compressor
            .decompress(&compressed)
            .expect("Decompression failed");
        assert_eq!(decompressed, TEST_DATA);
    }

    #[test]
    fn test_compressor_stats_without_tracking() {
        // Test stats() method when tracking is disabled
        let compressor = CompressionBuilder::with_algorithm(CompressionAlgorithm::Zstd)
            .track_stats(false)
            .build()
            .expect("Failed to create compressor");

        // Perform some operations
        let _compressed = compressor.compress(TEST_DATA).expect("Compression failed");

        // Stats should return default values
        let stats = compressor.stats();
        assert_eq!(stats.total_compressions, 0);
        assert_eq!(stats.total_decompressions, 0);
        assert_eq!(stats.total_input_bytes, 0);
        assert_eq!(stats.total_compressed_bytes, 0);
        assert_eq!(stats.total_decompressed_bytes, 0);
    }

    #[test]
    fn test_compressor_reset_stats_without_tracking() {
        // Test reset_stats() method when tracking is disabled
        let compressor = CompressionBuilder::with_algorithm(CompressionAlgorithm::Zstd)
            .track_stats(false)
            .build()
            .expect("Failed to create compressor");

        // This should not panic even when stats tracking is disabled
        compressor.reset_stats();

        let stats = compressor.stats();
        assert_eq!(stats.total_compressions, 0);
    }

    #[test]
    fn test_lz4_compressor_stats() {
        // Test stats() and reset_stats() for LZ4 compressor
        let compressor = CompressionBuilder::with_algorithm(CompressionAlgorithm::Lz4)
            .track_stats(true)
            .build()
            .expect("Failed to create LZ4 compressor");

        // Test stats without tracking
        let stats_initial = compressor.stats();
        assert_eq!(stats_initial.total_compressions, 0);

        // Perform compression
        let compressed = compressor.compress(TEST_DATA).expect("Compression failed");
        let stats_after_compression = compressor.stats();
        assert_eq!(stats_after_compression.total_compressions, 1);

        // Test reset
        compressor.reset_stats();
        let stats_after_reset = compressor.stats();
        assert_eq!(stats_after_reset.total_compressions, 0);

        // Test decompression
        let _decompressed = compressor
            .decompress(&compressed)
            .expect("Decompression failed");
        let stats_after_decompression = compressor.stats();
        assert_eq!(stats_after_decompression.total_decompressions, 1);
    }

    #[test]
    fn test_lz4_compressor_stats_without_tracking() {
        // Test LZ4 compressor stats() when tracking is disabled
        let compressor = CompressionBuilder::with_algorithm(CompressionAlgorithm::Lz4)
            .track_stats(false)
            .build()
            .expect("Failed to create LZ4 compressor");

        let _compressed = compressor.compress(TEST_DATA).expect("Compression failed");

        let stats = compressor.stats();
        assert_eq!(stats.total_compressions, 0);

        // Reset should not panic
        compressor.reset_stats();
    }

    #[test]
    fn test_snappy_compressor_stats() {
        // Test stats() and reset_stats() for Snappy compressor
        let compressor = CompressionBuilder::with_algorithm(CompressionAlgorithm::Snappy)
            .track_stats(true)
            .build()
            .expect("Failed to create Snappy compressor");

        let stats_initial = compressor.stats();
        assert_eq!(stats_initial.total_compressions, 0);

        // Perform compression
        let compressed = compressor.compress(TEST_DATA).expect("Compression failed");
        let stats_after_compression = compressor.stats();
        assert_eq!(stats_after_compression.total_compressions, 1);

        // Test reset
        compressor.reset_stats();
        let stats_after_reset = compressor.stats();
        assert_eq!(stats_after_reset.total_compressions, 0);

        // Test decompression
        let _decompressed = compressor
            .decompress(&compressed)
            .expect("Decompression failed");
        let stats_after_decompression = compressor.stats();
        assert_eq!(stats_after_decompression.total_decompressions, 1);
    }

    #[test]
    fn test_snappy_compressor_stats_without_tracking() {
        // Test Snappy compressor stats() when tracking is disabled
        let compressor = CompressionBuilder::with_algorithm(CompressionAlgorithm::Snappy)
            .track_stats(false)
            .build()
            .expect("Failed to create Snappy compressor");

        let _compressed = compressor.compress(TEST_DATA).expect("Compression failed");

        let stats = compressor.stats();
        assert_eq!(stats.total_compressions, 0);

        // Reset should not panic
        compressor.reset_stats();
    }
}
