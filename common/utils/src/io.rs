//! I/O utilities for Barks
//!
//! This module provides utility functions for I/O operations.

use std::fs;
use std::io::{self, Read, Write};
use std::path::Path;

/// Read a file to a string with error handling
pub fn read_file_to_string<P: AsRef<Path>>(path: P) -> io::Result<String> {
    fs::read_to_string(path)
}

/// Write a string to a file with error handling
pub fn write_string_to_file<P: AsRef<Path>>(path: P, content: &str) -> io::Result<()> {
    fs::write(path, content)
}

/// Read bytes from a file
pub fn read_file_to_bytes<P: AsRef<Path>>(path: P) -> io::Result<Vec<u8>> {
    fs::read(path)
}

/// Write bytes to a file
pub fn write_bytes_to_file<P: AsRef<Path>>(path: P, content: &[u8]) -> io::Result<()> {
    fs::write(path, content)
}

/// Create a directory if it doesn't exist
pub fn ensure_dir_exists<P: AsRef<Path>>(path: P) -> io::Result<()> {
    let path = path.as_ref();
    if !path.exists() {
        fs::create_dir_all(path)?;
    }
    Ok(())
}

/// Check if a path exists
pub fn path_exists<P: AsRef<Path>>(path: P) -> bool {
    path.as_ref().exists()
}

/// Get file size in bytes
pub fn file_size<P: AsRef<Path>>(path: P) -> io::Result<u64> {
    let metadata = fs::metadata(path)?;
    Ok(metadata.len())
}

/// Copy a file from source to destination
pub fn copy_file<P: AsRef<Path>>(from: P, to: P) -> io::Result<u64> {
    fs::copy(from, to)
}

/// Remove a file if it exists
pub fn remove_file_if_exists<P: AsRef<Path>>(path: P) -> io::Result<()> {
    let path = path.as_ref();
    if path.exists() {
        fs::remove_file(path)?;
    }
    Ok(())
}

/// Remove a directory and all its contents if it exists
pub fn remove_dir_if_exists<P: AsRef<Path>>(path: P) -> io::Result<()> {
    let path = path.as_ref();
    if path.exists() {
        fs::remove_dir_all(path)?;
    }
    Ok(())
}

/// Buffered reader wrapper for convenient reading
pub struct BufferedReader<R: Read> {
    inner: R,
    buffer: Vec<u8>,
}

impl<R: Read> BufferedReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            inner: reader,
            buffer: Vec::new(),
        }
    }

    pub fn read_chunk(&mut self, size: usize) -> io::Result<Vec<u8>> {
        let mut chunk = vec![0; size];
        let bytes_read = self.inner.read(&mut chunk)?;
        chunk.truncate(bytes_read);
        Ok(chunk)
    }

    pub fn read_all(&mut self) -> io::Result<Vec<u8>> {
        self.inner.read_to_end(&mut self.buffer)?;
        Ok(self.buffer.clone())
    }
}

/// Buffered writer wrapper for convenient writing
pub struct BufferedWriter<W: Write> {
    inner: W,
    buffer: Vec<u8>,
}

impl<W: Write> BufferedWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            inner: writer,
            buffer: Vec::new(),
        }
    }

    pub fn write_chunk(&mut self, data: &[u8]) -> io::Result<()> {
        self.buffer.extend_from_slice(data);
        Ok(())
    }

    pub fn flush_buffer(&mut self) -> io::Result<()> {
        self.inner.write_all(&self.buffer)?;
        self.inner.flush()?;
        self.buffer.clear();
        Ok(())
    }
}

impl<W: Write> Drop for BufferedWriter<W> {
    fn drop(&mut self) {
        let _ = self.flush_buffer();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use tempfile::tempdir;

    #[test]
    fn test_buffered_reader() {
        let data = b"Hello, World!";
        let cursor = Cursor::new(data);
        let mut reader = BufferedReader::new(cursor);

        let chunk = reader.read_chunk(5).unwrap();
        assert_eq!(chunk, b"Hello");
    }

    #[test]
    fn test_buffered_writer() {
        let mut buffer = Vec::new();
        {
            let mut writer = BufferedWriter::new(&mut buffer);
            writer.write_chunk(b"Hello, ").unwrap();
            writer.write_chunk(b"World!").unwrap();
            writer.flush_buffer().unwrap();
        }
        assert_eq!(buffer, b"Hello, World!");
    }

    #[test]
    fn test_file_utils() {
        let dir = tempdir().unwrap();
        let path = dir.path();

        // --- Test ensure_dir_exists and path_exists ---
        let sub_dir = path.join("sub");
        assert!(!path_exists(&sub_dir));
        ensure_dir_exists(&sub_dir).unwrap();
        assert!(path_exists(&sub_dir));

        // --- Test write and read string ---
        let file_path = path.join("test.txt");
        let content = "Hello, barks!";
        write_string_to_file(&file_path, content).unwrap();
        let read_content = read_file_to_string(&file_path).unwrap();
        assert_eq!(read_content, content);

        // --- Test write and read bytes ---
        let bytes_path = path.join("test.bin");
        let bytes_content = vec![1, 2, 3, 4, 5];
        write_bytes_to_file(&bytes_path, &bytes_content).unwrap();
        let read_bytes = read_file_to_bytes(&bytes_path).unwrap();
        assert_eq!(read_bytes, bytes_content);

        // --- Test file_size ---
        let size = file_size(&bytes_path).unwrap();
        assert_eq!(size, 5);

        // --- Test copy_file ---
        let copy_path = path.join("test.bin.copy");
        let copied_bytes = copy_file(&bytes_path, &copy_path).unwrap();
        assert_eq!(copied_bytes, 5);
        assert!(path_exists(&copy_path));
        let copied_content = read_file_to_bytes(&copy_path).unwrap();
        assert_eq!(copied_content, bytes_content);

        // --- Test remove_file_if_exists ---
        remove_file_if_exists(&file_path).unwrap();
        assert!(!path_exists(&file_path));
        // Removing non-existent file should be ok
        remove_file_if_exists(&file_path).unwrap();

        // --- Test remove_dir_if_exists ---
        let dir_to_remove = path.join("to_remove");
        ensure_dir_exists(&dir_to_remove).unwrap();
        write_string_to_file(dir_to_remove.join("file.txt"), "content").unwrap();
        assert!(path_exists(&dir_to_remove));
        remove_dir_if_exists(&dir_to_remove).unwrap();
        assert!(!path_exists(&dir_to_remove));
        // Removing non-existent dir should be ok
        remove_dir_if_exists(&dir_to_remove).unwrap();
    }
}
