use crate::file::err::PageError;
use std::io::{Cursor, Read, Write};
use std::mem::size_of;
use std::str;

/// Size of i32 in bytes.
const I32_SIZE: usize = size_of::<i32>();

/// Represents a page within a block.
///
/// A `Page` is essentially a wrapper around a byte buffer, providing methods to
/// read and write different types of data at specific offsets.
pub struct Page {
    byte_buffer: Cursor<Vec<u8>>,
}

impl Page {
    /// Creates a new `Page` with a given block size.
    ///
    /// # Arguments
    ///
    /// * `blocksize` - The size of the block in bytes.
    pub fn new_from_blocksize(blocksize: usize) -> Page {
        Page {
            byte_buffer: Cursor::new(vec![0; blocksize]),
        }
    }

    /// Creates a new `Page` from an existing byte buffer.
    ///
    /// # Arguments
    ///
    /// * `bytes` - The byte buffer to initialize the page with.
    pub fn new_from_bytes(bytes: Vec<u8>) -> Page {
        Page {
            byte_buffer: Cursor::new(bytes),
        }
    }

    /// Reads a 32-bit integer from the given offset within the page.
    ///
    /// # Arguments
    ///
    /// * `offset` - The byte offset to read the integer from.
    ///
    /// # Errors
    ///
    /// Returns an `Err` if the read operation fails.
    pub fn get_int(&mut self, offset: usize) -> Result<i32, PageError> {
        self.byte_buffer.set_position(offset as u64);
        let mut bytes = [0; I32_SIZE];
        self.byte_buffer
            .read_exact(&mut bytes)
            .map_err(PageError::IoError)?;
        Ok(i32::from_le_bytes(bytes))
    }

    /// Writes a 32-bit integer to the given offset within the page.
    ///
    /// # Arguments
    ///
    /// * `offset` - The byte offset to write the integer to.
    /// * `value` - The integer value to write.
    ///
    /// # Errors
    ///
    /// Returns an `Err` if the write operation fails.
    pub fn set_int(&mut self, offset: usize, value: i32) -> Result<(), PageError> {
        self.byte_buffer.set_position(offset as u64);
        self.byte_buffer
            .write_all(&value.to_le_bytes())
            .map_err(PageError::IoError)
    }

    /// Reads a byte array from the given offset within the page.
    ///
    /// # Arguments
    ///
    /// * `offset` - The byte offset to read the byte array from.
    ///
    /// # Errors
    ///
    /// Returns an `Err` if the read operation fails.
    pub fn get_bytes(&mut self, offset: usize) -> Result<Vec<u8>, PageError> {
        let length = self.get_int(offset)? as usize;
        let mut bytes = vec![0; length];
        self.byte_buffer
            .read_exact(&mut bytes)
            .map_err(PageError::IoError)?;
        Ok(bytes)
    }

    /// Writes a byte array to the given offset within the page.
    ///
    /// # Arguments
    ///
    /// * `offset` - The byte offset to write the byte array to.
    /// * `bytes` - The byte array to write.
    ///
    /// # Errors
    ///
    /// Returns an `Err` if the write operation fails.
    pub fn set_bytes(&mut self, offset: usize, bytes: &[u8]) -> Result<(), PageError> {
        self.byte_buffer.set_position(offset as u64);
        let length = bytes.len() as i32;
        self.set_int(offset, length)?;
        self.byte_buffer
            .write_all(bytes)
            .map_err(PageError::IoError)
    }

    /// Reads a string from the given offset within the page.
    ///
    /// # Arguments
    ///
    /// * `offset` - The byte offset to read the string from.
    ///
    /// # Errors
    ///
    /// Returns an `Err` if the read operation fails or the bytes are not valid UTF-8.
    pub fn get_string(&mut self, offset: usize) -> Result<String, PageError> {
        let bytes = self.get_bytes(offset)?; // `?`を使ってエラーを伝播
        String::from_utf8(bytes).map_err(PageError::Utf8Error)
    }

    /// Writes a string to the given offset within the page.
    ///
    /// # Arguments
    ///
    /// * `offset` - The byte offset to write the string to.
    /// * `value` - The string to write.
    ///
    /// # Errors
    ///
    /// Returns an `Err` if the write operation fails.
    pub fn set_string(&mut self, offset: usize, value: &str) -> Result<(), PageError> {
        let bytes = value.as_bytes();
        self.set_bytes(offset, bytes)
    }

    /// Calculates the maximum byte length for a string of a given character length.
    ///
    /// # Arguments
    ///
    /// * `strlen` - The length of the string in characters.
    ///
    /// # Returns
    ///
    /// Returns the maximum byte length that the string will occupy.
    pub fn max_length(strlen: usize) -> usize {
        let u32_bytes = size_of::<u32>();
        u32_bytes + (strlen * size_of::<u8>())
    }

    /// Reads a byte slice from the given offset within the page.
    ///
    /// # Arguments
    ///
    /// * `offset` - The byte offset to read from.
    /// * `len` - The length of the byte slice to read.
    ///
    /// # Errors
    ///
    /// Returns an `Err` if the read operation fails.
    pub fn read_bytes(&mut self, offset: usize, len: usize) -> Result<Vec<u8>, PageError> {
        self.byte_buffer.set_position(offset as u64);
        let mut bytes = vec![0; len];
        self.byte_buffer
            .read_exact(&mut bytes)
            .map_err(PageError::IoError)?;
        Ok(bytes)
    }

    /// Writes a byte slice to the given offset within the page.
    ///
    /// # Arguments
    ///
    /// * `offset` - The byte offset to write to.
    /// * `bytes` - The byte slice to write.
    ///
    /// # Errors
    ///
    /// Returns an `Err` if the write operation fails.
    pub fn write_bytes(&mut self, offset: usize, bytes: &[u8]) -> Result<(), PageError> {
        self.byte_buffer.set_position(offset as u64);
        self.byte_buffer
            .write_all(bytes)
            .map_err(PageError::IoError)
    }
}
