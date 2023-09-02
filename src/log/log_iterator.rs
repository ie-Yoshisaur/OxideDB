use crate::file::block_id::BlockId;
use crate::file::file_manager::FileManager;
use crate::file::page::Page;
use crate::log::err::LogError;
use std::iter::Iterator;

/// `LogIterator` is an iterator over the log records in a log file.
///
/// It allows for sequential reading of log records stored in blocks.
/// The iterator starts from a given block and reads records until no more are available.
pub struct LogIterator<'a> {
    file_manager: &'a FileManager,
    block: BlockId,
    page: Page,
    current_position: usize,
    boundary: usize,
}

impl<'a> LogIterator<'a> {
    /// Creates a new `LogIterator` starting from a given block.
    ///
    /// # Arguments
    ///
    /// * `file_manager`: Reference to the `FileManager` for file operations.
    /// * `block`: The `BlockId` where the iterator starts.
    ///
    /// # Errors
    ///
    /// Returns a `LogError` if reading the block or page fails.
    pub fn new(file_manager: &'a FileManager, block: BlockId) -> Result<Self, LogError> {
        let mut page = Page::new_from_blocksize(file_manager.get_block_size());
        let mut log_iterator = LogIterator {
            file_manager,
            block: block.clone(),
            page,
            current_position: 0,
            boundary: 0,
        };
        log_iterator
            .move_to_block(block)
            .map_err(|e| LogError::BlockError(e.to_string()))?;
        Ok(log_iterator)
    }

    /// Checks if there are more records to read.
    ///
    /// # Returns
    ///
    /// Returns `true` if more records are available, otherwise `false`.
    pub fn has_next(&self) -> bool {
        self.current_position < self.file_manager.get_block_size()
            || 0 < self.block.get_block_number()
    }

    /// Moves the iterator to a new block.
    ///
    /// # Arguments
    ///
    /// * `block`: The `BlockId` to move to.
    ///
    /// # Errors
    ///
    /// Returns a `LogError` if reading the block or page fails.
    fn move_to_block(&mut self, block: BlockId) -> Result<(), LogError> {
        self.file_manager
            .read(&block, &mut self.page)
            .map_err(|e| LogError::BlockError(e.to_string()))?;
        self.boundary = self
            .page
            .get_int(0)
            .map_err(|e| LogError::PageError(e.to_string()))? as usize;
        self.current_position = self.boundary;
        Ok(())
    }
}

impl<'a> Iterator for LogIterator<'a> {
    type Item = Result<Vec<u8>, LogError>;

    /// Fetches the next log record.
    ///
    /// # Returns
    ///
    /// Returns `Some(Ok(record))` if a record is successfully read,
    /// `Some(Err(e))` if an error occurs, and `None` if no more records are available.
    fn next(&mut self) -> Option<Self::Item> {
        if !self.has_next() {
            return None;
        }

        if self.current_position == self.file_manager.get_block_size() {
            self.block = BlockId::new(
                self.block.get_file_name().to_string(),
                self.block.get_block_number().saturating_sub(1),
            );
            if let Err(e) = self.move_to_block(self.block.clone()) {
                return Some(Err(e));
            }
        }

        let record_result = self.page.get_bytes(self.current_position);
        match record_result {
            Ok(record) => {
                self.current_position += std::mem::size_of::<i32>() + record.len();
                Some(Ok(record))
            }
            Err(e) => Some(Err(LogError::PageError(e.to_string()))),
        }
    }
}
