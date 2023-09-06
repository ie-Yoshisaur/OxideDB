use crate::file::block_id::BlockId;
use crate::file::file_manager::FileManager;
use crate::file::page::Page;
use crate::log::err::LogError;
use std::iter::Iterator;
use std::sync::{Arc, Mutex};

/// `LogIterator` iterates over the log records in a log file.
///
/// This iterator allows for sequential reading of log records stored in blocks.
/// The iterator starts from a given block and reads records until no more records are available.
pub struct LogIterator {
    file_manager: Arc<Mutex<FileManager>>,
    block: BlockId,
    page: Page,
    current_position: usize,
    boundary: usize,
}

impl LogIterator {
    /// Creates a new `LogIterator` starting from a given block.
    ///
    /// # Arguments
    ///
    /// * `file_manager`: An `Arc<Mutex<FileManager>>` for performing file operations.
    /// * `block`: The `BlockId` from which the iterator will start.
    ///
    /// # Returns
    ///
    /// Returns a `Result` wrapping the created `LogIterator` instance.
    ///
    /// # Errors
    ///
    /// Returns a `LogError` if there is an error in reading the block or the page.
    pub fn new(file_manager: Arc<Mutex<FileManager>>, block: BlockId) -> Result<Self, LogError> {
        let mut page = Page::new_from_blocksize(file_manager.lock().unwrap().get_block_size());
        let mut log_iterator = LogIterator {
            file_manager,
            block: block.clone(),
            page,
            current_position: 0,
            boundary: 0,
        };
        log_iterator.move_to_block(block)?;
        Ok(log_iterator)
    }

    /// Checks if there are more records to read.
    ///
    /// # Returns
    ///
    /// Returns `true` if more records are available, otherwise `false`.
    pub fn has_next(&self) -> bool {
        self.current_position < self.file_manager.lock().unwrap().get_block_size()
            || 0 < self.block.get_block_number()
    }

    /// Moves the iterator to a new block.
    ///
    /// # Arguments
    ///
    /// * `block`: The `BlockId` to move to.
    ///
    /// # Returns
    ///
    /// Returns a `Result` that is `Ok(())` if the move was successful.
    ///
    /// # Errors
    ///
    /// Returns a `LogError` if there is an error in reading the block or the page.
    fn move_to_block(&mut self, block: BlockId) -> Result<(), LogError> {
        // Acquire the lock and read the block, converting errors to LogError
        self.file_manager
            .lock()
            .map_err(|_| LogError::MutexLockError)?
            .read(&block, &mut self.page)
            .map_err(|e| LogError::FileManagerError(e.to_string()))?;

        // Get the boundary, converting errors to LogError
        self.boundary = self
            .page
            .get_int(0)
            .map_err(|e| LogError::PageError(e.to_string()))? as usize;

        self.current_position = self.boundary;
        Ok(())
    }
}

impl Iterator for LogIterator {
    type Item = Result<Vec<u8>, LogError>;

    /// Fetches the next log record.
    ///
    /// # Returns
    ///
    /// Returns an `Option` wrapping a `Result` that contains the log record or an error.
    fn next(&mut self) -> Option<Self::Item> {
        if !self.has_next() {
            return None;
        }

        if self.current_position == self.file_manager.lock().unwrap().get_block_size() {
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
