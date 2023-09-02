use crate::file::block_id::BlockId;
use crate::file::file_manager::FileManager;
use crate::file::page::Page;
use crate::log::err::LogError;
use crate::log::log_iterator::LogIterator;
use std::sync::Arc;
use std::sync::Mutex;

/// `LogManager` manages the writing and reading of log records.
///
/// It keeps track of the current log block, the latest log sequence number (LSN),
/// and the last saved LSN.
pub struct LogManager {
    file_manager: Arc<FileManager>,
    log_file: String,
    log_page: Page,
    current_block: BlockId,
    latest_lsn: Mutex<i32>,
    last_saved_lsn: Mutex<i32>,
}

impl LogManager {
    /// Creates a new `LogManager`.
    ///
    /// # Arguments
    ///
    /// * `file_manager`: An `Arc` wrapped `FileManager` for file operations.
    /// * `log_file`: The name of the log file.
    ///
    /// # Errors
    ///
    /// Returns a `LogError` if initialization fails.
    pub fn new(file_manager: Arc<FileManager>, log_file: String) -> Result<Self, LogError> {
        let mut log_page = Page::new_from_blocksize(file_manager.get_block_size());
        let logsize = file_manager
            .length(&log_file)
            .map_err(|e| LogError::FileManagerError(e.to_string()))?;

        let current_block = if logsize == 0 {
            Self::append_new_block(&file_manager, &log_file, &mut log_page)
                .map_err(|e| LogError::FileManagerError(e.to_string()))?
        } else {
            let block = BlockId::new(log_file.clone(), logsize - 1);
            file_manager
                .read(&block, &mut log_page)
                .map_err(|e| LogError::FileManagerError(e.to_string()))?;
            block
        };

        Ok(LogManager {
            file_manager,
            log_file,
            log_page,
            current_block,
            latest_lsn: Mutex::new(0),
            last_saved_lsn: Mutex::new(0),
        })
    }

    /// Flushes log records up to a given LSN.
    ///
    /// # Arguments
    ///
    /// * `lsn`: The log sequence number up to which records should be flushed.
    ///
    /// # Errors
    ///
    /// Returns a `LogError` if flushing fails.
    pub fn flush_by_lsn(&mut self, lsn: i32) -> Result<(), LogError> {
        let last_saved_lsn = *self
            .last_saved_lsn
            .lock()
            .map_err(|_| LogError::MutexLockError)?;
        if lsn >= last_saved_lsn {
            self.flush()?;
        }
        Ok(())
    }

    /// Returns a `LogIterator` for reading log records.
    ///
    /// # Errors
    ///
    /// Returns a `LogError` if iterator creation fails.
    pub fn iterator(&mut self) -> Result<LogIterator, LogError> {
        self.flush()?;
        Ok(LogIterator::new(
            &self.file_manager,
            self.current_block.clone(),
        )?)
    }

    /// Appends a log record to the log file.
    ///
    /// # Arguments
    ///
    /// * `log_record`: The log record to append.
    ///
    /// # Errors
    ///
    /// Returns a `LogError` if appending fails.
    ///
    /// # Returns
    ///
    /// Returns the LSN of the appended log record.
    pub fn append(&mut self, log_record: &[u8]) -> Result<i32, LogError> {
        let mut boundary = self
            .log_page
            .get_int(0)
            .map_err(|e| LogError::PageError(e.to_string()))? as usize;
        let record_size = log_record.len();
        let bytes_needed = record_size + std::mem::size_of::<i32>();

        if boundary.checked_sub(bytes_needed).unwrap_or(0) < std::mem::size_of::<i32>() {
            self.flush()?;
            self.current_block =
                Self::append_new_block(&self.file_manager, &self.log_file, &mut self.log_page)?;
            boundary = self
                .log_page
                .get_int(0)
                .map_err(|e| LogError::PageError(e.to_string()))? as usize;
        }

        let record_position = boundary - bytes_needed;
        self.log_page
            .set_bytes(record_position, log_record)
            .map_err(|e| LogError::PageError(e.to_string()))?;

        self.log_page
            .set_int(0, record_position as i32)
            .map_err(|e| LogError::PageError(e.to_string()))?;
        let mut latest_lsn = self
            .latest_lsn
            .lock()
            .map_err(|_| LogError::MutexLockError)?;
        *latest_lsn += 1;
        Ok(*latest_lsn)
    }

    /// Appends a new block to the log file.
    ///
    /// # Arguments
    ///
    /// * `file_manager`: Reference to the `FileManager` for file operations.
    /// * `log_file`: The name of the log file.
    /// * `log_page`: The log page to write.
    ///
    /// # Errors
    ///
    /// Returns a `LogError` if appending a new block fails.
    ///
    /// # Returns
    ///
    /// Returns the `BlockId` of the new block.
    fn append_new_block(
        file_manager: &FileManager,
        log_file: &String,
        log_page: &mut Page,
    ) -> Result<BlockId, LogError> {
        let block = file_manager
            .append(log_file)
            .map_err(|e| LogError::FileManagerError(e.to_string()))?;
        log_page
            .set_int(0, file_manager.get_block_size() as i32)
            .map_err(|e| LogError::PageError(e.to_string()))?;
        file_manager
            .write(&block, log_page)
            .map_err(|e| LogError::FileManagerError(e.to_string()))?;
        Ok(block)
    }

    /// Flushes the current log page to disk.
    ///
    /// # Errors
    ///
    /// Returns a `LogError` if flushing fails.
    fn flush(&mut self) -> Result<(), LogError> {
        self.file_manager
            .write(&self.current_block, &mut self.log_page)
            .map_err(|e| LogError::FileManagerError(e.to_string()))?;
        let mut last_saved_lsn = self
            .last_saved_lsn
            .lock()
            .map_err(|_| LogError::MutexLockError)?;
        let latest_lsn = *self
            .latest_lsn
            .lock()
            .map_err(|_| LogError::MutexLockError)?;
        *last_saved_lsn = latest_lsn;
        Ok(())
    }
}
