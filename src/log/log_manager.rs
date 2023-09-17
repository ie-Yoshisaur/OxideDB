use crate::file::block_id::BlockId;
use crate::file::file_manager::FileManager;
use crate::file::page::Page;
use crate::log::err::LogError;
use crate::log::log_iterator::LogIterator;
use std::sync::{Arc, Mutex};

/// `LogManager` manages the writing and reading of log records.
///
/// It keeps track of the current log block, the latest log sequence number (LSN),
/// and the last saved LSN.
pub struct LogManager {
    file_manager: Arc<Mutex<FileManager>>,
    log_file: String,
    log_page: Page,
    current_block: BlockId,
    latest_lsn: Mutex<i32>,
    last_saved_lsn: Mutex<i32>,
}

impl LogManager {
    /// Creates a new `LogManager`.
    pub fn new(file_manager: Arc<Mutex<FileManager>>, log_file: String) -> Result<Self, LogError> {
        let mut log_page = {
            let locked_file_manager = file_manager.lock().unwrap();
            Page::new_from_blocksize(locked_file_manager.get_block_size())
        };

        let logsize = {
            let locked_file_manager = file_manager.lock().unwrap();
            locked_file_manager
                .length(&log_file)
                .map_err(|e| LogError::FileError(e))?
        };

        let current_block = if logsize == 0 {
            let locked_file_manager = file_manager.lock().unwrap();
            Self::append_new_block(&locked_file_manager, &log_file, &mut log_page)?
        } else {
            let locked_file_manager = file_manager.lock().unwrap();
            let block = BlockId::new(log_file.clone(), logsize as i32 - 1);
            locked_file_manager
                .read(&block, &mut log_page)
                .map_err(|e| LogError::FileError(e))?;
            block
        };

        Ok(Self {
            file_manager,
            log_file,
            log_page,
            current_block,
            latest_lsn: Mutex::new(0),
            last_saved_lsn: Mutex::new(0),
        })
    }

    /// Flushes log records up to a given LSN.
    pub fn flush_by_lsn(&mut self, lsn: i32) -> Result<(), LogError> {
        let last_saved_lsn = *self.last_saved_lsn.lock().unwrap();
        if lsn >= last_saved_lsn {
            self.flush()?;
        }
        Ok(())
    }

    /// Returns a `LogIterator` for reading log records.
    pub fn iterator(&mut self) -> Result<LogIterator, LogError> {
        self.flush()?;
        Ok(LogIterator::new(
            self.file_manager.clone(),
            self.current_block.clone(),
        )?)
    }

    /// Appends a log record to the log file.
    pub fn append(&mut self, log_record: &[u8]) -> Result<i32, LogError> {
        let mut boundary = self
            .log_page
            .get_int(0)
            .map_err(|e| LogError::PageError(e))? as usize;
        let record_size = log_record.len();
        let bytes_needed = record_size + std::mem::size_of::<i32>();

        if boundary.checked_sub(bytes_needed).unwrap_or(0) < std::mem::size_of::<i32>() {
            self.flush()?;
            self.current_block = Self::append_new_block(
                &self.file_manager.lock().unwrap(),
                &self.log_file,
                &mut self.log_page,
            )?;
            boundary = self
                .log_page
                .get_int(0)
                .map_err(|e| LogError::PageError(e))? as usize;
        }

        let record_position = boundary - bytes_needed;
        self.log_page
            .set_bytes(record_position, log_record)
            .map_err(|e| LogError::PageError(e))?;
        self.log_page
            .set_int(0, record_position as i32)
            .map_err(|e| LogError::PageError(e))?;
        let mut latest_lsn = self.latest_lsn.lock().unwrap();
        *latest_lsn += 1;
        Ok(*latest_lsn)
    }

    /// Appends a new block to the log file.
    fn append_new_block(
        file_manager: &FileManager,
        log_file: &String,
        log_page: &mut Page,
    ) -> Result<BlockId, LogError> {
        let block = file_manager
            .append(log_file)
            .map_err(|e| LogError::FileError(e))?;
        log_page
            .set_int(0, file_manager.get_block_size() as i32)
            .map_err(|e| LogError::PageError(e))?;
        file_manager
            .write(&block, log_page)
            .map_err(|e| LogError::FileError(e))?;
        Ok(block)
    }

    /// Flushes the current log page to disk.
    fn flush(&mut self) -> Result<(), LogError> {
        self.file_manager
            .lock()
            .unwrap()
            .write(&self.current_block, &mut self.log_page)
            .map_err(|e| LogError::FileError(e))?;
        let mut last_saved_lsn = self.last_saved_lsn.lock().unwrap();
        let latest_lsn = *self.latest_lsn.lock().unwrap();
        *last_saved_lsn = latest_lsn;
        Ok(())
    }
}
