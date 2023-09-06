use crate::buffer::err::BufferError;
use crate::file::block_id::BlockId;
use crate::file::file_manager::FileManager;
use crate::file::page::Page;
use crate::log::log_manager::LogManager;
use std::sync::Arc;
use std::sync::Mutex;

/// `Buffer` wraps a page and stores information about its status,
/// such as the associated disk block, the number of times the buffer has been pinned,
/// whether its contents have been modified, and if so, the id and lsn of the modifying transaction.
pub struct Buffer {
    file_manager: Arc<Mutex<FileManager>>,
    log_manager: Arc<Mutex<LogManager>>,
    contents: Page,
    block: Option<BlockId>,
    pins: i32,
    transaction_number: i32,
    lsn: i32,
}

impl Buffer {
    /// Creates a new `Buffer` with initial settings.
    ///
    /// This function initializes a new buffer with the given FileManager and LogManager.
    /// It also sets the initial block size based on the FileManager's block size.
    ///
    /// # Arguments
    ///
    /// * `file_manager`: An `Arc` wrapped `FileManager` for file operations.
    /// * `log_manager`: An `Arc` wrapped `LogManager` for log operations.
    pub fn new(
        file_manager: Arc<Mutex<FileManager>>,
        log_manager: Arc<Mutex<LogManager>>,
    ) -> Result<Self, BufferError> {
        let block_size = file_manager
            .lock()
            .map_err(|_| BufferError::MutexLockError)?
            .get_block_size();
        let mut contents = Page::new_from_blocksize(block_size);
        Ok(Buffer {
            file_manager,
            log_manager,
            contents,
            block: None,
            pins: 0,
            transaction_number: -1,
            lsn: -1,
        })
    }

    /// Returns the contents of the buffer.
    pub fn get_contents(&mut self) -> &mut Page {
        &mut self.contents
    }

    /// Returns a reference to the disk block allocated to the buffer.
    pub fn get_block(&self) -> Option<&BlockId> {
        self.block.as_ref()
    }

    /// Sets the buffer as modified by a transaction.
    pub fn set_modified(&mut self, transaction_number: i32, lsn: i32) {
        self.transaction_number = transaction_number;
        if lsn >= 0 {
            self.lsn = lsn;
        }
    }

    /// Returns true if the buffer is currently pinned.
    pub fn is_pinned(&self) -> bool {
        self.pins > 0
    }

    /// Returns the ID of the modifying transaction.
    pub fn modifying_transaction(&self) -> i32 {
        self.transaction_number
    }

    /// Writes the buffer to its disk block if it is dirty.
    /// This function locks the LogManager and FileManager to ensure thread safety.
    pub fn assign_to_block(&mut self, block: BlockId) -> Result<(), BufferError> {
        self.flush()?;
        self.block = Some(block.clone());
        self.file_manager
            .lock()
            .map_err(|_| BufferError::MutexLockError)?
            .read(&block, &mut self.contents)
            .map_err(BufferError::from)?;

        self.pins = 0;
        Ok(())
    }

    /// Writes the buffer to its disk block if it is dirty.
    pub fn flush(&mut self) -> Result<(), BufferError> {
        if self.transaction_number >= 0 {
            {
                let mut log_manager = self
                    .log_manager
                    .lock()
                    .map_err(|_| BufferError::MutexLockError)?;
                log_manager
                    .flush_by_lsn(self.lsn)
                    .map_err(BufferError::from)?;
            }
            if let Some(ref block) = self.block {
                let file_manager = self
                    .file_manager
                    .lock()
                    .map_err(|_| BufferError::MutexLockError)?;
                file_manager
                    .write(block, &mut self.contents)
                    .map_err(BufferError::from)?;
            }
            self.transaction_number = -1;
        }
        Ok(())
    }

    /// Increases the buffer's pin count.
    pub fn pin(&mut self) {
        self.pins += 1;
    }

    /// Decreases the buffer's pin count.
    pub fn unpin(&mut self) {
        self.pins -= 1;
    }
}
