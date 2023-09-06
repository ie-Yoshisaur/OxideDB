use crate::buffer::buffer::Buffer;
use crate::buffer::err::BufferAbortException;
use crate::buffer::err::BufferError;
use crate::file::block_id::BlockId;
use crate::file::file_manager::FileManager;
use crate::log::log_manager::LogManager;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

/// Manages a pool of buffers.
///
/// `BufferManager` is responsible for allocating buffers to disk blocks,
/// tracking the availability of buffers, and ensuring safe concurrent access.
pub struct BufferManager {
    buffer_pool: Vec<Mutex<Buffer>>,
    number_available: Mutex<usize>,
    max_time: u64,
    condvar: Condvar,
}

impl BufferManager {
    /// Creates a new `BufferManager` with the given FileManager and LogManager.
    ///
    /// # Arguments
    ///
    /// * `file_manager`: An `Arc` wrapped `FileManager` for file operations.
    /// * `log_manager`: An `Arc` wrapped `LogManager` for log operations.
    /// * `number_buffers`: The number of buffers to be managed.
    pub fn new(
        file_manager: Arc<Mutex<FileManager>>,
        log_manager: Arc<Mutex<LogManager>>,
        number_buffers: usize,
    ) -> Result<Self, BufferError> {
        let buffer_pool: Result<Vec<Mutex<Buffer>>, BufferError> = (0..number_buffers)
            .map(|_| {
                let buffer = Buffer::new(file_manager.clone(), log_manager.clone())?;
                Ok(Mutex::new(buffer))
            })
            .collect();

        let buffer_pool = buffer_pool?;

        Ok(Self {
            buffer_pool,
            number_available: Mutex::new(number_buffers),
            max_time: 10000,
            condvar: Condvar::new(),
        })
    }

    /// Returns the number of available buffers.
    ///
    /// # Errors
    ///
    /// Returns an error if the mutex lock is poisoned.
    pub fn get_number_available(&self) -> Result<MutexGuard<usize>, BufferError> {
        let number_available = self
            .number_available
            .lock()
            .map_err(|_| BufferError::MutexLockError)?;
        Ok(number_available)
    }

    /// Flushes all dirty buffers associated with a transaction to disk.
    ///
    /// # Arguments
    ///
    /// * `txnum`: The transaction number.
    ///
    /// # Errors
    ///
    /// Returns an error if the mutex lock is poisoned or if flushing fails.
    pub fn flush_all(&self, txnum: i32) -> Result<(), BufferError> {
        for buffer_mutex in &self.buffer_pool {
            let mut buffer = buffer_mutex
                .lock()
                .map_err(|_| BufferError::MutexLockError)?;
            if buffer.modifying_transaction() == txnum {
                buffer.flush()?; // Assuming `flush` returns a `Result`
            }
        }
        Ok(())
    }

    /// Unpins a buffer and possibly makes it available for other transactions.
    ///
    /// # Arguments
    ///
    /// * `buffer`: A mutex guard that provides access to a buffer.
    ///
    /// # Errors
    ///
    /// Returns an error if the mutex lock is poisoned.
    pub fn unpin(&self, buffer: MutexGuard<Buffer>) -> Result<(), BufferError> {
        let mut buffer = buffer;
        buffer.unpin();
        let mut number_available = self.get_number_available()?;
        if !buffer.is_pinned() {
            *number_available += 1;
            self.condvar.notify_all();
        }
        Ok(())
    }

    /// Pins a buffer to a disk block and locks it for a transaction.
    ///
    /// # Arguments
    ///
    /// * `block`: The disk block to be pinned.
    ///
    /// # Errors
    ///
    /// Returns an error if no buffer is available or if the mutex lock is poisoned.
    pub fn pin(&self, block: BlockId) -> Result<MutexGuard<Buffer>, BufferError> {
        let start_time = Instant::now();
        let timeout = Duration::from_millis(self.max_time);
        loop {
            if let Some(buffer) = self.try_to_pin(&block)? {
                return Ok(buffer);
            }
            if self.waiting_too_long(start_time) {
                return Err(BufferError::from(BufferAbortException));
            }
            let number_available = self.get_number_available()?;

            let (_lock, timeout_result) = self
                .condvar
                .wait_timeout(number_available, timeout)
                .map_err(|_| BufferError::MutexLockError)?;

            // Handle timeout_result as needed. It is a bool that's true if a timeout occurred.
            if timeout_result.timed_out() {
                return Err(BufferError::from(BufferAbortException));
            }
        }
    }

    /// Tries to pin a buffer to the provided block.
    ///
    /// # Arguments
    ///
    /// * `block`: The `BlockId` for which a buffer is required.
    ///
    /// # Returns
    ///
    /// Returns an `Option` containing a `MutexGuard` for a `Buffer` if one is available.
    ///
    /// # Errors
    ///
    /// Returns an error if the mutex lock is poisoned.
    fn try_to_pin(&self, block: &BlockId) -> Result<Option<MutexGuard<Buffer>>, BufferError> {
        let mut number_available = self.get_number_available()?;
        if let Some(mut buffer) = self.find_existing_buffer(block)? {
            if !buffer.is_pinned() {
                *number_available -= 1;
            }
            buffer.pin();
            return Ok(Some(buffer));
        }
        if let Some(mut buffer) = self.choose_unpinned_buffer()? {
            buffer.assign_to_block(block.clone());
            *number_available -= 1;
            buffer.pin();
            return Ok(Some(buffer));
        }
        Ok(None)
    }

    /// Finds an existing buffer for the given block.
    ///
    /// # Arguments
    ///
    /// * `block`: The `BlockId` to find the buffer for.
    ///
    /// # Returns
    ///
    /// Returns an `Option` containing a `MutexGuard` for a `Buffer` if one exists for the block.
    ///
    /// # Errors
    ///
    /// Returns an error if the mutex lock is poisoned.
    pub fn find_existing_buffer(
        &self,
        block: &BlockId,
    ) -> Result<Option<MutexGuard<Buffer>>, BufferError> {
        for buffer_mutex in &self.buffer_pool {
            let buffer = buffer_mutex
                .lock()
                .map_err(|_| BufferError::MutexLockError)?;
            if buffer.get_block().map_or(false, |b| *b == *block) {
                return Ok(Some(buffer));
            }
        }
        Ok(None)
    }

    /// Chooses an unpinned buffer from the buffer pool.
    ///
    /// # Returns
    ///
    /// Returns an `Option` containing a `MutexGuard` for a `Buffer` if an unpinned one is available.
    ///
    /// # Errors
    ///
    /// Returns an error if the mutex lock is poisoned.
    fn choose_unpinned_buffer(&self) -> Result<Option<MutexGuard<Buffer>>, BufferError> {
        for buffer_mutex in &self.buffer_pool {
            let buffer = buffer_mutex
                .lock()
                .map_err(|_| BufferError::MutexLockError)?;
            if !buffer.is_pinned() {
                return Ok(Some(buffer));
            }
        }
        Ok(None)
    }

    /// Checks if waiting for a buffer has exceeded the maximum time.
    ///
    /// # Arguments
    ///
    /// * `start_time`: The `Instant` at which the waiting started.
    ///
    /// # Returns
    ///
    /// Returns `true` if waiting has exceeded the maximum time, otherwise `false`.
    fn waiting_too_long(&self, start_time: Instant) -> bool {
        start_time.elapsed() > Duration::from_millis(self.max_time)
    }
}
