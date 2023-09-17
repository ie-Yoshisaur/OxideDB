use crate::buffer::buffer::Buffer;
use crate::buffer::err::BufferAbortException;
use crate::buffer::err::BufferError;
use crate::file::block_id::BlockId;
use crate::file::file_manager::FileManager;
use crate::log::log_manager::LogManager;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

/// Manages a pool of buffers.
///
/// `BufferManager` is responsible for allocating buffers to disk blocks,
/// tracking the availability of buffers, and ensuring safe concurrent access.
pub struct BufferManager {
    buffer_pool: Vec<Arc<Mutex<Buffer>>>,
    number_available: Arc<Mutex<usize>>,
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
        let buffer_pool: Result<Vec<Arc<Mutex<Buffer>>>, BufferError> = (0..number_buffers)
            .map(|_| {
                let buffer = Buffer::new(file_manager.clone(), log_manager.clone())?;
                Ok(Arc::new(Mutex::new(buffer)))
            })
            .collect();

        let buffer_pool = buffer_pool?;

        Ok(Self {
            buffer_pool,
            number_available: Arc::new(Mutex::new(number_buffers)),
            max_time: 10000,
            condvar: Condvar::new(),
        })
    }

    /// Returns the number of available buffers as an `Arc<Mutex<usize>>`.
    pub fn get_number_available(&self) -> Arc<Mutex<usize>> {
        self.number_available.clone()
    }

    /// Flushes all dirty buffers associated with a transaction to disk.
    ///
    /// # Arguments
    ///
    /// * `transaction_number`: The transaction number.
    ///
    /// # Errors
    ///
    /// Returns an error if flushing fails.
    pub fn flush_all(&self, transaction_number: i32) -> Result<(), BufferError> {
        for buffer in &self.buffer_pool {
            let mut locked_buffer = buffer.lock().unwrap();
            if locked_buffer.modifying_transaction() == transaction_number {
                locked_buffer.flush()?;
            }
        }
        Ok(())
    }

    /// Unpins a buffer and possibly makes it available for other transactions.
    ///
    /// # Arguments
    ///
    /// * `buffer`: An `Arc<Mutex<Buffer>>` that provides access to a buffer.
    pub fn unpin(&self, buffer: Arc<Mutex<Buffer>>) -> () {
        let mut locked_buffer = buffer.lock().unwrap();
        locked_buffer.unpin();
        let mut number_available = self.number_available.lock().unwrap();
        if !locked_buffer.is_pinned() {
            *number_available += 1;
            self.condvar.notify_all();
        }
    }

    /// Pins a buffer to a disk block and locks it for a transaction.
    ///
    /// # Arguments
    ///
    /// * `block`: The disk block to be pinned.
    ///
    /// # Errors
    ///
    /// Returns an error if no buffer is available.
    pub fn pin(&self, block: BlockId) -> Result<Arc<Mutex<Buffer>>, BufferError> {
        let start_time = Instant::now();
        let timeout = Duration::from_millis(self.max_time);
        loop {
            if let Some(buffer) = self.try_to_pin(&block) {
                return Ok(buffer);
            }
            if self.waiting_too_long(start_time) {
                return Err(BufferError::from(BufferAbortException));
            }
            let number_available = self.get_number_available();

            let (_lock, timeout_result) = self
                .condvar
                .wait_timeout(number_available.lock().unwrap(), timeout)
                .unwrap();

            if timeout_result.timed_out() {
                return Err(BufferError::from(BufferAbortException));
            }
        }
    }

    /// Tries to pin a buffer to the provided block.
    ///
    /// # Arguments
    ///
    /// * `block`: The disk block to be pinned.
    fn try_to_pin(&self, block: &BlockId) -> Option<Arc<Mutex<Buffer>>> {
        if let Some(buffer) = self.find_existing_buffer(block) {
            let mut locked_buffer = buffer.lock().unwrap();
            if !locked_buffer.is_pinned() {
                let mut number_available = self.number_available.lock().unwrap();
                *number_available -= 1;
            }
            locked_buffer.pin();
            return Some(buffer.clone());
        }

        if let Some(buffer) = self.choose_unpinned_buffer().clone() {
            let mut locked_buffer = buffer.lock().unwrap();
            let mut number_available = self.number_available.lock().unwrap();
            locked_buffer.assign_to_block(block.clone());
            *number_available -= 1;
            locked_buffer.pin();
            return Some(buffer.clone());
        }

        None
    }

    /// Finds an existing buffer for the provided block, if available.
    ///
    /// # Arguments
    ///
    /// * `block`: The disk block to search for.
    fn find_existing_buffer(&self, block: &BlockId) -> Option<Arc<Mutex<Buffer>>> {
        for buffer in &self.buffer_pool {
            let locked_buffer = buffer.lock().unwrap();
            if locked_buffer.get_block().as_ref() == Some(&block) {
                return Some(buffer.clone());
            }
        }
        None
    }

    /// Chooses an unpinned buffer, if available.
    fn choose_unpinned_buffer(&self) -> Option<Arc<Mutex<Buffer>>> {
        for buffer in &self.buffer_pool {
            let locked_buffer = buffer.lock().unwrap();
            if !locked_buffer.is_pinned() {
                return Some(buffer.clone());
            }
        }
        None
    }

    /// Checks if the system has been waiting too long to pin a buffer.
    ///
    /// # Arguments
    ///
    /// * `start_time`: The instant at which the system started waiting.
    ///
    /// Returns `true` if the system has been waiting too long, otherwise `false`.
    fn waiting_too_long(&self, start_time: Instant) -> bool {
        let duration = Instant::now().duration_since(start_time);
        duration.as_millis() > self.max_time.into()
    }
}
