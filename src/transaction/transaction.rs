use crate::buffer::buffer_manager::BufferManager;
use crate::file::block_id::BlockId;
use crate::file::file_manager::FileManager;
use crate::log::log_manager::LogManager;
use crate::transaction::buffer_list::BufferList;
use crate::transaction::concurrency::concurrency_manager::ConcurrencyManager;
use crate::transaction::concurrency::lock_table::LockTable;
use crate::transaction::err::TransactionError;
use crate::transaction::recovery::recovery_manager::RecoveryManager;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex};

/// Provides an atomic counter for generating unique transaction numbers.
static NEXT_TRANSACTION_NUM: AtomicI32 = AtomicI32::new(0);

/// Provides transaction management for clients, ensuring that all transactions are serializable, recoverable, and in general satisfy the ACID properties.
#[derive(Clone)]
pub struct Transaction {
    /// A thread-safe reference to the file manager.
    file_manager: Arc<Mutex<FileManager>>,
    /// A thread-safe reference to the buffer manager.
    buffer_manager: Arc<Mutex<BufferManager>>,
    /// A thread-safe reference to the log manager.
    log_manager: Arc<Mutex<LogManager>>,
    /// A unique identifier for the transaction.
    transaction_number: i32,
    /// A thread-safe reference to the concurrency manager.
    concurrency_manager: Arc<Mutex<ConcurrencyManager>>,
    /// A thread-safe reference to the buffer list.
    buffer_list: Arc<Mutex<BufferList>>,
    /// A thread-safe reference to the recovery manager.
    recovery_manager: Arc<Mutex<RecoveryManager>>,
}

impl Transaction {
    /// Creates a new transaction and its associated recovery and concurrency managers.
    ///
    /// # Arguments
    /// * `file_manager`: A thread-safe reference to the file manager.
    /// * `log_manager`: A thread-safe reference to the log manager.
    /// * `buffer_manager`: A thread-safe reference to the buffer manager.
    /// * `lock_table`: A thread-safe reference to the lock table.
    ///
    /// # Returns
    /// * `Result<Self, TransactionError>`: Returns a new `Transaction` object wrapped in a `Result`.
    pub fn new(
        file_manager: Arc<Mutex<FileManager>>,
        log_manager: Arc<Mutex<LogManager>>,
        buffer_manager: Arc<Mutex<BufferManager>>,
        lock_table: Arc<Mutex<LockTable>>,
    ) -> Result<Self, TransactionError> {
        let transaction_number = NEXT_TRANSACTION_NUM.fetch_add(1, Ordering::SeqCst);
        Ok(Self {
            file_manager,
            log_manager: log_manager.clone(),
            buffer_manager: buffer_manager.clone(),
            transaction_number,
            concurrency_manager: Arc::new(Mutex::new(ConcurrencyManager::new(lock_table))),
            buffer_list: Arc::new(Mutex::new(BufferList::new(buffer_manager.clone()))),
            recovery_manager: Arc::new(Mutex::new(
                RecoveryManager::new(transaction_number, log_manager, buffer_manager)
                    .map_err(|e| TransactionError::RecoveryError(e))?,
            )),
        })
    }

    /// Commits the current transaction. Flushes all modified buffers (and their log records), writes and flushes a commit record to the log, releases all locks, and unpins any pinned buffers.
    ///
    /// # Returns
    /// * `Result<(), TransactionError>`: Returns `Ok(())` if the transaction commits successfully, otherwise returns an error.
    pub fn commit(&mut self) -> Result<(), TransactionError> {
        self.recovery_manager
            .lock()
            .unwrap()
            .commit()
            .map_err(|e| TransactionError::RecoveryError(e))?;
        println!("transaction {} committed", self.transaction_number);
        self.concurrency_manager.lock().unwrap().release();
        self.buffer_list.lock().unwrap().unpin_all();
        Ok(())
    }

    /// Rolls back the current transaction. Undoes any modified values, flushes those buffers, writes and flushes a rollback record to the log, releases all locks, and unpins any pinned buffers.
    ///
    /// # Returns
    /// * `Result<(), TransactionError>`: Returns `Ok(())` if the transaction rolls back successfully, otherwise returns an error.
    pub fn rollback(&mut self) -> Result<(), TransactionError> {
        let mut self_clone = self.clone();
        self.recovery_manager
            .lock()
            .unwrap()
            .rollback(&mut self_clone)
            .map_err(|e| TransactionError::RecoveryError(e))?;
        println!("transaction {} rolled back", self.transaction_number);
        self.concurrency_manager.lock().unwrap().release();
        Ok(())
    }

    /// Recovers the system by flushing all modified buffers. Then goes through the log, rolling back all uncommitted transactions. Finally, writes a quiescent checkpoint record to the log. This method is called during system startup, before user transactions begin.
    ///
    /// # Returns
    /// * `Result<(), TransactionError>`: Returns `Ok(())` if the system recovers successfully, otherwise returns an error.
    pub fn recover(&mut self) -> Result<(), TransactionError> {
        let mut self_clone = self.clone();
        self.buffer_manager
            .lock()
            .unwrap()
            .flush_all(self.transaction_number)
            .map_err(|e| TransactionError::BufferError(e))?;
        self.recovery_manager
            .lock()
            .unwrap()
            .recover(&mut self_clone)
            .map_err(|e| TransactionError::RecoveryError(e))?;
        Ok(())
    }

    /// Pins a specified block in the buffer pool.
    ///
    /// The function ensures that the block is loaded into the buffer pool and
    /// pinned to prevent it from being replaced.
    ///
    /// # Arguments
    ///
    /// * `block: BlockId` - The ID of the block to pin.
    pub fn pin(&mut self, block: BlockId) {
        self.buffer_list.lock().unwrap().pin(block);
    }

    /// Unpins the specified block. The transaction looks up the buffer pinned to this block, and unpins it.
    ///
    /// The function releases the pin on the block, allowing it to be replaced
    /// if needed.
    ///
    /// # Arguments
    ///
    /// * `block: BlockId` - The ID of the block to unpin.
    pub fn unpin(&mut self, block: BlockId) {
        self.buffer_list.lock().unwrap().unpin(block);
    }

    /// Retrieves an integer value from a specified block and offset.
    ///
    /// The function first acquires a shared lock (SLock) on the block, then
    /// retrieves the integer value from the buffer pool.
    ///
    /// # Arguments
    ///
    /// * `block: BlockId` - The ID of the block to read from.
    /// * `offset: i32` - The offset within the block to read the integer from.
    ///
    /// # Returns
    ///
    /// * `Result<Option<i32>, TransactionError>` - The integer value read or an error.
    pub fn get_int(
        &mut self,
        block: BlockId,
        offset: i32,
    ) -> Result<Option<i32>, TransactionError> {
        self.concurrency_manager
            .lock()
            .unwrap()
            .s_lock(block.clone())
            .map_err(|e| TransactionError::ConcurrencyError(e))?;
        let locked_buffer_list = self.buffer_list.lock().unwrap();
        if let Some(buffer) = locked_buffer_list.get_buffer(&block) {
            let mut locked_buffer = buffer.lock().unwrap();
            Ok(Some(
                locked_buffer
                    .get_contents()
                    .get_int(offset as usize)
                    .map_err(|e| TransactionError::PageError(e))?,
            ))
        } else {
            Ok(None)
        }
    }

    /// Retrieves a string value from a specified block and offset.
    ///
    /// The function first acquires a shared lock (SLock) on the block, then
    /// retrieves the string value from the buffer pool.
    ///
    /// # Arguments
    ///
    /// * `block: BlockId` - The ID of the block to read from.
    /// * `offset: i32` - The offset within the block to read the string from.
    ///
    /// # Returns
    ///
    /// * `Result<Option<String>, TransactionError>` - The string value read or an error.
    pub fn get_string(
        &mut self,
        block: BlockId,
        offset: i32,
    ) -> Result<Option<String>, TransactionError> {
        self.concurrency_manager
            .lock()
            .unwrap()
            .s_lock(block.clone())
            .map_err(|e| TransactionError::ConcurrencyError(e))?;
        let locked_buffer_list = self.buffer_list.lock().unwrap();
        if let Some(buffer) = locked_buffer_list.get_buffer(&block) {
            let mut locked_buffer = buffer.lock().unwrap();
            Ok(Some(
                locked_buffer
                    .get_contents()
                    .get_string(offset as usize)
                    .map_err(|e| TransactionError::PageError(e))?,
            ))
        } else {
            Ok(None)
        }
    }

    /// Stores an integer value at a specified block and offset.
    ///
    /// The function first acquires an exclusive lock (XLock) on the block, then
    /// stores the integer value into the buffer pool.
    ///
    /// # Arguments
    ///
    /// * `block: BlockId` - The ID of the block to write to.
    /// * `offset: i32` - The offset within the block to write the integer to.
    /// * `value: i32` - The integer value to write.
    /// * `ok_to_log: bool` - Whether to log this operation.
    ///
    /// # Returns
    ///
    /// * `Result<(), TransactionError>` - Indicates success or an error.
    pub fn set_int(
        &mut self,
        block: BlockId,
        offset: i32,
        value: i32,
        ok_to_log: bool,
    ) -> Result<(), TransactionError> {
        self.concurrency_manager
            .lock()
            .unwrap()
            .x_lock(block.clone())
            .unwrap();
        let locked_buffer_list = self.buffer_list.lock().unwrap();
        if let Some(buffer) = locked_buffer_list.get_buffer(&block) {
            let mut locked_buffer = buffer.lock().unwrap();
            let lsn = if ok_to_log {
                self.recovery_manager
                    .lock()
                    .unwrap()
                    .set_int(&mut locked_buffer, offset, value)
                    .map_err(|e| TransactionError::RecoveryError(e))?
            } else {
                -1
            };
            locked_buffer
                .get_contents()
                .set_int(offset as usize, value)
                .unwrap();
            locked_buffer.set_modified(self.transaction_number, lsn);
            Ok(())
        } else {
            Err(TransactionError::BufferNotFoundError)
        }
    }

    /// Stores a string value at a specified block and offset.
    ///
    /// The function first acquires an exclusive lock (XLock) on the block, then
    /// stores the string value into the buffer pool.
    ///
    /// # Arguments
    ///
    /// * `block: BlockId` - The ID of the block to write to.
    /// * `offset: i32` - The offset within the block to write the string to.
    /// * `value: &String` - The string value to write.
    /// * `ok_to_log: bool` - Whether to log this operation.
    ///
    /// # Returns
    ///
    /// * `Result<(), TransactionError>` - Indicates success or an error.
    pub fn set_string(
        &mut self,
        block: BlockId,
        offset: i32,
        value: &String,
        ok_to_log: bool,
    ) -> Result<(), TransactionError> {
        self.concurrency_manager
            .lock()
            .unwrap()
            .x_lock(block.clone())
            .map_err(|e| TransactionError::ConcurrencyError(e))?;
        let locked_buffer_list = self.buffer_list.lock().unwrap();
        if let Some(buffer) = locked_buffer_list.get_buffer(&block) {
            let mut locked_buffer = buffer.lock().unwrap();
            let lsn = if ok_to_log {
                self.recovery_manager
                    .lock()
                    .unwrap()
                    .set_string(&mut locked_buffer, offset, value.clone())
                    .map_err(|e| TransactionError::RecoveryError(e))?
            } else {
                -1
            };
            locked_buffer
                .get_contents()
                .set_string(offset as usize, &value)
                .map_err(|e| TransactionError::PageError(e))?;
            locked_buffer.set_modified(self.transaction_number, lsn);
            Ok(())
        } else {
            Err(TransactionError::BufferNotFoundError)
        }
    }

    /// Retrieves the number of blocks in a specified file.
    ///
    /// The function first acquires a shared lock (SLock) on the "end of the file",
    /// then queries the file manager for the file size.
    ///
    /// # Arguments
    ///
    /// * `filename: &str` - The name of the file to query.
    ///
    /// # Returns
    ///
    /// * `Result<usize, TransactionError>` - The number of blocks in the file or an error.
    pub fn get_size(&mut self, filename: &str) -> Result<usize, TransactionError> {
        let dummy_block = BlockId::new(filename.to_string(), -1);
        let mut locked_concurrency_manager = self.concurrency_manager.lock().unwrap();
        locked_concurrency_manager
            .s_lock(dummy_block)
            .map_err(|e| TransactionError::ConcurrencyError(e))?;
        let locked_file_manager = self.file_manager.lock().unwrap();
        locked_file_manager
            .length(filename)
            .map_err(|e| TransactionError::FileError(e))
    }

    /// Appends a new block to the end of a specified file.
    ///
    /// The function first acquires an exclusive lock (XLock) on the "end of the file",
    /// then appends a new block to the file.
    ///
    /// # Arguments
    ///
    /// * `filename: &str` - The name of the file to append to.
    ///
    /// # Returns
    ///
    /// * `Result<BlockId, TransactionError>` - The ID of the new block or an error.
    pub fn append(&mut self, filename: &str) -> Result<BlockId, TransactionError> {
        let dummy_block = BlockId::new(filename.to_string(), -1);
        let mut locked_concurrency_manager = self.concurrency_manager.lock().unwrap();
        locked_concurrency_manager
            .x_lock(dummy_block)
            .map_err(|e| TransactionError::ConcurrencyError(e))?;
        let locked_file_manager = self.file_manager.lock().unwrap();
        locked_file_manager
            .append(filename)
            .map_err(|e| TransactionError::FileError(e))
    }

    /// Retrieves the block size as managed by the file manager.
    ///
    /// # Returns
    ///
    /// * `usize` - The size of a block in bytes.
    pub fn block_size(&self) -> usize {
        let locked_file_manager = self.file_manager.lock().unwrap();
        locked_file_manager.get_block_size()
    }

    /// Retrieves the number of available buffers as managed by the buffer manager.
    ///
    /// # Returns
    ///
    /// * `i32` - The number of available buffers.
    pub fn available_buffers(&self) -> i32 {
        let locked_buffer_manager = self.buffer_manager.lock().unwrap();
        let locked_available_buffers = locked_buffer_manager
            .get_number_available()
            .lock()
            .unwrap()
            .clone();
        locked_available_buffers as i32
    }
}
