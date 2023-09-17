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

/// Provides transaction management for clients,
/// ensuring that all transactions are serializable, recoverable,
/// and in general satisfy the ACID properties.
#[derive(Clone)]
pub struct Transaction {
    file_manager: Arc<Mutex<FileManager>>,
    buffer_manager: Arc<Mutex<BufferManager>>,
    log_manager: Arc<Mutex<LogManager>>,
    transaction_number: i32,
    concurrency_manager: Arc<Mutex<ConcurrencyManager>>,
    buffer_list: Arc<Mutex<BufferList>>,
    recovery_manager: Arc<Mutex<RecoveryManager>>,
}

impl Transaction {
    /// Creates a new transaction and its associated
    /// recovery and concurrency managers.
    pub fn new(
        file_manager: Arc<Mutex<FileManager>>,
        log_manager: Arc<Mutex<LogManager>>,
        buffer_manager: Arc<Mutex<BufferManager>>,
        lock_table: Arc<Mutex<LockTable>>,
    ) -> Self {
        let transaction_number = NEXT_TRANSACTION_NUM.fetch_add(1, Ordering::SeqCst);
        Self {
            file_manager,
            log_manager: log_manager.clone(),
            buffer_manager: buffer_manager.clone(),
            transaction_number,
            concurrency_manager: Arc::new(Mutex::new(ConcurrencyManager::new(lock_table))),
            buffer_list: Arc::new(Mutex::new(BufferList::new(buffer_manager.clone()))),
            recovery_manager: Arc::new(Mutex::new(RecoveryManager::new(
                transaction_number,
                log_manager,
                buffer_manager,
            ))),
        }
    }

    /// Commits the current transaction.
    /// Flushes all modified buffers (and their log records),
    /// writes and flushes a commit record to the log,
    /// releases all locks, and unpins any pinned buffers.
    pub fn commit(&mut self) {
        self.recovery_manager.lock().unwrap().commit();
        println!("transaction {} committed", self.transaction_number);
        self.concurrency_manager.lock().unwrap().release();
        self.buffer_list.lock().unwrap().unpin_all();
    }

    /// Rolls back the current transaction.
    /// Undoes any modified values,
    /// flushes those buffers,
    /// writes and flushes a rollback record to the log,
    /// releases all locks, and unpins any pinned buffers.
    pub fn rollback(&mut self) {
        let mut self_clone = self.clone();
        self.recovery_manager
            .lock()
            .unwrap()
            .rollback(&mut self_clone);
        println!("transaction {} rolled back", self.transaction_number);
        self.concurrency_manager.lock().unwrap().release();
    }

    /// Recovers the system by flushing all modified buffers.
    /// Then goes through the log, rolling back all
    /// uncommitted transactions. Finally,
    /// writes a quiescent checkpoint record to the log.
    /// This method is called during system startup,
    /// before user transactions begin.
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
            .recover(&mut self_clone);
        Ok(())
    }

    /// Pins the specified block.
    /// The transaction manages the buffer for the client.
    pub fn pin(&mut self, block: BlockId) {
        self.buffer_list.lock().unwrap().pin(block);
    }

    /// Unpins the specified block.
    /// The transaction looks up the buffer pinned to this block,
    /// and unpins it.
    pub fn unpin(&mut self, block: BlockId) {
        self.buffer_list.lock().unwrap().unpin(block);
    }

    /// Returns the integer value stored at the specified offset of the specified block.
    /// The method first obtains an SLock on the block,
    /// then it retrieves the value from the buffer.
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

    /// Returns the string value stored at the specified offset of the specified block.
    /// The method first obtains an SLock on the block,
    /// then it retrieves the value from the buffer.
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

    /// Stores an integer at the specified offset of the specified block.
    /// The method first obtains an XLock on the block,
    /// then it stores the value into the buffer.
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

    /// Stores a string at the specified offset of the specified block.
    /// The method first obtains an XLock on the block,
    /// then it stores the value into the buffer.
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

    /// Returns the number of blocks in the specified file.
    /// This method first obtains an SLock on the "end of the file",
    /// before asking the file manager to return the file size.
    pub fn size(&mut self, filename: &str) -> Result<usize, TransactionError> {
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

    /// Appends a new block to the end of the specified file and returns a reference to it.
    /// This method first obtains an XLock on the "end of the file", before performing the append.
    pub fn append(&mut self, filename: &str) -> Result<BlockId, TransactionError> {
        let dummy_block = BlockId::new(filename.to_string(), -1);
        let mut locked_concurrency_manager = self.concurrency_manager.lock().unwrap();
        locked_concurrency_manager
            .x_lock(dummy_block)
            .map_err(|e| TransactionError::ConcurrencyError(e));
        let locked_file_manager = self.file_manager.lock().unwrap();
        locked_file_manager
            .append(filename)
            .map_err(|e| TransactionError::FileError(e))
    }

    /// Returns the block size as managed by the file manager.
    pub fn block_size(&self) -> usize {
        let locked_file_manager = self.file_manager.lock().unwrap();
        locked_file_manager.get_block_size()
    }

    /// Returns the number of available buffers as managed by the buffer manager.
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
