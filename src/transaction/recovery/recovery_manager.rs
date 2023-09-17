use crate::buffer::buffer::Buffer;
use crate::buffer::buffer_manager::BufferManager;
use crate::log::log_manager::LogManager;
use crate::transaction::recovery::err::RecoveryError;
use crate::transaction::recovery::log_record::LogRecord;
use crate::transaction::recovery::log_record::LogRecordType;
use crate::transaction::recovery::record::checkpoint_record::CheckpointRecord;
use crate::transaction::recovery::record::commit_record::CommitRecord;
use crate::transaction::recovery::record::rollback_record::RollbackRecord;
use crate::transaction::recovery::record::set_int_record::SetIntRecord;
use crate::transaction::recovery::record::set_string_record::SetStringRecord;
use crate::transaction::recovery::record::start_record::StartRecord;
use crate::transaction::transaction::Transaction;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

/// The `RecoveryManager` struct manages transaction recovery.
/// Each transaction has its own recovery manager.
pub struct RecoveryManager {
    log_manager: Arc<Mutex<LogManager>>,
    buffer_manager: Arc<Mutex<BufferManager>>,
    transaction_number: i32,
}

impl RecoveryManager {
    /// Creates a new `RecoveryManager` for the specified transaction.
    ///
    /// # Arguments
    ///
    /// * `transaction_number` - The ID of the specified transaction.
    /// * `log_manager` - The log manager.
    /// * `buffer_manager` - The buffer manager.
    pub fn new(
        transaction_number: i32,
        log_manager: Arc<Mutex<LogManager>>,
        buffer_manager: Arc<Mutex<BufferManager>>,
    ) -> Self {
        StartRecord::write_to_log(log_manager.clone(), transaction_number);
        Self {
            transaction_number,
            log_manager,
            buffer_manager,
        }
    }

    /// Writes a commit record to the log and flushes it to disk.
    ///
    /// # Returns
    ///
    /// * `Result<(), RecoveryError>` - Result of the operation.
    pub fn commit(&self) -> Result<(), RecoveryError> {
        self.buffer_manager
            .lock()
            .unwrap()
            .flush_all(self.transaction_number)
            .map_err(|e| RecoveryError::BufferError(e))?;
        let lsn = CommitRecord::write_to_log(self.log_manager.clone(), self.transaction_number)
            .map_err(|e| RecoveryError::LogRecordError(e))?;
        self.log_manager
            .lock()
            .unwrap()
            .flush_by_lsn(lsn)
            .map_err(|e| RecoveryError::LogError(e));
        Ok(())
    }

    /// Writes a rollback record to the log and flushes it to disk.
    ///
    /// # Arguments
    ///
    /// * `transaction` - The transaction to rollback.
    ///
    /// # Returns
    ///
    /// * `Result<(), RecoveryError>` - Result of the operation.
    pub fn rollback(&self, transaction: &mut Transaction) -> Result<(), RecoveryError> {
        self.do_rollback(transaction);
        self.buffer_manager
            .lock()
            .unwrap()
            .flush_all(self.transaction_number)
            .map_err(|e| RecoveryError::BufferError(e))?;
        let lsn = RollbackRecord::write_to_log(self.log_manager.clone(), self.transaction_number)
            .map_err(|e| RecoveryError::LogRecordError(e))?;
        self.log_manager
            .lock()
            .unwrap()
            .flush_by_lsn(lsn)
            .map_err(|e| RecoveryError::LogError(e))?;
        Ok(())
    }

    /// Recovers uncompleted transactions from the log and then writes a quiescent checkpoint record to the log and flush it.
    ///
    /// # Arguments
    ///
    /// * `transaction` - The transaction to recover.
    ///
    /// # Returns
    ///
    /// * `Result<(), RecoveryError>` - Result of the operation.
    pub fn recover(&self, transaction: &mut Transaction) -> Result<(), RecoveryError> {
        self.do_recover(transaction);
        self.buffer_manager
            .lock()
            .unwrap()
            .flush_all(self.transaction_number)
            .map_err(|e| RecoveryError::BufferError(e))?;
        let lsn = CheckpointRecord::write_to_log(self.log_manager.clone())
            .map_err(|e| RecoveryError::LogRecordError(e))?;
        self.log_manager
            .lock()
            .unwrap()
            .flush_by_lsn(lsn)
            .map_err(|e| RecoveryError::LogError(e))?;
        Ok(())
    }

    /// Writes a `setint` record to the log and returns its LSN.
    ///
    /// # Arguments
    ///
    /// * `buffer` - The buffer containing the page.
    /// * `offset` - The offset of the value in the page.
    /// * `_new_value` - The value to be written.
    ///
    /// # Returns
    ///
    /// * `Result<i32, RecoveryError>` - Result of the operation.
    pub fn set_int(
        &self,
        buffer: &mut Buffer,
        offset: i32,
        _new_value: i32,
    ) -> Result<i32, RecoveryError> {
        let old_value = buffer
            .get_contents()
            .get_int(offset as usize)
            .map_err(|e| RecoveryError::PageError(e))?;
        let block = buffer
            .get_block()
            .ok_or(RecoveryError::BlockNotFoundError)?;
        SetIntRecord::write_to_log(
            self.log_manager.clone(),
            self.transaction_number,
            block,
            offset,
            old_value,
        )
        .map_err(|e| RecoveryError::LogRecordError(e))
    }

    /// Writes a `setstring` record to the log and returns its LSN.
    ///
    /// # Arguments
    ///
    /// * `buffer` - The buffer containing the page.
    /// * `offset` - The offset of the value in the page.
    /// * `_new_value` - The value to be written.
    ///
    /// # Returns
    ///
    /// * `Result<i32, RecoveryError>` - Result of the operation.
    pub fn set_string(
        &self,
        buffer: &mut Buffer,
        offset: i32,
        _new_value: String,
    ) -> Result<i32, RecoveryError> {
        let old_value = buffer
            .get_contents()
            .get_string(offset as usize)
            .map_err(|e| RecoveryError::PageError(e))?;
        let block = buffer
            .get_block()
            .ok_or(RecoveryError::BlockNotFoundError)?;
        SetStringRecord::write_to_log(
            self.log_manager.clone(),
            self.transaction_number,
            block,
            offset,
            &old_value,
        )
        .map_err(|e| RecoveryError::LogRecordError(e))
    }

    /// Private method to rollback the transaction by iterating through the log records.
    fn do_rollback(&self, transaction: &mut Transaction) -> Result<(), RecoveryError> {
        let iterator = self
            .log_manager
            .lock()
            .unwrap()
            .iterator()
            .map_err(|e| RecoveryError::LogError(e))?;
        for bytes in iterator {
            let record: Box<dyn LogRecord> =
                <dyn LogRecord>::create_log_record(bytes.map_err(|e| RecoveryError::LogError(e))?)
                    .map_err(|e| RecoveryError::LogRecordError(e))?;
            if record.get_transaction_number() == self.transaction_number {
                if let Some(LogRecordType::Start) =
                    LogRecordType::get_record_type_from_i32(record.get_log_record_type())
                {
                    return Ok(());
                }
                record.undo(transaction);
            }
        }
        Ok(())
    }

    /// Private method to do a complete database recovery.
    fn do_recover(&self, transaction: &mut Transaction) -> Result<(), RecoveryError> {
        let mut finished_transactions = HashSet::new();
        let iterator = self
            .log_manager
            .lock()
            .unwrap()
            .iterator()
            .map_err(|e| RecoveryError::LogError(e))?;
        for bytes in iterator {
            let record: Box<dyn LogRecord> =
                <dyn LogRecord>::create_log_record(bytes.map_err(|e| RecoveryError::LogError(e))?)
                    .map_err(|e| RecoveryError::LogRecordError(e))?;
            match LogRecordType::get_record_type_from_i32(record.get_log_record_type()) {
                Some(LogRecordType::Checkpoint) => return Ok(()),
                Some(LogRecordType::Commit) | Some(LogRecordType::Rollback) => {
                    finished_transactions.insert(record.get_transaction_number());
                }
                _ => {
                    if !finished_transactions.contains(&record.get_transaction_number()) {
                        record.undo(transaction);
                    }
                }
            }
        }
        Ok(())
    }
}
