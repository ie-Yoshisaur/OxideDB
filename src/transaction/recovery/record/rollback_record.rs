use crate::file::page::Page;
use crate::log::log_manager::LogManager;
use crate::transaction::recovery::err::LogRecordError;
use crate::transaction::recovery::log_record::LogRecord;
use crate::transaction::recovery::log_record::LogRecordType::Rollback;
use crate::transaction::transaction::Transaction;
use std::mem::size_of;
use std::sync::{Arc, Mutex};

const I32_SIZE: usize = size_of::<i32>();

/// Represents a Rollback log record.
/// This log record contains the ROLLBACK operator,
/// followed by the transaction id.
pub struct RollbackRecord {
    transaction_number: i32,
}

impl RollbackRecord {
    // Create a new RollbackRecord by reading the transaction id from the page
    pub fn new(page: &mut Page) -> Result<Self, LogRecordError> {
        let transaction_position = I32_SIZE;
        let transaction_number = page
            .get_int(transaction_position)
            .map_err(|e| LogRecordError::PageError(e))?;
        Ok(Self { transaction_number })
    }

    /// A static method to write a RollbackRecord to the log.
    ///
    /// # Returns
    ///
    /// Returns the LSN of the last log value.
    pub fn write_to_log(
        log_manager: Arc<Mutex<LogManager>>,
        txnum: i32,
    ) -> Result<i32, LogRecordError> {
        let mut page = Page::new_from_blocksize(2 * I32_SIZE);
        page.set_int(0, Rollback as i32)
            .map_err(|e| LogRecordError::PageError(e))?;
        page.set_int(I32_SIZE, txnum).unwrap();
        log_manager
            .lock()
            .unwrap()
            .append(
                &page
                    .read_bytes(0, 2 * I32_SIZE)
                    .map_err(|e| LogRecordError::PageError(e))?,
            )
            .map_err(|e| LogRecordError::LogError(e))
    }
}

impl LogRecord for RollbackRecord {
    /// Returns the log record's type as an i32.
    fn get_log_record_type(&self) -> i32 {
        Rollback as i32
    }

    /// Returns the log record's transaction id as an i32.
    fn get_transaction_number(&self) -> i32 {
        self.transaction_number
    }

    /// Does nothing, because a rollback record contains no undo information.
    fn undo(&self, _transaction: &mut Transaction) {
        // Does nothing
    }
}

impl std::fmt::Display for RollbackRecord {
    /// Formats the RollbackRecord for display purposes.
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "<ROLLBACK {}>", self.transaction_number)
    }
}
