use crate::file::page::Page;
use crate::log::log_manager::LogManager;
use crate::transaction::recovery::err::LogRecordError;
use crate::transaction::recovery::log_record::LogRecord;
use crate::transaction::recovery::log_record::LogRecordType::Commit;
use crate::transaction::transaction::Transaction;
use std::mem::size_of;
use std::sync::{Arc, Mutex};

const I32_SIZE: usize = size_of::<i32>();

/// Represents a Commit log record.
/// This log record contains the COMMIT operator,
/// followed by the transaction id.
pub struct CommitRecord {
    transaction_number: i32,
}

impl CommitRecord {
    // Create a new CommitRecord by reading the transaction id from the page
    pub fn new(page: &mut Page) -> Result<Self, LogRecordError> {
        let transaction_position = I32_SIZE;
        let transaction_number = page
            .get_int(transaction_position)
            .map_err(|e| LogRecordError::PageError(e))?;
        Ok(Self { transaction_number })
    }

    /// A static method to write a CommitRecord to the log.
    ///
    /// # Returns
    ///
    /// Returns the LSN of the last log value.
    pub fn write_to_log(
        log_manager: Arc<Mutex<LogManager>>,
        txnum: i32,
    ) -> Result<i32, LogRecordError> {
        let mut page = Page::new_from_blocksize(2 * I32_SIZE);
        page.set_int(0, Commit as i32).unwrap();
        page.set_int(I32_SIZE, txnum).unwrap();
        log_manager
            .lock()
            .unwrap()
            .append(&page.read_bytes(0, 2 * I32_SIZE).unwrap())
            .map_err(|e| LogRecordError::LogError(e))
    }
}

impl LogRecord for CommitRecord {
    /// Returns the log record's type as an i32.
    fn get_log_record_type(&self) -> i32 {
        Commit as i32
    }

    /// Returns the log record's transaction id as an i32.
    fn get_transaction_number(&self) -> i32 {
        self.transaction_number
    }

    /// Does nothing, because a commit record contains no undo information.
    fn undo(&self, _transaction: &mut Transaction) {
        // Does nothing
    }
}

impl std::fmt::Display for CommitRecord {
    /// Formats the CommitRecord for display purposes.
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "<COMMIT {}>", self.transaction_number)
    }
}
