use crate::file::page::Page;
use crate::log::log_manager::LogManager;
use crate::transaction::recovery::err::LogRecordError;
use crate::transaction::recovery::log_record::LogRecord;
use crate::transaction::recovery::log_record::LogRecordType::Checkpoint;
use crate::transaction::transaction::Transaction;
use std::mem::size_of;
use std::sync::{Arc, Mutex};

const I32_SIZE: usize = size_of::<i32>();

/// Represents a Checkpoint log record.
/// This log record contains only the CHECKPOINT operator.
pub struct CheckpointRecord;

impl CheckpointRecord {
    /// Create a new CheckpointRecord.
    pub fn new() -> Self {
        Self
    }

    /// A static method to write a CheckpointRecord to the log.
    ///
    /// # Returns
    ///
    /// Returns the LSN of the last log value.
    pub fn write_to_log(log_manager: Arc<Mutex<LogManager>>) -> Result<i32, LogRecordError> {
        let mut page = Page::new_from_blocksize(I32_SIZE);
        page.set_int(0, Checkpoint as i32)
            .map_err(|e| LogRecordError::PageError(e))?;
        log_manager
            .lock()
            .unwrap()
            .append(&page.read_bytes(0, I32_SIZE).unwrap())
            .map_err(|e| LogRecordError::LogError(e))
    }
}

impl LogRecord for CheckpointRecord {
    /// Returns the log record's type as an i32.
    fn get_log_record_type(&self) -> i32 {
        Checkpoint as i32
    }

    /// Returns a dummy transaction id as an i32.
    /// Checkpoint records do not have a transaction id.
    fn get_transaction_number(&self) -> i32 {
        -1 // dummy value
    }

    /// Does nothing, because a checkpoint record contains no undo information.
    fn undo(&self, _transaction: &mut Transaction) {
        // Does nothing
    }
}

impl std::fmt::Display for CheckpointRecord {
    /// Formats the CheckpointRecord for display purposes.
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "<CHECKPOINT>")
    }
}
