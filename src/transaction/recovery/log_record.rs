use crate::file::page::Page;
use crate::transaction::recovery::err::LogRecordError;
use crate::transaction::recovery::record::checkpoint_record::CheckpointRecord;
use crate::transaction::recovery::record::commit_record::CommitRecord;
use crate::transaction::recovery::record::rollback_record::RollbackRecord;
use crate::transaction::recovery::record::set_int_record::SetIntRecord;
use crate::transaction::recovery::record::set_string_record::SetStringRecord;
use crate::transaction::recovery::record::start_record::StartRecord;
use crate::transaction::transaction::Transaction;

/// `LogRecordType` enum defines the types of log records.
pub enum LogRecordType {
    Checkpoint = 0,
    Start = 1,
    Commit = 2,
    Rollback = 3,
    SetInt = 4,
    SetString = 5,
}

impl LogRecordType {
    /// Converts an i32 to a `LogRecordType`.
    ///
    /// # Arguments
    ///
    /// * `i` - The integer representing the log record type.
    ///
    /// # Returns
    ///
    /// Returns an `Option<LogRecordType>` based on the integer.
    pub fn get_record_type_from_i32(i: i32) -> Option<LogRecordType> {
        match i {
            0 => Some(LogRecordType::Checkpoint),
            1 => Some(LogRecordType::Start),
            2 => Some(LogRecordType::Commit),
            3 => Some(LogRecordType::Rollback),
            4 => Some(LogRecordType::SetInt),
            5 => Some(LogRecordType::SetString),
            _ => None,
        }
    }
}

/// The `LogRecord` trait defines the behavior for log records.
pub trait LogRecord {
    /// Returns the log record's type.
    fn get_log_record_type(&self) -> i32;

    /// Returns the log record's transaction id.
    fn get_transaction_number(&self) -> i32;

    /// Undoes the operation encoded by this log record.
    fn undo(&self, transaction: &mut Transaction);
}

impl dyn LogRecord {
    /// Creates a log record from a byte vector.
    ///
    /// # Arguments
    ///
    /// * `bytes` - The byte vector representing the log record.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing a boxed `LogRecord` or a `LogRecordError`.
    pub fn create_log_record(bytes: Vec<u8>) -> Result<Box<dyn LogRecord>, LogRecordError> {
        let mut page = Page::new_from_bytes(bytes);

        // Make sure to handle errors; here we unwrap
        let log_record_type = page.get_int(0).map_err(|e| LogRecordError::PageError(e))?;

        match log_record_type {
            log_record if log_record == LogRecordType::Checkpoint as i32 => {
                Ok(Box::new(CheckpointRecord::new()))
            }
            log_record if log_record == LogRecordType::Start as i32 => {
                Ok(Box::new(StartRecord::new(&mut page)?))
            }
            log_record if log_record == LogRecordType::Commit as i32 => {
                Ok(Box::new(CommitRecord::new(&mut page)?))
            }
            log_record if log_record == LogRecordType::Rollback as i32 => {
                Ok(Box::new(RollbackRecord::new(&mut page)?))
            }
            log_record if log_record == LogRecordType::SetInt as i32 => {
                Ok(Box::new(SetIntRecord::new(&mut page)?))
            }
            log_record if log_record == LogRecordType::SetString as i32 => {
                Ok(Box::new(SetStringRecord::new(&mut page)?))
            }
            _ => panic!("Unknown log record type"),
        }
    }
}
