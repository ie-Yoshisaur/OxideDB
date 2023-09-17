use crate::file::block_id::BlockId;
use crate::file::page::Page;
use crate::log::log_manager::LogManager;
use crate::transaction::recovery::err::LogRecordError;
use crate::transaction::recovery::log_record::LogRecord;
use crate::transaction::recovery::log_record::LogRecordType::SetString;
use crate::transaction::transaction::Transaction;
use std::mem::size_of;
use std::sync::{Arc, Mutex};

const I32_SIZE: usize = size_of::<i32>();

/// Represents a SetString log record.
/// This log record contains the SETSTRING operator,
/// followed by the transaction id, the filename, number,
/// and offset of the modified block, and the previous
/// string value at that offset.
pub struct SetStringRecord {
    transaction_number: i32,
    block: BlockId,
    offset: i32,
    value: String,
}

impl SetStringRecord {
    // Create a new SetStringRecord by reading the values from the page
    pub fn new(page: &mut Page) -> Result<Self, LogRecordError> {
        let transaction_position = I32_SIZE;
        let transaction_number = page
            .get_int(transaction_position)
            .map_err(|e| LogRecordError::PageError(e))?;

        let filename_position = transaction_position + I32_SIZE;
        let filename = page
            .get_string(filename_position)
            .map_err(|e| LogRecordError::PageError(e))?;

        let block_position = filename_position + Page::max_length(filename.len());
        let block_number = page
            .get_int(block_position)
            .map_err(|e| LogRecordError::PageError(e))?;
        let block = BlockId::new(filename, block_number);

        let offset_position = block_position + I32_SIZE;
        let offset = page
            .get_int(offset_position)
            .map_err(|e| LogRecordError::PageError(e))?;

        let value_position = offset_position + I32_SIZE;
        let value = page
            .get_string(value_position)
            .map_err(|e| LogRecordError::PageError(e))?;

        Ok(Self {
            transaction_number,
            block,
            offset,
            value,
        })
    }

    /// A static method to write a SetStringRecord to the log.
    ///
    /// # Returns
    ///
    /// Returns the LSN of the last log value.
    pub fn write_to_log(
        log_manager: Arc<Mutex<LogManager>>,
        transaction_number: i32,
        block: &BlockId,
        offset: i32,
        value: &str,
    ) -> Result<i32, LogRecordError> {
        let transaction_position = I32_SIZE;
        let filename_position = transaction_position + I32_SIZE;
        let block_position = filename_position + Page::max_length(block.get_file_name().len());
        let offset_position = block_position + I32_SIZE;
        let value_position = offset_position + I32_SIZE;
        let total_size = value_position + Page::max_length(value.len());
        let mut page = Page::new_from_blocksize(total_size as usize);
        page.set_int(0, SetString as i32)
            .map_err(|e| LogRecordError::PageError(e))?;
        page.set_int(transaction_position, transaction_number)
            .map_err(|e| LogRecordError::PageError(e))?;
        page.set_string(filename_position, &block.get_file_name())
            .map_err(|e| LogRecordError::PageError(e))?;
        page.set_int(block_position, block.get_block_number() as i32)
            .map_err(|e| LogRecordError::PageError(e))?;
        page.set_int(offset_position, offset)
            .map_err(|e| LogRecordError::PageError(e))?;
        page.set_string(value_position, value)
            .map_err(|e| LogRecordError::PageError(e))?;
        log_manager
            .lock()
            .unwrap()
            .append(&page.read_bytes(0, total_size).unwrap())
            .map_err(|e| LogRecordError::LogError(e))
    }
}

impl LogRecord for SetStringRecord {
    /// Returns the log record's type as an i32.
    fn get_log_record_type(&self) -> i32 {
        SetString as i32
    }

    /// Returns the log record's transaction id as an i32.
    fn get_transaction_number(&self) -> i32 {
        self.transaction_number
    }

    /// Undoes the operation encoded by this log record.
    /// The method pins a buffer to the specified block,
    /// calls set_string to restore the saved value,
    /// and unpins the buffer.
    fn undo(&self, transaction: &mut Transaction) {
        transaction.pin(self.block.clone());
        transaction.set_string(self.block.clone(), self.offset, &self.value, false); // don't log the undo!
        transaction.unpin(self.block.clone());
    }
}

impl std::fmt::Display for SetStringRecord {
    /// Formats the SetStringRecord for display purposes.
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "<SETSTRING {} {} {} {}>",
            self.transaction_number, self.block, self.offset, self.value
        )
    }
}
