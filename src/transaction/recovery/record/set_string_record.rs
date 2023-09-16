use crate::file::block_id::BlockId;
use crate::file::page::Page;
use crate::log::log_manager::LogManager;
use crate::transaction::recovery::log_record::LogRecord;
use crate::transaction::recovery::log_record::LogRecordType::SetString;
use crate::transaction::transaction::Transaction;
use std::mem::size_of;
use std::sync::{Arc, Mutex};

const I32_SIZE: usize = size_of::<i32>();

pub struct SetStringRecord {
    transaction_number: i32,
    block: BlockId,
    offset: i32,
    value: String,
}

impl SetStringRecord {
    // Create a new SetStringRecord by reading the values from the page
    pub fn new(page: &mut Page) -> Self {
        let transaction_position = I32_SIZE;
        let transaction_number = page.get_int(transaction_position).unwrap();

        let filename_position = transaction_position + I32_SIZE;
        let filename = page.get_string(filename_position).unwrap();

        let block_position = filename_position + Page::max_length(filename.len());
        let block_number = page.get_int(block_position).unwrap();
        let block = BlockId::new(filename, block_number);

        let offset_position = block_position + I32_SIZE;
        let offset = page.get_int(offset_position).unwrap();

        let value_position = offset_position + I32_SIZE;
        let value = page.get_string(value_position).unwrap();

        Self {
            transaction_number,
            block,
            offset,
            value,
        }
    }

    // A static method to write a SetStringRecord to the log
    pub fn write_to_log(
        log_manager: Arc<Mutex<LogManager>>,
        transaction_number: i32,
        block: &BlockId,
        offset: i32,
        value: &str,
    ) -> i32 {
        let transaction_position = I32_SIZE;
        let filename_position = transaction_position + I32_SIZE;
        let block_position = filename_position + Page::max_length(block.get_file_name().len());
        let offset_position = block_position + I32_SIZE;
        let value_position = offset_position + I32_SIZE;
        let total_size = value_position + Page::max_length(value.len());
        let mut page = Page::new_from_blocksize(total_size as usize);
        page.set_int(0, SetString as i32).unwrap();
        page.set_int(transaction_position, transaction_number)
            .unwrap();
        page.set_string(filename_position, &block.get_file_name())
            .unwrap();
        page.set_int(block_position, block.get_block_number() as i32)
            .unwrap();
        page.set_int(offset_position, offset).unwrap();
        page.set_string(value_position, value).unwrap();
        log_manager
            .lock()
            .unwrap()
            .append(&page.read_bytes(0, total_size).unwrap())
            .unwrap()
    }
}

impl LogRecord for SetStringRecord {
    fn get_log_record_type(&self) -> i32 {
        SetString as i32
    }

    fn get_transaction_number(&self) -> i32 {
        self.transaction_number
    }

    fn undo(&self, transaction: &mut Transaction) {
        transaction.pin(self.block.clone());
        transaction.set_string(self.block.clone(), self.offset, &self.value, false); // don't log the undo!
        transaction.unpin(self.block.clone());
    }
}

impl std::fmt::Display for SetStringRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "<SETSTRING {} {} {} {}>",
            self.transaction_number, self.block, self.offset, self.value
        )
    }
}
