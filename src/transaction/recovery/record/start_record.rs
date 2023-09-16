use crate::file::page::Page;
use crate::log::log_manager::LogManager;
use crate::transaction::recovery::log_record::LogRecord;
use crate::transaction::recovery::log_record::LogRecordType::Start;
use crate::transaction::transaction::Transaction;
use std::mem::size_of;
use std::sync::{Arc, Mutex};

const I32_SIZE: usize = size_of::<i32>();

pub struct StartRecord {
    transaction_number: i32,
}

impl StartRecord {
    pub fn new(page: &mut Page) -> Self {
        let transaction_position = I32_SIZE;
        let transaction_number = page.get_int(transaction_position).unwrap();

        Self { transaction_number }
    }

    // A static method to write a start record to the log
    pub fn write_to_log(log_manager: Arc<Mutex<LogManager>>, txnum: i32) -> i32 {
        let mut page = Page::new_from_blocksize(2 * I32_SIZE);
        page.set_int(0, Start as i32).unwrap();
        page.set_int(I32_SIZE, txnum).unwrap();
        log_manager
            .lock()
            .unwrap()
            .append(&page.read_bytes(0, 2 * I32_SIZE).unwrap())
            .unwrap()
    }
}

impl LogRecord for StartRecord {
    fn get_log_record_type(&self) -> i32 {
        Start as i32
    }

    fn get_transaction_number(&self) -> i32 {
        self.transaction_number
    }

    fn undo(&self, _transaction: &mut Transaction) {
        // Does nothing, because a start record contains no undo information
    }
}

impl std::fmt::Display for StartRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "<START {}>", self.transaction_number)
    }
}
