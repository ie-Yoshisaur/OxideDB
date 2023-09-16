use crate::file::page::Page;
use crate::log::log_manager::LogManager;
use crate::transaction::recovery::log_record::LogRecord;
use crate::transaction::recovery::log_record::LogRecordType::Checkpoint;
use crate::transaction::transaction::Transaction;
use std::mem::size_of;
use std::sync::{Arc, Mutex};

const I32_SIZE: usize = size_of::<i32>();

pub struct CheckpointRecord;

impl CheckpointRecord {
    pub fn new() -> Self {
        Self
    }

    // A static method to write a checkpoint record to the log.
    pub fn write_to_log(log_manager: Arc<Mutex<LogManager>>) -> i32 {
        let mut page = Page::new_from_blocksize(I32_SIZE);
        page.set_int(0, Checkpoint as i32).unwrap();
        log_manager
            .lock()
            .unwrap()
            .append(&page.read_bytes(0, I32_SIZE).unwrap())
            .unwrap()
    }
}

impl LogRecord for CheckpointRecord {
    fn get_log_record_type(&self) -> i32 {
        Checkpoint as i32
    }

    fn get_transaction_number(&self) -> i32 {
        -1 // dummy value
    }

    fn undo(&self, _transaction: &mut Transaction) {
        // Does nothing, because a checkpoint record contains no undo information.
    }
}

impl std::fmt::Display for CheckpointRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "<CHECKPOINT>")
    }
}
