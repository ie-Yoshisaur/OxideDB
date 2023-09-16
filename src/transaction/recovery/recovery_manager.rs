use crate::buffer::buffer::Buffer;
use crate::buffer::buffer_manager::BufferManager;
use crate::log::log_manager::LogManager;
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

pub struct RecoveryManager {
    log_manager: Arc<Mutex<LogManager>>,
    buffer_manager: Arc<Mutex<BufferManager>>,
    transaction_number: i32,
}

impl RecoveryManager {
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

    pub fn commit(&self) {
        self.buffer_manager
            .lock()
            .unwrap()
            .flush_all(self.transaction_number)
            .unwrap();
        let lsn = CommitRecord::write_to_log(self.log_manager.clone(), self.transaction_number);
        self.log_manager.lock().unwrap().flush_by_lsn(lsn).unwrap();
    }

    pub fn rollback(&self, transaction: &mut Transaction) {
        self.do_rollback(transaction);
        self.buffer_manager
            .lock()
            .unwrap()
            .flush_all(self.transaction_number)
            .unwrap();
        let lsn = RollbackRecord::write_to_log(self.log_manager.clone(), self.transaction_number);
        self.log_manager.lock().unwrap().flush_by_lsn(lsn).unwrap();
    }

    pub fn recover(&self, transaction: &mut Transaction) {
        self.do_recover(transaction);
        self.buffer_manager
            .lock()
            .unwrap()
            .flush_all(self.transaction_number)
            .unwrap();
        let lsn = CheckpointRecord::write_to_log(self.log_manager.clone());
        self.log_manager.lock().unwrap().flush_by_lsn(lsn).unwrap();
    }

    pub fn set_int(&self, buffer: &mut Buffer, offset: i32, _new_value: i32) -> i32 {
        let old_value = buffer.get_contents().get_int(offset as usize).unwrap();
        let block = buffer.get_block().unwrap();
        SetIntRecord::write_to_log(
            self.log_manager.clone(),
            self.transaction_number,
            block,
            offset,
            old_value,
        )
    }

    pub fn set_string(&self, buffer: &mut Buffer, offset: i32, _new_value: String) -> i32 {
        let old_value = buffer.get_contents().get_string(offset as usize).unwrap();
        let block = buffer.get_block().unwrap();
        SetStringRecord::write_to_log(
            self.log_manager.clone(),
            self.transaction_number,
            block,
            offset,
            &old_value,
        )
    }

    fn do_rollback(&self, transaction: &mut Transaction) {
        let iterator = self.log_manager.lock().unwrap().iterator().unwrap();
        for bytes in iterator {
            let record: Box<dyn LogRecord> = <dyn LogRecord>::create_log_record(bytes.unwrap());
            if record.get_transaction_number() == self.transaction_number {
                if let Some(LogRecordType::Start) =
                    LogRecordType::get_record_type_from_i32(record.get_log_record_type())
                {
                    return;
                }
                record.undo(transaction);
            }
        }
    }

    fn do_recover(&self, transaction: &mut Transaction) {
        let mut finished_transactions = HashSet::new();
        let iterator = self.log_manager.lock().unwrap().iterator().unwrap();
        for bytes in iterator {
            let record: Box<dyn LogRecord> = <dyn LogRecord>::create_log_record(bytes.unwrap());
            match LogRecordType::get_record_type_from_i32(record.get_log_record_type()) {
                Some(LogRecordType::Checkpoint) => return,
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
    }
}
