use crate::buffer::buffer_manager::BufferManager;
use crate::file::block_id::BlockId;
use crate::file::file_manager::FileManager;
use crate::log::log_manager::LogManager;
use crate::transaction::buffer_list::BufferList;
use crate::transaction::concurrency::concurrency_manager::ConcurrencyManager;
use crate::transaction::concurrency::lock_table::LockTable;
use crate::transaction::recovery::recovery_manager::RecoveryManager;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex};

static NEXT_TRANSACTION_NUM: AtomicI32 = AtomicI32::new(0);

#[derive(Clone)]
pub struct Transaction {
    file_manager: Arc<Mutex<FileManager>>,
    buffer_manager: Arc<Mutex<BufferManager>>,
    log_manager: Arc<Mutex<LogManager>>,
    transaction_number: i32,
    concurrency_manager: Arc<Mutex<ConcurrencyManager>>,
    buffer_list: Arc<Mutex<BufferList>>,
    recovery_manager: Arc<Mutex<RecoveryManager>>,
}

impl Transaction {
    pub fn new(
        file_manager: Arc<Mutex<FileManager>>,
        log_manager: Arc<Mutex<LogManager>>,
        buffer_manager: Arc<Mutex<BufferManager>>,
        lock_table: Arc<Mutex<LockTable>>,
    ) -> Self {
        let transaction_number = NEXT_TRANSACTION_NUM.fetch_add(1, Ordering::SeqCst);
        Self {
            file_manager,
            log_manager: log_manager.clone(),
            buffer_manager: buffer_manager.clone(),
            transaction_number,
            concurrency_manager: Arc::new(Mutex::new(ConcurrencyManager::new(lock_table))),
            buffer_list: Arc::new(Mutex::new(BufferList::new(buffer_manager.clone()))),
            recovery_manager: Arc::new(Mutex::new(RecoveryManager::new(
                transaction_number,
                log_manager,
                buffer_manager,
            ))),
        }
    }

    pub fn commit(&mut self) {
        self.recovery_manager.lock().unwrap().commit();
        println!("transaction {} committed", self.transaction_number);
        self.concurrency_manager.lock().unwrap().release();
        self.buffer_list.lock().unwrap().unpin_all();
    }

    pub fn rollback(&mut self) {
        let mut self_clone = self.clone();
        self.recovery_manager
            .lock()
            .unwrap()
            .rollback(&mut self_clone);
        println!("transaction {} rolled back", self.transaction_number);
        self.concurrency_manager.lock().unwrap().release();
    }

    pub fn recover(&mut self) {
        let mut self_clone = self.clone();
        self.buffer_manager
            .lock()
            .unwrap()
            .flush_all(self.transaction_number)
            .unwrap();
        self.recovery_manager
            .lock()
            .unwrap()
            .recover(&mut self_clone);
    }

    pub fn pin(&mut self, block: BlockId) {
        self.buffer_list.lock().unwrap().pin(block);
    }

    pub fn unpin(&mut self, block: BlockId) {
        self.buffer_list.lock().unwrap().unpin(block);
    }

    pub fn get_int(&mut self, block: BlockId, offset: i32) -> Option<i32> {
        self.concurrency_manager
            .lock()
            .unwrap()
            .s_lock(block.clone())
            .unwrap();
        let locked_buffer_list = self.buffer_list.lock().unwrap();
        if let Some(buffer) = locked_buffer_list.get_buffer(&block) {
            let mut locked_buffer = buffer.lock().unwrap();
            Some(
                locked_buffer
                    .get_contents()
                    .get_int(offset as usize)
                    .unwrap(),
            )
        } else {
            None
        }
    }

    pub fn get_string(&mut self, block: BlockId, offset: i32) -> Option<String> {
        self.concurrency_manager
            .lock()
            .unwrap()
            .s_lock(block.clone())
            .unwrap();
        let locked_buffer_list = self.buffer_list.lock().unwrap();
        if let Some(buffer) = locked_buffer_list.get_buffer(&block) {
            let mut locked_buffer = buffer.lock().unwrap();
            Some(
                locked_buffer
                    .get_contents()
                    .get_string(offset as usize)
                    .unwrap(),
            )
        } else {
            None
        }
    }

    pub fn set_int(
        &mut self,
        block: BlockId,
        offset: i32,
        value: i32,
        ok_to_log: bool,
    ) -> Option<()> {
        self.concurrency_manager
            .lock()
            .unwrap()
            .x_lock(block.clone())
            .unwrap();
        let locked_buffer_list = self.buffer_list.lock().unwrap();
        if let Some(buffer) = locked_buffer_list.get_buffer(&block) {
            let mut locked_buffer = buffer.lock().unwrap();
            let lsn = if ok_to_log {
                self.recovery_manager
                    .lock()
                    .unwrap()
                    .set_int(&mut locked_buffer, offset, value)
            } else {
                -1
            };
            locked_buffer
                .get_contents()
                .set_int(offset as usize, value)
                .unwrap();
            locked_buffer.set_modified(self.transaction_number, lsn);
            Some(())
        } else {
            None
        }
    }

    pub fn set_string(
        &mut self,
        block: BlockId,
        offset: i32,
        value: &String,
        ok_to_log: bool,
    ) -> Option<()> {
        self.concurrency_manager
            .lock()
            .unwrap()
            .x_lock(block.clone())
            .unwrap();
        let locked_buffer_list = self.buffer_list.lock().unwrap();
        if let Some(buffer) = locked_buffer_list.get_buffer(&block) {
            let mut locked_buffer = buffer.lock().unwrap();
            let lsn = if ok_to_log {
                self.recovery_manager.lock().unwrap().set_string(
                    &mut locked_buffer,
                    offset,
                    value.clone(),
                )
            } else {
                -1
            };
            locked_buffer
                .get_contents()
                .set_string(offset as usize, &value)
                .unwrap();
            locked_buffer.set_modified(self.transaction_number, lsn);
            Some(())
        } else {
            None
        }
    }

    pub fn size(&mut self, filename: &str) -> usize {
        let dummy_block = BlockId::new(filename.to_string(), -1);
        let mut locked_concurrency_manager = self.concurrency_manager.lock().unwrap();
        locked_concurrency_manager.s_lock(dummy_block).unwrap();
        let locked_file_manager = self.file_manager.lock().unwrap();
        locked_file_manager.length(filename).unwrap()
    }

    pub fn append(&mut self, filename: &str) -> BlockId {
        let dummy_block = BlockId::new(filename.to_string(), -1);
        let mut locked_concurrency_manager = self.concurrency_manager.lock().unwrap();
        locked_concurrency_manager.x_lock(dummy_block).unwrap();
        let locked_file_manager = self.file_manager.lock().unwrap();
        locked_file_manager.append(filename).unwrap()
    }

    pub fn block_size(&self) -> usize {
        let locked_file_manager = self.file_manager.lock().unwrap();
        locked_file_manager.get_block_size()
    }

    pub fn available_buffers(&self) -> i32 {
        let locked_buffer_manager = self.buffer_manager.lock().unwrap();
        let locked_available_buffers = locked_buffer_manager
            .get_number_available()
            .lock()
            .unwrap()
            .clone();
        locked_available_buffers as i32
    }
}
