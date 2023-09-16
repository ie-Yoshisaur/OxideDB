use crate::file::block_id::BlockId;
use crate::transaction::concurrency::lock_table::LockAbortError;
use crate::transaction::concurrency::lock_table::LockTable;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug, PartialEq, Eq)]
enum LockType {
    Shared,
    Exclusive,
}

pub struct ConcurrencyManager {
    lock_table: Arc<Mutex<LockTable>>,
    locks: HashMap<BlockId, LockType>,
}

impl ConcurrencyManager {
    pub fn new(lock_table: Arc<Mutex<LockTable>>) -> Self {
        Self {
            lock_table,
            locks: HashMap::new(),
        }
    }

    pub fn s_lock(&mut self, block: BlockId) -> Result<(), LockAbortError> {
        if !self.locks.contains_key(&block) {
            let mut locked_lock_table = self.lock_table.lock().unwrap();
            locked_lock_table.s_lock(block.clone())?;
            self.locks.insert(block, LockType::Shared);
        }
        Ok(())
    }

    pub fn x_lock(&mut self, block: BlockId) -> Result<(), LockAbortError> {
        let start_time = Instant::now();
        let timeout = Duration::from_millis(10000); // 10 seconds

        loop {
            {
                let mut locked_lock_table = self.lock_table.lock().unwrap();
                if !locked_lock_table.has_other_s_locks(&block) {
                    locked_lock_table.x_lock(block.clone())?;
                    self.locks.insert(block.clone(), LockType::Exclusive);
                    return Ok(());
                }
            }

            if Instant::now().duration_since(start_time) > timeout {
                return Err(LockAbortError::Timeout);
            }

            thread::sleep(Duration::from_millis(100));
        }
    }

    pub fn release(&mut self) {
        for block in self.locks.keys().cloned().collect::<Vec<BlockId>>() {
            let mut locked_lock_table = self.lock_table.lock().unwrap();
            locked_lock_table.unlock(block);
        }
        self.locks.clear();
    }

    fn has_x_lock(&self, block: &BlockId) -> bool {
        match self.locks.get(block) {
            Some(LockType::Exclusive) => true,
            _ => false,
        }
    }
}
