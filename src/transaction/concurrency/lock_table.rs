use crate::file::block_id::BlockId;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub enum LockAbortError {
    Timeout,
}

pub struct LockTable {
    locks: HashMap<BlockId, i32>,
}

impl LockTable {
    pub fn new() -> Self {
        Self {
            locks: HashMap::new(),
        }
    }

    pub fn s_lock(&mut self, block: BlockId) -> Result<(), LockAbortError> {
        if self.has_other_s_locks(&block) {
            return Err(LockAbortError::Timeout);
        }
        let value = self.get_lock_value(&block);
        self.locks.insert(block, value + 1);
        Ok(())
    }

    pub fn x_lock(&mut self, block: BlockId) -> Result<(), LockAbortError> {
        if self.has_other_s_locks(&block) {
            return Err(LockAbortError::Timeout);
        }
        self.locks.insert(block, -1);
        Ok(())
    }

    pub fn unlock(&mut self, block: BlockId) {
        let value = self.get_lock_value(&block);
        if value > 1 {
            self.locks.insert(block, value - 1);
        } else {
            self.locks.remove(&block);
        }
    }

    pub fn has_other_s_locks(&self, block: &BlockId) -> bool {
        match self.locks.get(block) {
            Some(&value) => value > 1,
            None => false,
        }
    }

    fn get_lock_value(&self, block: &BlockId) -> i32 {
        match self.locks.get(block) {
            Some(&value) => value,
            None => 0,
        }
    }
}
