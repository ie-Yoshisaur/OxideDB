use crate::file::block_id::BlockId;
use crate::transaction::concurrency::err::LockAbortError;
use std::collections::HashMap;

/// The `LockTable` struct manages methods to lock and unlock blocks.
/// If a transaction requests a lock that causes a conflict with an
/// existing lock, then that transaction will receive a `LockAbortError`.
/// There is only one lock table for all blocks.
pub struct LockTable {
    locks: HashMap<BlockId, i32>,
}

impl LockTable {
    /// Creates a new `LockTable`.
    pub fn new() -> Self {
        Self {
            locks: HashMap::new(),
        }
    }

    /// Grants an SLock on the specified block.
    /// If an XLock exists when the method is called,
    /// then a `LockAbortError::Timeout` is returned.
    ///
    /// # Arguments
    ///
    /// * `block` - A reference to the disk block.
    ///
    /// # Returns
    ///
    /// * `Result<(), LockAbortError>` - Result of the operation.
    pub fn s_lock(&mut self, block: BlockId) -> Result<(), LockAbortError> {
        if self.has_other_s_locks(&block) {
            return Err(LockAbortError::Timeout);
        }
        let value = self.get_lock_value(&block);
        self.locks.insert(block, value + 1);
        Ok(())
    }

    /// Grants an XLock on the specified block.
    /// If a lock of any type exists when the method is called,
    /// then a `LockAbortError::Timeout` is returned.
    ///
    /// # Arguments
    ///
    /// * `block` - A reference to the disk block.
    ///
    /// # Returns
    ///
    /// * `Result<(), LockAbortError>` - Result of the operation.
    pub fn x_lock(&mut self, block: BlockId) -> Result<(), LockAbortError> {
        if self.has_other_s_locks(&block) {
            return Err(LockAbortError::Timeout);
        }
        self.locks.insert(block, -1);
        Ok(())
    }

    /// Releases a lock on the specified block.
    ///
    /// # Arguments
    ///
    /// * `block` - A reference to the disk block.
    pub fn unlock(&mut self, block: BlockId) {
        let value = self.get_lock_value(&block);
        if value > 1 {
            self.locks.insert(block, value - 1);
        } else {
            self.locks.remove(&block);
        }
    }

    /// Checks if there are other SLocks on the specified block.
    ///
    /// # Arguments
    ///
    /// * `block` - A reference to the disk block.
    ///
    /// # Returns
    ///
    /// * `bool` - Whether other SLocks exist or not.
    pub fn has_other_s_locks(&self, block: &BlockId) -> bool {
        match self.locks.get(block) {
            Some(&value) => value > 1,
            None => false,
        }
    }

    /// Gets the lock value for the specified block.
    ///
    /// # Arguments
    ///
    /// * `block` - A reference to the disk block.
    ///
    /// # Returns
    ///
    /// * `i32` - The lock value.
    fn get_lock_value(&self, block: &BlockId) -> i32 {
        match self.locks.get(block) {
            Some(&value) => value,
            None => 0,
        }
    }
}
