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
    /// then the calling thread will be placed on a wait list
    /// until the lock is released.
    /// If the thread remains on the wait list for a certain
    /// amount of time (currently 10 seconds),
    /// then an exception is thrown.
    ///
    /// # Arguments
    ///
    /// * `block` - A reference to the disk block.
    ///
    /// # Returns
    ///
    /// * `Result<(), LockAbortError>` - Result of the operation.
    pub fn s_lock(&mut self, block: BlockId) -> Result<(), LockAbortError> {
        let value = self.get_lock_value(&block);
        self.locks.insert(block, value + 1);
        Ok(())
    }

    /// Grants an XLock on the specified block.
    /// If a lock of any type exists when the method is called,
    /// then the calling thread will be placed on a wait list
    /// until the locks are released.
    /// If the thread remains on the wait list for a certain
    /// amount of time (currently 10 seconds),
    /// then an exception is thrown.
    ///
    /// # Arguments
    ///
    /// * `block` - A reference to the disk block.
    ///
    /// # Returns
    ///
    /// * `Result<(), LockAbortError>` - Result of the operation.
    pub fn x_lock(&mut self, block: BlockId) -> Result<(), LockAbortError> {
        self.locks.insert(block, -1);
        Ok(())
    }

    /// Releases a lock on the specified block.
    /// If this lock is the last lock on that block,
    /// then the waiting transactions are notified.
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
    /// * `locks` - A reference to the lock table.
    ///
    /// # Returns
    ///
    /// * `bool` - Whether other SLocks exist or not.
    pub fn has_other_s_locks(&self, block: &BlockId) -> bool {
        let value = self.get_lock_value(block);
        value > 1
    }

    /// Checks if the specified block has an XLock.
    ///
    /// # Arguments
    ///
    /// * `block` - A reference to the disk block.
    /// * `locks` - A reference to the lock table.
    ///
    /// # Returns
    ///
    /// * `bool` - Whether an XLock exists or not.
    pub fn has_x_lock(&self, block: &BlockId) -> bool {
        let value = self.get_lock_value(block);
        value < 0
    }

    /// Gets the lock value for the specified block.
    ///
    /// # Arguments
    ///
    /// * `block` - A reference to the disk block.
    /// * `locks` - A reference to the lock table.
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
