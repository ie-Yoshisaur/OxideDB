use crate::file::block_id::BlockId;
use crate::transaction::concurrency::err::LockAbortError;
use std::collections::HashMap;
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

const MAX_TIME: Duration = Duration::from_secs(10);

/// The `LockTable` struct manages methods to lock and unlock blocks.
/// If a transaction requests a lock that causes a conflict with an
/// existing lock, then that transaction will receive a `LockAbortError`.
/// There is only one lock table for all blocks.
pub struct LockTable {
    locks: Mutex<HashMap<BlockId, i32>>,
    cvar: Condvar,
}

impl LockTable {
    /// Creates a new `LockTable`.
    pub fn new() -> Self {
        Self {
            locks: Mutex::new(HashMap::new()),
            cvar: Condvar::new(),
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
    pub fn s_lock(&self, block: BlockId) -> Result<(), LockAbortError> {
        let start_time = Instant::now();
        let mut locks = self.locks.lock().unwrap();
        while self.has_xlock(&block, &locks) && !self.waiting_too_long(start_time) {
            locks = self.cvar.wait_timeout(locks, MAX_TIME).unwrap().0;
        }
        if self.has_xlock(&block, &locks) {
            return Err(LockAbortError::Timeout);
        }
        let value = self.get_lock_value(&block, &locks);
        locks.insert(block, value + 1);
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
    pub fn x_lock(&self, block: BlockId) -> Result<(), LockAbortError> {
        let start_time = Instant::now();
        let mut locks = self.locks.lock().unwrap();
        while self.has_other_s_locks(&block, &locks) && !self.waiting_too_long(start_time) {
            locks = self.cvar.wait_timeout(locks, MAX_TIME).unwrap().0;
        }
        if self.has_other_s_locks(&block, &locks) {
            return Err(LockAbortError::Timeout);
        }
        locks.insert(block, -1);
        Ok(())
    }

    /// Releases a lock on the specified block.
    /// If this lock is the last lock on that block,
    /// then the waiting transactions are notified.
    ///
    /// # Arguments
    ///
    /// * `block` - A reference to the disk block.
    pub fn unlock(&self, block: BlockId) {
        let mut locks = self.locks.lock().unwrap();
        let value = self.get_lock_value(&block, &locks);
        if value > 1 {
            locks.insert(block, value - 1);
        } else {
            locks.remove(&block);
            self.cvar.notify_all();
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
    fn has_other_s_locks(&self, block: &BlockId, locks: &HashMap<BlockId, i32>) -> bool {
        match locks.get(block) {
            Some(&value) => value > 1,
            None => false,
        }
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
    fn has_xlock(&self, block: &BlockId, locks: &HashMap<BlockId, i32>) -> bool {
        self.get_lock_value(block, locks) < 0
    }

    /// Checks if the specified block has an SLock.
    ///
    /// # Arguments
    /// * start_time - The time when the transaction started.
    ///
    /// # Returns
    /// * `bool` - Whether the transaction has waited too long or not.
    fn waiting_too_long(&self, start_time: Instant) -> bool {
        start_time.elapsed() > MAX_TIME
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
    fn get_lock_value(&self, block: &BlockId, locks: &HashMap<BlockId, i32>) -> i32 {
        match locks.get(block) {
            Some(&value) => value,
            None => 0,
        }
    }
}
