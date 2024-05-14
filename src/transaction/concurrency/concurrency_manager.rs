use crate::file::block_id::BlockId;
use crate::transaction::concurrency::err::ConcurrencyError;
use crate::transaction::concurrency::lock_table::LockTable;
use std::collections::HashMap;
use std::sync::Condvar;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::time::Instant;

const MAX_TIME: Duration = Duration::from_secs(10);

#[derive(Debug, PartialEq, Eq)]
enum LockType {
    Shared,
    Exclusive,
}

/// The `ConcurrencyManager` struct manages the concurrency for the transaction.
/// Each transaction has its own concurrency manager.
/// The concurrency manager keeps track of which locks the transaction currently has,
/// and interacts with the global lock table as needed.
pub struct ConcurrencyManager {
    lock_table: Arc<(Mutex<LockTable>, Condvar)>,
    locks: HashMap<BlockId, LockType>,
}

impl ConcurrencyManager {
    /// Creates a new `ConcurrencyManager` for the specified transaction.
    ///
    /// # Arguments
    ///
    /// * `lock_table` - The global lock table shared among all transactions.
    pub fn new(lock_table: Arc<(Mutex<LockTable>, Condvar)>) -> Self {
        Self {
            lock_table,
            locks: HashMap::new(),
        }
    }

    /// Obtains an SLock on the block, if necessary.
    /// The method will ask the lock table for an SLock
    /// if the transaction currently has no locks on that block.
    ///
    /// # Arguments
    ///
    /// * `block` - A reference to the disk block.
    ///
    /// # Returns
    ///
    /// * `Result<(), ConcurrencyError>` - Result of the operation.
    pub fn s_lock(&mut self, block: BlockId) -> Result<(), ConcurrencyError> {
        let (lock, condvar) = &*self.lock_table;
        let mut lock_table = lock.lock().unwrap();
        let start_time = Instant::now();

        while lock_table.has_x_lock(&block) {
            let result = condvar.wait_timeout(lock_table, MAX_TIME).unwrap();
            lock_table = result.0;

            if self.waiting_too_long(start_time) {
                return Err(ConcurrencyError::Timeout);
            }
        }

        lock_table.s_lock(block.clone()).unwrap();
        self.locks.insert(block, LockType::Shared);
        Ok(())
    }

    /// Obtains an XLock on the block, if necessary.
    /// If the transaction does not have an XLock on that block,
    /// then the method first gets an SLock on that block
    /// (if necessary), and then upgrades it to an XLock.
    ///
    /// # Arguments
    ///
    /// * `block` - A reference to the disk block.
    ///
    /// # Returns
    ///
    /// * `Result<(), ConcurrencyError>` - Result of the operation.
    pub fn x_lock(&mut self, block: BlockId) -> Result<(), ConcurrencyError> {
        if !self.has_x_lock(&block) {
            let (lock, cvar) = &*self.lock_table;
            let mut lock_table = lock.lock().unwrap();
            lock_table.s_lock(block.clone()).unwrap();
            let start_time = Instant::now();

            while lock_table.has_other_s_locks(&block) {
                let result = cvar.wait_timeout(lock_table, MAX_TIME).unwrap();
                lock_table = result.0;

                if self.waiting_too_long(start_time) {
                    return Err(ConcurrencyError::Timeout);
                }
            }

            lock_table.x_lock(block.clone()).unwrap();
            self.locks.insert(block, LockType::Exclusive);
        }
        Ok(())
    }

    /// Releases all locks by asking the lock table to unlock each one.
    pub fn release(&mut self) {
        let (lock, cvar) = &*self.lock_table;
        let mut lock_table = lock.lock().unwrap();
        for block in self.locks.keys().cloned().collect::<Vec<BlockId>>() {
            lock_table.unlock(block);
        }
        self.locks.clear();
        cvar.notify_all();
    }

    /// Checks if the transaction has an XLock on the specified block.
    ///
    /// # Arguments
    ///
    /// * `block` - A reference to the disk block.
    ///
    /// # Returns
    ///
    /// * `bool` - Whether the transaction has an XLock or not.
    fn has_x_lock(&self, block: &BlockId) -> bool {
        match self.locks.get(block) {
            Some(LockType::Exclusive) => true,
            _ => false,
        }
    }

    fn waiting_too_long(&self, start_time: Instant) -> bool {
        start_time.elapsed() > MAX_TIME
    }
}
