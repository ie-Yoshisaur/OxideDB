use crate::buffer::buffer::Buffer;
use crate::buffer::buffer_manager::BufferManager;
use crate::file::block_id::BlockId;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Manages the transaction's currently-pinned buffers.
pub struct BufferList {
    buffers: HashMap<BlockId, Arc<Mutex<Buffer>>>,
    pins: Vec<BlockId>,
    buffer_manager: Arc<Mutex<BufferManager>>,
}

impl BufferList {
    /// Creates a new `BufferList` for the specified transaction.
    ///
    /// # Arguments
    ///
    /// * `buffer_manager` - The global buffer manager shared among all transactions.
    pub fn new(buffer_manager: Arc<Mutex<BufferManager>>) -> BufferList {
        BufferList {
            buffers: HashMap::new(),
            pins: Vec::new(),
            buffer_manager,
        }
    }

    /// Returns the buffer pinned to the specified block.
    /// Returns `None` if the transaction has not pinned the block.
    ///
    /// # Arguments
    ///
    /// * `block` - A reference to the disk block.
    ///
    /// # Returns
    ///
    /// * `Option<&Arc<Mutex<Buffer>>>` - The buffer pinned to that block, if any.
    pub fn get_buffer(&self, block: &BlockId) -> Option<&Arc<Mutex<Buffer>>> {
        self.buffers.get(block)
    }

    /// Pins the block and keeps track of the buffer internally.
    ///
    /// # Arguments
    ///
    /// * `block` - A reference to the disk block.
    pub fn pin(&mut self, block: BlockId) {
        let locked_buffer_manager = self.buffer_manager.lock().unwrap();
        match locked_buffer_manager.pin(block.clone()) {
            Ok(buffer) => {
                self.buffers.insert(block.clone(), buffer);
                self.pins.push(block);
            }
            Err(_) => {
                // Handle error here
            }
        }
    }

    /// Unpins the specified block.
    ///
    /// # Arguments
    ///
    /// * `block` - A reference to the disk block.
    pub fn unpin(&mut self, block: BlockId) {
        if let Some(buffer) = self.buffers.get(&block) {
            self.buffer_manager.lock().unwrap().unpin(buffer.clone());
        }
        self.pins.retain(|x| *x != block);
        if !self.pins.contains(&block) {
            self.buffers.remove(&block);
        }
    }

    /// Unpin any buffers still pinned by this transaction.
    pub fn unpin_all(&mut self) {
        for block in &self.pins {
            if let Some(buffer) = self.buffers.get(block) {
                self.buffer_manager.lock().unwrap().unpin(buffer.clone());
            }
        }
        self.buffers.clear();
        self.pins.clear();
    }
}
