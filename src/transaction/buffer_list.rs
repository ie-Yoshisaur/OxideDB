use crate::buffer::buffer::Buffer;
use crate::buffer::buffer_manager::BufferManager;
use crate::file::block_id::BlockId;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub struct BufferList {
    buffers: HashMap<BlockId, Arc<Mutex<Buffer>>>,
    pins: Vec<BlockId>,
    buffer_manager: Arc<Mutex<BufferManager>>,
}

impl BufferList {
    pub fn new(buffer_manager: Arc<Mutex<BufferManager>>) -> BufferList {
        BufferList {
            buffers: HashMap::new(),
            pins: Vec::new(),
            buffer_manager,
        }
    }

    /// Return the buffer pinned to the specified block.
    /// Returns `None` if the transaction has not pinned the block.
    pub fn get_buffer(&self, block: &BlockId) -> Option<&Arc<Mutex<Buffer>>> {
        self.buffers.get(block)
    }

    /// Pin the block and keep track of the buffer internally.
    pub fn pin(&mut self, block: BlockId) {
        let mut locked_buffer_manager = self.buffer_manager.lock().unwrap();
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

    /// Unpin the specified block.
    pub fn unpin(&mut self, block: BlockId) {
        if let Some(buffer) = self.buffers.get(&block) {
            self.buffer_manager
                .lock()
                .unwrap()
                .unpin(buffer.clone())
                .unwrap();
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
                self.buffer_manager
                    .lock()
                    .unwrap()
                    .unpin(buffer.clone())
                    .unwrap();
            }
        }
        self.buffers.clear();
        self.pins.clear();
    }
}
