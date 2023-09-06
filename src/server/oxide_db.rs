use crate::buffer::buffer_manager::BufferManager;
use crate::file::file_manager::FileManager;
use crate::log::log_manager::LogManager;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

const LOG_FILE: &str = "oxidedb.log";

pub struct OxideDB {
    block_size: usize,
    file_manager: Arc<Mutex<FileManager>>,
    log_manager: Arc<Mutex<LogManager>>,
    buffer_manager: Arc<Mutex<BufferManager>>,
}

impl OxideDB {
    pub fn new_for_debug(db_directory: PathBuf, block_size: usize, buffer_size: usize) -> OxideDB {
        let file_manager = Arc::new(Mutex::new(
            FileManager::new(db_directory, block_size).unwrap(),
        ));

        let block_size = {
            let fm = file_manager.lock().unwrap();
            fm.get_block_size()
        };

        let log_manager = Arc::new(Mutex::new(
            LogManager::new(Arc::clone(&file_manager), LOG_FILE.to_string()).unwrap(),
        ));

        let buffer_manager = Arc::new(Mutex::new(
            BufferManager::new(
                Arc::clone(&file_manager),
                Arc::clone(&log_manager),
                buffer_size,
            )
            .unwrap(),
        ));

        OxideDB {
            block_size,
            file_manager,
            log_manager,
            buffer_manager,
        }
    }

    pub fn get_file_manager(&self) -> &Arc<Mutex<FileManager>> {
        &self.file_manager
    }

    pub fn get_log_manager(&self) -> &Arc<Mutex<LogManager>> {
        &self.log_manager
    }

    pub fn get_buffer_manager(&self) -> &Arc<Mutex<BufferManager>> {
        &self.buffer_manager
    }
}
