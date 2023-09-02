use crate::file::file_manager::FileManager;
use crate::log::log_manager::LogManager;
use std::path::PathBuf;
use std::sync::Arc;

const LOG_FILE: &str = "oxidedb.log";

pub struct OxideDB {
    block_size: usize,
    file_manager: Arc<FileManager>,
    log_manager: LogManager,
}

impl OxideDB {
    pub fn new_for_debug(db_directory: PathBuf, block_size: usize) -> OxideDB {
        let file_manager = Arc::new(FileManager::new(db_directory, block_size).unwrap());
        let log_manager = LogManager::new(Arc::clone(&file_manager), LOG_FILE.to_string()).unwrap();

        OxideDB {
            block_size: file_manager.get_block_size(),
            file_manager,
            log_manager,
        }
    }

    pub fn get_file_manager(&self) -> &FileManager {
        &self.file_manager
    }

    pub fn get_log_manager(&mut self) -> &mut LogManager {
        &mut self.log_manager
    }
}
