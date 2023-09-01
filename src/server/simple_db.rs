use crate::file::file_manager::FileManager;
use std::path::PathBuf;

pub struct SimpleDB {
    block_size: usize,
    file_manager: FileManager,
}

impl SimpleDB {
    pub fn new_for_debug(db_directory: PathBuf, block_size: usize) -> SimpleDB {
        let file_manager = FileManager::new(db_directory, block_size).unwrap();
        SimpleDB {
            block_size,
            file_manager,
        }
    }

    // Accessor methods for debugging
    pub fn get_file_manager(&self) -> &FileManager {
        return &self.file_manager;
    }
}
