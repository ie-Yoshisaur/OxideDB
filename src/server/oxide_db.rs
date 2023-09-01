use crate::file::file_manager::FileManager;
use std::path::PathBuf;

pub struct OxideDB {
    block_size: usize,
    file_manager: FileManager,
}

impl OxideDB {
    pub fn new_for_debug(db_directory: PathBuf, block_size: usize) -> OxideDB {
        let file_manager = FileManager::new(db_directory, block_size).unwrap();
        OxideDB {
            block_size,
            file_manager,
        }
    }

    // Accessor methods for debugging
    pub fn get_file_manager(&self) -> &FileManager {
        return &self.file_manager;
    }
}
