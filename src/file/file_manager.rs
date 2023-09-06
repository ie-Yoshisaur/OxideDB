use crate::file::block_id::BlockId;
use crate::file::err::FileManagerError;
use crate::file::page::Page;
use std::collections::HashMap;
use std::fs::{create_dir_all, read_dir, remove_file, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Mutex;

/// Manages files and blocks for a database.
///
/// This struct provides methods to read, write, and append blocks,
/// as well as to obtain metadata such as block size.
pub struct FileManager {
    db_directory: PathBuf,
    blocksize: usize,
    is_new: bool,
    open_files: Mutex<HashMap<String, File>>,
}

impl FileManager {
    /// Creates a new `FileManager`.
    ///
    /// # Arguments
    ///
    /// * `db_directory`: The directory where the database files are stored.
    /// * `blocksize`: The block size for reading and writing.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory cannot be created or read.
    pub fn new(db_directory: PathBuf, blocksize: usize) -> Result<FileManager, FileManagerError> {
        let is_new = !db_directory.exists();
        if is_new {
            create_dir_all(&db_directory).map_err(FileManagerError::Io)?;
        }

        let entries = read_dir(&db_directory)?;
        for entry in entries {
            if let Ok(entry) = entry {
                let path = entry.path();
                if path.is_file() {
                    if let Some(filename) = path.file_name() {
                        if filename.to_string_lossy().starts_with("temp") {
                            remove_file(path)?;
                        }
                    }
                }
            }
        }

        Ok(FileManager {
            db_directory,
            blocksize,
            is_new,
            open_files: Mutex::new(HashMap::new()),
        })
    }

    /// Reads a block from a file into a page.
    ///
    /// # Arguments
    ///
    /// * `block`: The identifier for the block to read.
    /// * `page`: The page to store the read data.
    ///
    /// # Errors
    ///
    /// Returns an error if the read operation fails.
    pub fn read(&self, block: &BlockId, page: &mut Page) -> Result<(), FileManagerError> {
        let mut file = self.get_file(&block.get_file_name())?;
        let file_offset = (block.get_block_number() * self.get_block_size() as u32) as u64;

        file.seek(SeekFrom::Start(file_offset))
            .map_err(FileManagerError::Io)?;

        let mut bytes = vec![0; self.get_block_size()];
        file.read(&mut bytes)?;
        page.write_bytes(0, &bytes)?;
        Ok(())
    }

    /// Writes a page to a block in a file.
    ///
    /// # Arguments
    ///
    /// * `block`: The identifier for the block to write.
    /// * `page`: The page containing the data to write.
    ///
    /// # Errors
    ///
    /// Returns an error if the write operation fails.
    pub fn write(&self, block: &BlockId, page: &mut Page) -> Result<(), FileManagerError> {
        let mut file = self.get_file(&block.get_file_name())?;
        let file_offset = (block.get_block_number() * self.get_block_size() as u32) as u64;

        file.seek(SeekFrom::Start(file_offset))
            .map_err(FileManagerError::Io)?;

        let bytes = page
            .read_bytes(0, self.get_block_size())
            .map_err(FileManagerError::Page)?;

        file.write_all(&bytes).map_err(FileManagerError::Io)?;

        Ok(())
    }

    /// Appends a new block to a file.
    ///
    /// # Arguments
    ///
    /// * `filename`: The name of the file to append to.
    ///
    /// # Errors
    ///
    /// Returns an error if the append operation fails.
    pub fn append(&self, filename: &str) -> Result<BlockId, FileManagerError> {
        let new_block_num = self.length(filename)?;
        let block = BlockId::new(filename.to_string(), new_block_num);
        let mut file = self.get_file(filename)?;
        let empty_block = vec![0; self.get_block_size()];
        file.seek(SeekFrom::Start(
            (block.get_block_number() * self.get_block_size() as u32) as u64,
        ))?;
        file.write_all(&empty_block)?;
        Ok(block)
    }

    /// Gets the number of blocks in a file.
    ///
    /// # Arguments
    ///
    /// * `filename`: The name of the file.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be accessed.
    pub fn length(&self, filename: &str) -> Result<u32, FileManagerError> {
        let file = self.get_file(filename)?;
        let len = file.metadata()?.len() as u32;
        Ok(len / self.get_block_size() as u32)
    }

    /// Checks if the database directory was newly created during the initialization of the FileManager.
    ///
    /// # Returns
    ///
    /// Returns `true` if the database directory is new, `false` otherwise.
    pub fn is_new(&self) -> bool {
        self.is_new
    }

    /// Gets the block size.
    ///
    /// # Returns
    ///
    /// Returns the block size used for reading and writing.
    pub fn get_block_size(&self) -> usize {
        self.blocksize
    }

    /// Gets or creates a file handle.
    ///
    /// # Arguments
    ///
    /// * `filename`: The name of the file.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be accessed.
    fn get_file(&self, filename: &str) -> Result<File, FileManagerError> {
        let mut open_files = self
            .open_files
            .lock()
            .map_err(|_| FileManagerError::MutexLockError)?;
        if let Some(file) = open_files.get(filename) {
            return Ok(file.try_clone().map_err(FileManagerError::Io)?);
        }

        let file_path = self.db_directory.join(filename);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file_path)
            .map_err(FileManagerError::Io)?;

        open_files.insert(
            filename.to_string(),
            file.try_clone().map_err(FileManagerError::Io)?,
        );
        Ok(file)
    }
}
