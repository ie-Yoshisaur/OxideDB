use crate::buffer::buffer_manager::BufferManager;
use crate::file::file_manager::FileManager;
use crate::log::log_manager::LogManager;
use crate::metadata::metadata_manager::MetadataManager;
use crate::plan::basic_query_planner::BasicQueryPlanner;
use crate::plan::basic_update_planner::BasicUpdatePlanner;
use crate::plan::planner::Planner;
use crate::transaction::concurrency::lock_table::LockTable;
use crate::transaction::transaction::Transaction;
use std::path::PathBuf;
use std::sync::Condvar;
use std::sync::{Arc, Mutex};

const LOG_FILE: &str = "oxidedb.log";
const BLOCK_SIZE: usize = 400;
const BUFFER_SIZE: usize = 24;

pub struct OxideDB {
    block_size: usize,
    file_manager: Arc<Mutex<FileManager>>,
    log_manager: Arc<Mutex<LogManager>>,
    buffer_manager: Arc<Mutex<BufferManager>>,
    lock_table: Arc<(Mutex<LockTable>, Condvar)>,
    metadata_manager: Option<Arc<MetadataManager>>,
    planner: Option<Arc<Mutex<Planner>>>,
}

impl OxideDB {
    pub fn new_from_parameters(
        db_directory: PathBuf,
        block_size: usize,
        buffer_size: usize,
    ) -> OxideDB {
        let file_manager = Arc::new(Mutex::new(
            FileManager::new(db_directory, block_size).unwrap(),
        ));

        let block_size = {
            let file_manager = file_manager.lock().unwrap();
            file_manager.get_block_size()
        };

        let log_manager = Arc::new(Mutex::new(
            LogManager::new(file_manager.clone(), LOG_FILE.to_string()).unwrap(),
        ));

        let buffer_manager = Arc::new(Mutex::new(
            BufferManager::new(file_manager.clone(), log_manager.clone(), buffer_size).unwrap(),
        ));

        let lock_table = Arc::new((Mutex::new(LockTable::new()), Condvar::new()));

        OxideDB {
            block_size,
            file_manager,
            log_manager,
            buffer_manager,
            lock_table,
            metadata_manager: None,
            planner: None,
        }
    }

    pub fn new(db_directory: PathBuf) -> Result<OxideDB, Box<dyn std::error::Error>> {
        let mut oxide_db = OxideDB::new_from_parameters(db_directory, BLOCK_SIZE, BUFFER_SIZE);
        let mut transaction = oxide_db.new_transaction();

        let is_new = {
            let file_manager = oxide_db.file_manager.lock().unwrap();
            file_manager.is_new()
        };

        if is_new {
            println!("creating new database");
        } else {
            println!("recovering existing database");
            transaction.recover()?;
        }

        let transaction = Arc::new(Mutex::new(transaction.clone()));

        let metadata_manager = Arc::new(MetadataManager::new(is_new, transaction.clone())?);
        let query_planner = Arc::new(BasicQueryPlanner::new(metadata_manager.clone()));
        let update_planner = Arc::new(BasicUpdatePlanner::new(metadata_manager));

        oxide_db.planner = Some(Arc::new(Mutex::new(Planner::new(
            query_planner,
            update_planner,
        ))));

        transaction.lock().unwrap().commit()?;

        Ok(oxide_db)
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

    pub fn get_lock_table(&self) -> &Arc<(Mutex<LockTable>, Condvar)> {
        &self.lock_table
    }

    pub fn get_metadata_manager(&mut self) -> &mut Option<Arc<MetadataManager>> {
        &mut self.metadata_manager
    }

    pub fn get_planner(&mut self) -> &mut Option<Arc<Mutex<Planner>>> {
        &mut self.planner
    }

    pub fn new_transaction(&self) -> Transaction {
        Transaction::new(
            self.file_manager.clone(),
            self.log_manager.clone(),
            self.buffer_manager.clone(),
            self.lock_table.clone(),
        )
        .unwrap()
    }
}
