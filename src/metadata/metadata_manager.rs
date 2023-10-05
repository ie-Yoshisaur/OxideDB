use crate::metadata::err::MetadataManagerError;
use crate::metadata::index_information::IndexInformation;
use crate::metadata::index_manager::IndexManager;
use crate::metadata::statistics_information::StatisticsInformation;
use crate::metadata::statistics_manager::StatisticsManager;
use crate::metadata::table_manager::TableManager;
use crate::metadata::view_manager::ViewManager;
use crate::record::layout::Layout;
use crate::record::schema::Schema;
use crate::transaction::transaction::Transaction;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

// no docs
// no comments
// no error handlings
// no variable name edit
pub struct MetadataManager {
    table_manager: Arc<Mutex<TableManager>>,
    view_manager: Arc<Mutex<ViewManager>>,
    statistics_manager: Arc<Mutex<StatisticsManager>>,
    index_manager: Arc<Mutex<IndexManager>>,
}

impl MetadataManager {
    pub fn new(
        is_new: bool,
        transaction: Arc<Mutex<Transaction>>,
    ) -> Result<Self, MetadataManagerError> {
        let table_manager = Arc::new(Mutex::new(
            TableManager::new(is_new, transaction.clone())
                .map_err(|e| MetadataManagerError::TableManagerError(e))?,
        ));
        let view_manager = Arc::new(Mutex::new(
            ViewManager::new(is_new, table_manager.clone(), transaction.clone())
                .map_err(|e| MetadataManagerError::ViewManagerError(e))?,
        ));
        let statistics_manager = Arc::new(Mutex::new(
            StatisticsManager::new(table_manager.clone(), transaction.clone())
                .map_err(|e| MetadataManagerError::StatisticsManagerError(e))?,
        ));
        let index_manager = Arc::new(Mutex::new(IndexManager::new(
            is_new,
            table_manager.clone(),
            statistics_manager.clone(),
            transaction,
        )));

        Ok(Self {
            table_manager,
            view_manager,
            statistics_manager,
            index_manager,
        })
    }

    pub fn create_table(
        &self,
        table_name: &str,
        schema: Arc<Mutex<Schema>>,
        transaction: Arc<Mutex<Transaction>>,
    ) -> Result<(), MetadataManagerError> {
        self.table_manager
            .lock()
            .unwrap()
            .create_table_from_table_manager(table_name, schema, transaction)
            .map_err(|e| MetadataManagerError::TableManagerError(e))?;
        Ok(())
    }

    pub fn get_layout(
        &self,
        table_name: &str,
        transaction: Arc<Mutex<Transaction>>,
    ) -> Result<Layout, MetadataManagerError> {
        self.table_manager
            .lock()
            .unwrap()
            .get_layout(table_name, transaction)
            .map_err(|e| MetadataManagerError::TableManagerError(e))
    }

    pub fn create_view(
        &self,
        view_name: &str,
        view_def: &str,
        transaction: Arc<Mutex<Transaction>>,
    ) -> Result<(), MetadataManagerError> {
        self.view_manager
            .lock()
            .unwrap()
            .create_view(view_name, view_def, transaction)
            .map_err(|e| MetadataManagerError::ViewManagerError(e))?;
        Ok(())
    }

    pub fn get_view_def(
        &self,
        view_name: &str,
        transaction: Arc<Mutex<Transaction>>,
    ) -> Result<Option<String>, MetadataManagerError> {
        self.view_manager
            .lock()
            .unwrap()
            .get_view_definition(view_name, transaction)
            .map_err(|e| MetadataManagerError::ViewManagerError(e))
    }

    pub fn create_index(
        &self,
        index_name: &str,
        table_name: &str,
        field_name: &str,
        transaction: Arc<Mutex<Transaction>>,
    ) {
        self.index_manager.lock().unwrap().create_index(
            index_name.to_string(),
            table_name.to_string(),
            field_name.to_string(),
            transaction,
        );
    }

    pub fn get_index_information(
        &self,
        table_name: &str,
        transaction: Arc<Mutex<Transaction>>,
    ) -> HashMap<String, IndexInformation> {
        self.index_manager
            .lock()
            .unwrap()
            .get_index_info(table_name.to_string(), transaction)
    }

    pub fn get_statistics_information(
        &self,
        table_name: &str,
        layout: Arc<Layout>,
        transaction: Arc<Mutex<Transaction>>,
    ) -> Result<StatisticsInformation, MetadataManagerError> {
        self.statistics_manager
            .lock()
            .unwrap()
            .get_statistics_information(table_name, layout, transaction)
            .map_err(|e| MetadataManagerError::StatisticsManagerError(e))
    }
}
