use crate::metadata::err::ViewManagerError;
use crate::metadata::table_manager::TableManager;
use crate::metadata::table_manager::MAX_NAME;
use crate::record::schema::Schema;
use crate::record::table_scan::TableScan;
use crate::transaction::transaction::Transaction;
use std::sync::{Arc, Mutex};

const MAX_VIEW_DEFINITION: usize = 100;

/// `ViewManager` is responsible for managing the metadata associated with SQL views
/// stored in a database. It enables creating new views, as well as fetching their
/// SQL definitions. The manager operates in conjunction with a `TableManager` to
/// facilitate operations on a "view_catalog" table where the metadata is stored.
pub struct ViewManager {
    table_manager: Arc<Mutex<TableManager>>,
}

/// Manages views and their metadata.
impl ViewManager {
    /// Creates a new ViewManager instance.
    ///
    /// # Arguments
    ///
    /// * `is_new` - Indicates whether the database is new.
    /// * `table_manager` - Shared reference to a TableManager.
    /// * `transaction` - Transaction instance.
    ///
    /// # Returns
    ///
    /// Returns either a new ViewManager instance or an error.
    pub fn new(
        is_new: bool,
        table_manager: Arc<Mutex<TableManager>>,
        transaction: Arc<Mutex<Transaction>>,
    ) -> Result<Self, ViewManagerError> {
        if is_new {
            let mut schema = Schema::new();
            schema.add_string_field("view_name".to_string(), MAX_NAME);
            schema.add_string_field("view_definition".to_string(), MAX_VIEW_DEFINITION);
            let schema = Arc::new(schema);
            table_manager
                .lock()
                .unwrap()
                .create_table_from_table_manager("view_catalog", schema, transaction)
                .map_err(|e| ViewManagerError::TableManagerError(e))?;
        }
        Ok(Self { table_manager })
    }

    /// Creates a new view.
    ///
    /// # Arguments
    ///
    /// * `view_name` - The name of the new view.
    /// * `view_definition` - The SQL definition of the new view.
    /// * `transaction` - The transaction for creating the view.
    ///
    /// # Returns
    ///
    /// Returns either Ok(()) on successful view creation or an error.
    pub fn create_view(
        &self,
        view_name: &str,
        view_definition: &str,
        transaction: Arc<Mutex<Transaction>>,
    ) -> Result<(), ViewManagerError> {
        let layout = self
            .table_manager
            .lock()
            .unwrap()
            .get_layout("view_catalog", transaction.clone())
            .map_err(|e| ViewManagerError::TableManagerError(e))?;
        let mut table_scan = TableScan::new(transaction.clone(), "view_catalog", Arc::new(layout))
            .map_err(|e| ViewManagerError::TableScanError(e))?;
        table_scan
            .insert()
            .map_err(|e| ViewManagerError::TableScanError(e))?;
        table_scan
            .set_string("view_name", view_name.to_string())
            .map_err(|e| ViewManagerError::TableScanError(e))?;
        table_scan
            .set_string("view_definition", view_definition.to_string())
            .map_err(|e| ViewManagerError::TableScanError(e))?;
        table_scan.close();
        Ok(())
    }

    /// Retrieves the SQL definition of a specified view.
    ///
    /// # Arguments
    ///
    /// * `view_name` - The name of the view whose definition is to be retrieved.
    /// * `transaction` - The transaction.
    ///
    /// # Returns
    ///
    /// Returns either the SQL definition of the view or an error.
    pub fn get_view_definition(
        &self,
        view_name: &str,
        transaction: Arc<Mutex<Transaction>>,
    ) -> Result<Option<String>, ViewManagerError> {
        let layout = self
            .table_manager
            .lock()
            .unwrap()
            .get_layout("view_catalog", transaction.clone())
            .map_err(|e| ViewManagerError::TableManagerError(e))?;
        let mut table_scan = TableScan::new(transaction, "view_catalog", Arc::new(layout))
            .map_err(|e| ViewManagerError::TableScanError(e))?;
        let mut result = None;
        while table_scan
            .next()
            .map_err(|e| ViewManagerError::TableScanError(e))?
        {
            if table_scan.get_string("view_name").unwrap() == view_name {
                result = Some(
                    table_scan
                        .get_string("view_definition")
                        .map_err(|e| ViewManagerError::TableScanError(e))?,
                );
                break;
            }
        }
        table_scan.close();
        Ok(result)
    }
}
