use crate::metadata::err::StatisticsManagerError;
use crate::metadata::statistics_information::StatisticsInformation;
use crate::metadata::table_manager::TableManager;
use crate::record::{layout::Layout, table_scan::TableScan};
use crate::transaction::transaction::Transaction;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// The `StatisticsManager` is responsible for
/// managing statistical information about each table.
/// It refreshes the statistics periodically.
pub struct StatisticsManager {
    table_manager: Arc<Mutex<TableManager>>,
    table_statistics: Mutex<HashMap<String, StatisticsInformation>>,
    number_calls: Mutex<i32>,
}

impl StatisticsManager {
    /// Creates a new `StatisticsManager`.
    ///
    /// # Arguments
    ///
    /// * `table_manager` - An Arc-wrapped Mutex containing the `TableManager`.
    /// * `transaction` - An Arc-wrapped Mutex containing the current `Transaction`.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing a new `StatisticsManager`, or an error if creation fails.
    pub fn new(
        table_manager: Arc<Mutex<TableManager>>,
        transaction: Arc<Mutex<Transaction>>,
    ) -> Result<Self, StatisticsManagerError> {
        let statistics_manager = Self {
            table_manager,
            table_statistics: Mutex::new(HashMap::new()),
            number_calls: Mutex::new(0),
        };
        statistics_manager.refresh_statistics(transaction)?;
        Ok(statistics_manager)
    }

    /// Retrieves statistical information about a specific table.
    ///
    /// # Arguments
    ///
    /// * `table_name` - The name of the table.
    /// * `layout` - The `Layout` of the table.
    /// * `transaction` - An Arc-wrapped Mutex containing the current `Transaction`.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing the `StatisticsInformation` for the table, or an error if retrieval fails.
    pub fn get_statistics_info(
        &self,
        table_name: &str,
        layout: Layout,
        transaction: Arc<Mutex<Transaction>>,
    ) -> Result<StatisticsInformation, StatisticsManagerError> {
        let mut number_calls = self.number_calls.lock().unwrap();
        *number_calls += 1;
        if *number_calls > 100 {
            self.refresh_statistics(transaction.clone())?;
        }

        let mut table_statistics = self.table_statistics.lock().unwrap();
        match table_statistics.get(table_name) {
            Some(statistics_information) => Ok(statistics_information.clone()),
            None => {
                let statistics_information =
                    self.calculate_table_statistics(table_name, layout, transaction)?;
                table_statistics.insert(table_name.to_string(), statistics_information.clone());
                Ok(statistics_information)
            }
        }
    }

    /// Refreshes all table statistics.
    ///
    /// # Arguments
    ///
    /// * `transaction` - An Arc-wrapped Mutex containing the current `Transaction`.
    ///
    /// # Returns
    ///
    /// Returns a `Result` indicating success or failure.
    fn refresh_statistics(
        &self,
        transaction: Arc<Mutex<Transaction>>,
    ) -> Result<(), StatisticsManagerError> {
        let mut table_statistics = self.table_statistics.lock().unwrap();
        table_statistics.clear();
        *self.number_calls.lock().unwrap() = 0;

        let layout = self
            .table_manager
            .lock()
            .unwrap()
            .get_layout("table_catalog", transaction.clone())
            .map_err(|e| StatisticsManagerError::TableManagerError(e))?;
        let mut table_scan = TableScan::new(transaction.clone(), "table_catalog", Arc::new(layout))
            .map_err(|e| StatisticsManagerError::TableScanError(e))?;
        while table_scan
            .next()
            .map_err(|e| StatisticsManagerError::TableScanError(e))?
        {
            let table_name = table_scan.get_string("table_name").unwrap();
            let layout = self
                .table_manager
                .lock()
                .unwrap()
                .get_layout(&table_name, transaction.clone())
                .map_err(|e| StatisticsManagerError::TableManagerError(e))?;
            let statistics_information =
                self.calculate_table_statistics(&table_name, layout, transaction.clone())?;
            table_statistics.insert(table_name, statistics_information);
        }
        table_scan.close();
        Ok(())
    }

    /// Calculates statistics for a specific table.
    ///
    /// # Arguments
    ///
    /// * `table_name` - The name of the table.
    /// * `layout` - The `Layout` of the table.
    /// * `transaction` - An Arc-wrapped Mutex containing the current `Transaction`.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing the calculated `StatisticsInformation`, or an error if calculation fails.
    fn calculate_table_statistics(
        &self,
        table_name: &str,
        layout: Layout,
        transaction: Arc<Mutex<Transaction>>,
    ) -> Result<StatisticsInformation, StatisticsManagerError> {
        let mut number_records = 0;
        let mut number_blocks = 0;
        let mut table_scan = TableScan::new(transaction, table_name, Arc::new(layout))
            .map_err(|e| StatisticsManagerError::TableScanError(e))?;
        while table_scan
            .next()
            .map_err(|e| StatisticsManagerError::TableScanError(e))?
        {
            number_records += 1;
            number_blocks = table_scan.get_record_id().get_block_number() + 1;
        }
        table_scan.close();
        Ok(StatisticsInformation::new(number_blocks, number_records))
    }
}
