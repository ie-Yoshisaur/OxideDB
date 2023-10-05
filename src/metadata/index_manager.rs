use crate::metadata::index_information::IndexInformation;
use crate::metadata::statistics_manager::StatisticsManager;
use crate::metadata::table_manager::TableManager;
use crate::record::layout::Layout;
use crate::record::schema::Schema;
use crate::record::table_scan::TableScan;
use crate::transaction::transaction::Transaction;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

const MAX_NAME: usize = 64; // Adjust as per your needs

// no docs no comments
// no error handlings
// no variable name edit
pub struct IndexManager {
    layout: Arc<Layout>,
    table_manager: Arc<Mutex<TableManager>>,
    statistics_manager: Arc<Mutex<StatisticsManager>>,
}

impl IndexManager {
    pub fn new(
        is_new: bool,
        table_manager: Arc<Mutex<TableManager>>,
        statistics_manager: Arc<Mutex<StatisticsManager>>,
        transaction: Arc<Mutex<Transaction>>,
    ) -> Self {
        if is_new {
            let mut schema = Schema::new();
            schema.add_string_field("index_name".to_string(), MAX_NAME);
            schema.add_string_field("table_name".to_string(), MAX_NAME);
            schema.add_string_field("field_name".to_string(), MAX_NAME);
            let schema = Arc::new(Mutex::new(schema));

            table_manager
                .lock()
                .unwrap()
                .create_table_from_table_manager("index_catalog", schema, transaction.clone())
                .unwrap();
        }

        let layout = Arc::new(
            table_manager
                .lock()
                .unwrap()
                .get_layout("index_catalog", transaction)
                .unwrap(),
        );

        Self {
            layout,
            table_manager,
            statistics_manager,
        }
    }

    pub fn create_index(
        &self,
        idx_name: String,
        table_name: String,
        field_name: String,
        transaction: Arc<Mutex<Transaction>>,
    ) {
        let mut table_scan =
            TableScan::new(transaction.clone(), "index_catalog", self.layout.clone()).unwrap();
        table_scan.insert().unwrap();
        table_scan.set_string("index_name", idx_name).unwrap();
        table_scan.set_string("table_name", table_name).unwrap();
        table_scan.set_string("field_name", field_name).unwrap();
        table_scan.close();
    }

    pub fn get_index_info(
        &self,
        table_name: String,
        transaction: Arc<Mutex<Transaction>>,
    ) -> HashMap<String, IndexInformation> {
        let mut result: HashMap<String, IndexInformation> = HashMap::new();

        let mut table_scan =
            TableScan::new(transaction.clone(), "index_catalog", self.layout.clone()).unwrap();

        while table_scan.next().unwrap() {
            if table_scan.get_string("table_name").unwrap() == table_name {
                let idx_name = table_scan.get_string("index_name").unwrap();
                let field_name = table_scan.get_string("field_name").unwrap();
                let table_layout = Arc::new(
                    self.table_manager
                        .lock()
                        .unwrap()
                        .get_layout(&table_name, transaction.clone())
                        .unwrap(),
                );
                let table_si = self
                    .statistics_manager
                    .lock()
                    .unwrap()
                    .get_statistics_information(
                        &table_name,
                        table_layout.clone(),
                        transaction.clone(),
                    )
                    .unwrap();

                let index_info = IndexInformation::new(
                    idx_name,
                    field_name.clone(),
                    table_layout.get_schema(),
                    transaction.clone(),
                    table_si,
                );

                result.insert(field_name, index_info);
            }
        }

        table_scan.close();

        result
    }
}
