use crate::metadata::metadata_manager::MetadataManager;
use crate::metadata::statistics_information::StatisticsInformation;
use crate::plan::plan::Plan;
use crate::query::scan::Scan;
use crate::record::layout::Layout;
use crate::record::schema::Schema;
use crate::record::table_scan::TableScan;
use crate::transaction::transaction::Transaction;
use std::sync::{Arc, Mutex};

pub struct TablePlan {
    tblname: String,
    tx: Arc<Mutex<Transaction>>,
    layout: Arc<Layout>,
    si: StatisticsInformation,
}

impl TablePlan {
    // Creates a leaf node in the query tree corresponding
    // to the specified table.
    pub fn new(tx: Arc<Mutex<Transaction>>, tblname: String, md: Arc<MetadataManager>) -> Self {
        let layout = md.get_layout(&tblname, tx.clone()).unwrap();
        let si = md
            .get_stat_info(&tblname, layout.clone(), tx.clone())
            .unwrap();
        let layout = Arc::new(layout);
        Self {
            tblname,
            tx,
            layout,
            si,
        }
    }
}

impl Plan for TablePlan {
    // Creates a table scan for this query.
    fn open(&self) -> Arc<Mutex<dyn Scan>> {
        Arc::new(Mutex::new(
            TableScan::new(self.tx.clone(), &self.tblname, self.layout.clone()).unwrap(),
        ))
    }

    // Estimates the number of block accesses for the table,
    // which is obtainable from the statistics manager.
    fn blocks_accessed(&self) -> i32 {
        self.si.blocks_accessed()
    }

    // Estimates the number of records in the table,
    // which is obtainable from the statistics manager.
    fn records_output(&self) -> i32 {
        self.si.records_output()
    }

    // Estimates the number of distinct field values in the table,
    // which is obtainable from the statistics manager.
    fn distinct_values(&self, fldname: &str) -> i32 {
        self.si.distinct_values(fldname)
    }

    // Determines the schema of the table,
    // which is obtainable from the catalog manager.
    fn schema(&self) -> Arc<Schema> {
        self.layout.get_schema()
    }
}
