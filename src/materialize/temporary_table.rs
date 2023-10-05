use crate::query::scan::Scan;
use crate::record::layout::Layout;
use crate::record::schema::Schema;
use crate::record::table_scan::TableScan;
use crate::transaction::transaction::Transaction;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

static NEXT_TABLE_NUM: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone)]
pub struct TemporaryTable {
    tx: Arc<Mutex<Transaction>>,
    tblname: String,
    layout: Arc<Layout>,
}

impl TemporaryTable {
    pub fn new(tx: Arc<Mutex<Transaction>>, sch: Arc<Mutex<Schema>>) -> Self {
        let tblname = next_table_name();
        let layout = Arc::new(Layout::new(sch).unwrap());
        Self {
            tx,
            tblname,
            layout,
        }
    }

    pub fn open(&self) -> Arc<Mutex<dyn Scan>> {
        Arc::new(Mutex::new(
            TableScan::new(self.tx.clone(), &self.tblname, self.layout.clone()).unwrap(),
        ))
    }

    pub fn table_name(&self) -> &str {
        &self.tblname
    }

    // Return the table's metadata.
    pub fn get_layout(&self) -> &Layout {
        &self.layout
    }
}

fn next_table_name() -> String {
    let next_table_num = NEXT_TABLE_NUM.fetch_add(1, Ordering::SeqCst);
    format!("temp{}", next_table_num)
}
