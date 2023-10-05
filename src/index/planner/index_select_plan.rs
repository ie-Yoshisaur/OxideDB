use crate::index::query::index_select_scan::IndexSelectScan;
use crate::metadata::index_information::IndexInformation;
use crate::plan::plan::Plan;
use crate::query::constant::Constant;
use crate::query::scan::Scan;
use crate::record::schema::Schema;
use std::sync::{Arc, Mutex};

pub struct IndexSelectPlan {
    p: Arc<Mutex<dyn Plan>>,
    ii: Arc<Mutex<IndexInformation>>,
    val: Constant,
}

impl IndexSelectPlan {
    pub fn new(p: Arc<Mutex<dyn Plan>>, ii: Arc<Mutex<IndexInformation>>, val: Constant) -> Self {
        Self { p, ii, val }
    }

    pub fn open(&mut self) -> Arc<Mutex<dyn Scan>> {
        let ts = Arc::new(Mutex::new(
            self.p
                .lock()
                .unwrap()
                .open()
                .lock()
                .unwrap()
                .as_table_scan()
                .unwrap(),
        ));
        let idx = self.ii.lock().unwrap().open();
        Arc::new(Mutex::new(IndexSelectScan::new(ts, idx, self.val.clone())))
    }

    pub fn blocks_accessed(&self) -> i32 {
        self.ii.lock().unwrap().blocks_accessed() + self.records_output()
    }

    pub fn records_output(&self) -> i32 {
        self.ii.lock().unwrap().records_output()
    }

    pub fn distinct_values(&self, fldname: &str) -> i32 {
        self.ii.lock().unwrap().distinct_values(fldname)
    }

    pub fn schema(&self) -> Arc<Mutex<Schema>> {
        self.p.lock().unwrap().schema()
    }
}

impl Plan for IndexSelectPlan {
    fn open(&mut self) -> Arc<Mutex<dyn Scan>> {
        self.open()
    }

    fn blocks_accessed(&self) -> i32 {
        self.blocks_accessed()
    }

    fn records_output(&self) -> i32 {
        self.records_output()
    }

    fn distinct_values(&self, field_name: &str) -> i32 {
        self.distinct_values(field_name)
    }

    fn schema(&self) -> Arc<Mutex<Schema>> {
        self.schema()
    }
}
