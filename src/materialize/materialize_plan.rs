use crate::materialize::temporary_table::TemporaryTable;
use crate::plan::plan::Plan;
use crate::query::scan::Scan;
use crate::record::layout::Layout;
use crate::record::schema::Schema;
use crate::transaction::transaction::Transaction;
use std::sync::{Arc, Mutex};

pub struct MaterializePlan {
    src_plan: Arc<Mutex<dyn Plan>>,
    tx: Arc<Mutex<Transaction>>,
}

impl MaterializePlan {
    pub fn new(tx: Arc<Mutex<Transaction>>, src_plan: Arc<Mutex<dyn Plan>>) -> Self {
        Self { src_plan, tx }
    }

    pub fn open(&mut self) -> Arc<Mutex<dyn Scan>> {
        let sch = self.src_plan.lock().unwrap().schema();
        let temp = TemporaryTable::new(self.tx.clone(), sch.clone());
        let src = self.src_plan.lock().unwrap().open();
        let mut src = src.lock().unwrap();
        let dest = temp.open();

        while src.next() {
            dest.lock().unwrap().insert();
            for fldname in sch.lock().unwrap().get_fields() {
                let val = src.get_value(&fldname).unwrap();
                dest.lock().unwrap().set_value(&fldname, val);
            }
        }
        src.close();
        dest.lock().unwrap().before_first();
        dest
    }

    pub fn blocks_accessed(&self) -> i32 {
        let layout = Layout::new(self.src_plan.lock().unwrap().schema()).unwrap();
        let rpb = (self.tx.lock().unwrap().block_size() as f64) / (layout.get_slot_size() as f64);
        (self.src_plan.lock().unwrap().records_output() as f64 / rpb).ceil() as i32
    }

    pub fn records_output(&self) -> i32 {
        self.src_plan.lock().unwrap().records_output()
    }

    pub fn distinct_values(&self, fldname: &str) -> i32 {
        self.src_plan.lock().unwrap().distinct_values(fldname)
    }

    pub fn schema(&self) -> Arc<Mutex<Schema>> {
        self.src_plan.lock().unwrap().schema()
    }
}

impl Plan for MaterializePlan {
    fn open(&mut self) -> Arc<Mutex<dyn Scan>> {
        self.open()
    }
    fn blocks_accessed(&self) -> i32 {
        self.blocks_accessed()
    }
    fn records_output(&self) -> i32 {
        self.records_output()
    }
    fn distinct_values(&self, fldname: &str) -> i32 {
        self.distinct_values(fldname)
    }
    fn schema(&self) -> Arc<Mutex<Schema>> {
        self.schema()
    }
}
