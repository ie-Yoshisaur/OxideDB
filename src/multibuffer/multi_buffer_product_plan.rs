use crate::materialize::materialize_plan::MaterializePlan;
use crate::materialize::temporary_table::TemporaryTable;
use crate::multibuffer::multi_buffer_product_scan::MultibufferProductScan;
use crate::plan::plan::Plan;
use crate::query::scan::Scan;
use crate::record::schema::Schema;
use crate::transaction::transaction::Transaction;
use std::sync::{Arc, Mutex};

pub struct MultibufferProductPlan {
    tx: Arc<Mutex<Transaction>>,
    lhs: Arc<Mutex<dyn Plan>>,
    rhs: Arc<Mutex<dyn Plan>>,
    schema: Arc<Mutex<Schema>>,
}

impl MultibufferProductPlan {
    pub fn new(
        tx: Arc<Mutex<Transaction>>,
        lhs: Arc<Mutex<dyn Plan>>,
        rhs: Arc<Mutex<dyn Plan>>,
    ) -> Self {
        let mut schema = Schema::new();
        schema.add_all(lhs.lock().unwrap().schema());
        schema.add_all(rhs.lock().unwrap().schema());
        let schema = Arc::new(Mutex::new(schema));

        Self {
            tx: tx.clone(),
            lhs: Arc::new(Mutex::new(MaterializePlan::new(tx.clone(), lhs))),
            rhs,
            schema,
        }
    }

    pub fn open(&mut self) -> Arc<Mutex<dyn Scan>> {
        let left_scan = self.lhs.lock().unwrap().open();
        let temp_table = self.copy_records_from(self.rhs.clone());
        Arc::new(Mutex::new(MultibufferProductScan::new(
            self.tx.clone(),
            left_scan,
            &temp_table.table_name(),
            temp_table.get_layout(),
        )))
    }

    pub fn blocks_accessed(&self) -> i32 {
        let avail = self.tx.lock().unwrap().available_buffers();
        let size = MaterializePlan::new(self.tx.clone(), self.rhs.clone()).blocks_accessed();
        let num_chunks = size / avail;
        self.rhs.lock().unwrap().blocks_accessed()
            + (self.lhs.lock().unwrap().blocks_accessed() * num_chunks)
    }

    pub fn records_output(&self) -> i32 {
        self.lhs.lock().unwrap().records_output() * self.rhs.lock().unwrap().records_output()
    }

    pub fn distinct_values(&self, fldname: &str) -> i32 {
        if self
            .lhs
            .lock()
            .unwrap()
            .schema()
            .lock()
            .unwrap()
            .has_field(fldname)
        {
            self.lhs.lock().unwrap().distinct_values(fldname)
        } else {
            self.rhs.lock().unwrap().distinct_values(fldname)
        }
    }

    pub fn schema(&self) -> Arc<Mutex<Schema>> {
        self.schema.clone()
    }

    fn copy_records_from(&self, p: Arc<Mutex<dyn Plan>>) -> TemporaryTable {
        let mut p_guard = p.lock().unwrap();
        let src = p_guard.open();
        let sch = p_guard.schema();
        drop(p_guard); // explicitly drop the lock guard to release the lock

        let temp_table = TemporaryTable::new(self.tx.clone(), sch.clone());
        let dest = temp_table.open();

        let mut src_guard = src.lock().unwrap();
        let sch_guard = sch.lock().unwrap();
        while src_guard.next() {
            dest.lock().unwrap().insert();
            for fldname in sch_guard.get_fields() {
                let val = src_guard.get_value(&fldname).unwrap(); // Assuming get_value returns an Option
                dest.lock().unwrap().set_value(&fldname, val);
            }
        }
        src_guard.close();
        dest.lock().unwrap().close();

        temp_table
    }
}

impl Plan for MultibufferProductPlan {
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
