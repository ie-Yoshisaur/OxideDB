use crate::index::query::index_join_scan::IndexJoinScan;
use crate::metadata::index_information::IndexInformation;
use crate::plan::plan::Plan;
use crate::query::scan::Scan;
use crate::record::schema::Schema;
use std::sync::{Arc, Mutex};

pub struct IndexJoinPlan {
    p1: Arc<Mutex<dyn Plan>>,
    p2: Arc<Mutex<dyn Plan>>,
    ii: IndexInformation,
    joinfield: String,
    sch: Arc<Mutex<Schema>>,
}

impl IndexJoinPlan {
    pub fn new(
        p1: Arc<Mutex<dyn Plan>>,
        p2: Arc<Mutex<dyn Plan>>,
        ii: IndexInformation,
        joinfield: String,
    ) -> Self {
        let sch = p1.lock().unwrap().schema();
        sch.lock().unwrap().add_all(p2.lock().unwrap().schema());

        Self {
            p1,
            p2,
            ii,
            joinfield,
            sch,
        }
    }

    pub fn open(&mut self) -> Arc<Mutex<dyn Scan>> {
        let s = self.p1.lock().unwrap().open();
        let ts_befor_cast = self.p2.lock().unwrap().open();
        let ts = Arc::new(Mutex::new(
            ts_befor_cast.lock().unwrap().as_table_scan().unwrap(),
        ));
        let idx = self.ii.open();

        Arc::new(Mutex::new(IndexJoinScan::new(
            s,
            idx,
            self.joinfield.clone(),
            ts,
        )))
    }

    pub fn blocks_accessed(&self) -> i32 {
        self.p1.lock().unwrap().blocks_accessed()
            + (self.p1.lock().unwrap().records_output() * self.ii.blocks_accessed())
            + self.records_output()
    }

    pub fn records_output(&self) -> i32 {
        self.p1.lock().unwrap().records_output() * self.ii.records_output()
    }

    pub fn distinct_values(&self, fldname: &str) -> i32 {
        if self.sch.lock().unwrap().has_field(fldname) {
            self.p1.lock().unwrap().distinct_values(fldname)
        } else {
            self.p2.lock().unwrap().distinct_values(fldname)
        }
    }

    pub fn schema(&self) -> Arc<Mutex<Schema>> {
        self.sch.clone()
    }
}

impl Plan for IndexJoinPlan {
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
