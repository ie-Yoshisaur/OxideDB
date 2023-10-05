use crate::materialize::{MergeJoinScan, SortPlan};
use crate::plan::plan::Plan;
use crate::query::scan::Scan;
use crate::record::schema::Schema;
use crate::transaction::transaction::Transaction;
use std::sync::{Arc, Mutex};

pub struct MergeJoinPlan {
    tx: Arc<Mutex<Transaction>>,
    p1: Arc<Mutex<dyn Plan>>,
    p2: Arc<Mutex<dyn Plan>>,
    fldname1: String,
    fldname2: String,
    sch: Arc<Schema>,
}

impl MergeJoinPlan {
    pub fn new(
        tx: Arc<Mutex<Transaction>>,
        p1: Arc<Mutex<dyn Plan>>,
        p2: Arc<Mutex<dyn Plan>>,
        fldname1: String,
        fldname2: String,
    ) -> Self {
        let sortlist1 = vec![fldname1.clone()];
        let sort_plan1 = SortPlan::new(tx.clone(), p1.clone(), sortlist1);

        let sortlist2 = vec![fldname2.clone()];
        let sort_plan2 = SortPlan::new(tx.clone(), p2.clone(), sortlist2);

        let mut sch = p1.lock().unwrap().schema();
        sch.add_all(&*p2.lock().unwrap().schema());

        Self {
            tx,
            p1: Arc::new(Mutex::new(sort_plan1)),
            p2: Arc::new(Mutex::new(sort_plan2)),
            fldname1,
            fldname2,
            sch: Arc::new(sch),
        }
    }

    pub fn open(&self) -> Arc<Mutex<dyn Scan>> {
        let s1 = self.p1.lock().unwrap().open();
        let s2 = self.p2.lock().unwrap().open();
        Arc::new(Mutex::new(MergeJoinScan::new(
            s1,
            s2,
            self.fldname1.clone(),
            self.fldname2.clone(),
        )))
    }

    pub fn blocks_accessed(&self) -> i32 {
        self.p1.lock().unwrap().blocks_accessed() + self.p2.lock().unwrap().blocks_accessed()
    }

    pub fn records_output(&self) -> i32 {
        let maxvals = i32::max(
            self.p1.lock().unwrap().distinct_values(&self.fldname1),
            self.p2.lock().unwrap().distinct_values(&self.fldname2),
        );
        (self.p1.lock().unwrap().records_output() * self.p2.lock().unwrap().records_output())
            / maxvals
    }

    pub fn distinct_values(&self, fldname: &str) -> i32 {
        if self.sch.has_field(fldname) {
            self.p1.lock().unwrap().distinct_values(fldname)
        } else {
            self.p2.lock().unwrap().distinct_values(fldname)
        }
    }

    pub fn schema(&self) -> Arc<Schema> {
        self.sch.clone()
    }
}

impl Plan for MergeJoinPlan {
    fn open(&self) -> Arc<Mutex<dyn Scan>> {
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
    fn schema(&self) -> Arc<Schema> {
        self.schema()
    }
}
