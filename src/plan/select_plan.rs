// no docs
// no comments
// no error handlings
// no variable name edit
use crate::plan::plan::Plan;
use crate::query::predicate::Predicate;
use crate::query::scan::Scan;
use crate::query::select_scan::SelectScan;
use crate::record::schema::Schema;
use std::sync::{Arc, Mutex};

pub struct SelectPlan {
    p: Arc<Mutex<dyn Plan>>,
    pred: Predicate,
}

impl SelectPlan {
    // Creates a new select node in the query tree,
    // having the specified subquery and predicate.
    pub fn new(p: Arc<Mutex<dyn Plan>>, pred: Predicate) -> Self {
        Self { p, pred }
    }
}

impl Plan for SelectPlan {
    // Creates a select scan for this query.
    fn open(&mut self) -> Arc<Mutex<dyn Scan>> {
        let s = self.p.lock().unwrap().open();
        Arc::new(Mutex::new(SelectScan::new(s, self.pred.clone())))
    }

    // Estimates the number of block accesses in the selection,
    // which is the same as in the underlying query.
    fn blocks_accessed(&self) -> i32 {
        self.p.lock().unwrap().blocks_accessed()
    }

    // Estimates the number of output records in the selection,
    // which is determined by the reduction factor of the predicate.
    fn records_output(&self) -> i32 {
        self.p.lock().unwrap().records_output()
            / self.pred.reduction_factor(&*self.p.lock().unwrap())
    }

    // Estimates the number of distinct field values in the projection.
    fn distinct_values(&self, fldname: &str) -> i32 {
        if let Some(_) = self.pred.equates_with_constant(fldname) {
            return 1;
        } else {
            if let Some(fldname2) = self.pred.equates_with_field(fldname) {
                return std::cmp::min(
                    self.p.lock().unwrap().distinct_values(fldname),
                    self.p.lock().unwrap().distinct_values(&fldname2),
                );
            } else {
                return self.p.lock().unwrap().distinct_values(fldname);
            }
        }
    }

    // Returns the schema of the selection,
    // which is the same as in the underlying query.
    fn schema(&self) -> Arc<Mutex<Schema>> {
        self.p.lock().unwrap().schema()
    }
}
