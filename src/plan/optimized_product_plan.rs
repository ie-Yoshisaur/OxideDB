// no docs
// no comments
// no error handlings
// no variable name edit
use crate::plan::plan::Plan;
use crate::plan::product_plan::ProductPlan;
use crate::query::scan::Scan;
use crate::record::schema::Schema;
use std::sync::{Arc, Mutex};

pub struct OptimizedProductPlan {
    bestplan: Arc<Mutex<dyn Plan>>,
}

impl OptimizedProductPlan {
    pub fn new(p1: Arc<Mutex<dyn Plan>>, p2: Arc<Mutex<dyn Plan>>) -> Self {
        let prod1 = Arc::new(Mutex::new(ProductPlan::new(p1.clone(), p2.clone())));
        let prod2 = Arc::new(Mutex::new(ProductPlan::new(p2.clone(), p1.clone())));
        let b1 = prod1.lock().unwrap().blocks_accessed();
        let b2 = prod2.lock().unwrap().blocks_accessed();
        let bestplan = if b1 < b2 { prod1 } else { prod2 };
        OptimizedProductPlan { bestplan }
    }
}

impl Plan for OptimizedProductPlan {
    fn open(&mut self) -> Arc<Mutex<dyn Scan>> {
        self.bestplan.lock().unwrap().open()
    }

    fn blocks_accessed(&self) -> i32 {
        self.bestplan.lock().unwrap().blocks_accessed()
    }

    fn records_output(&self) -> i32 {
        self.bestplan.lock().unwrap().records_output()
    }

    fn distinct_values(&self, fldname: &str) -> i32 {
        self.bestplan.lock().unwrap().distinct_values(fldname)
    }

    fn schema(&self) -> Arc<Mutex<Schema>> {
        self.bestplan.lock().unwrap().schema().clone()
    }
}
