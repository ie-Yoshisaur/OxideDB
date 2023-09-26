use crate::plan::plan::Plan;
use crate::plan::product_plan::ProductPlan;
use crate::query::scan::Scan;
use crate::record::schema::Schema;
use std::sync::{Arc, Mutex};

pub struct OptimizedProductPlan {
    bestplan: Arc<dyn Plan>,
}

impl OptimizedProductPlan {
    pub fn new(p1: Arc<dyn Plan>, p2: Arc<dyn Plan>) -> Self {
        let prod1 = Arc::new(ProductPlan::new(p1.clone(), p2.clone()));
        let prod2 = Arc::new(ProductPlan::new(p2.clone(), p1.clone()));
        let b1 = prod1.blocks_accessed();
        let b2 = prod2.blocks_accessed();
        let bestplan = if b1 < b2 { prod1 } else { prod2 };
        OptimizedProductPlan { bestplan }
    }
}

impl Plan for OptimizedProductPlan {
    fn open(&self) -> Arc<Mutex<dyn Scan>> {
        self.bestplan.open()
    }

    fn blocks_accessed(&self) -> i32 {
        self.bestplan.blocks_accessed()
    }

    fn records_output(&self) -> i32 {
        self.bestplan.records_output()
    }

    fn distinct_values(&self, fldname: &str) -> i32 {
        self.bestplan.distinct_values(fldname)
    }

    fn schema(&self) -> Arc<Schema> {
        self.bestplan.schema().clone()
    }
}
