// no docs
// no comments
// no error handlings
// no variable name edit
use crate::plan::plan::Plan;
use crate::query::product_scan::ProductScan;
use crate::query::scan::Scan;
use crate::record::schema::Schema;
use std::sync::{Arc, Mutex};

pub struct ProductPlan {
    p1: Arc<Mutex<dyn Plan>>,
    p2: Arc<Mutex<dyn Plan>>,
    schema: Arc<Mutex<Schema>>,
}

impl ProductPlan {
    // Creates a new product node in the query tree,
    // having the two specified subqueries.
    pub fn new(p1: Arc<Mutex<dyn Plan>>, p2: Arc<Mutex<dyn Plan>>) -> Self {
        let mut schema = Schema::new();
        schema.add_all(p1.lock().unwrap().schema());
        schema.add_all(p2.lock().unwrap().schema());
        let schema = Arc::new(Mutex::new(schema));
        Self { p1, p2, schema }
    }
}

impl Plan for ProductPlan {
    // Creates a product scan for this query.
    fn open(&mut self) -> Arc<Mutex<dyn Scan>> {
        let s1 = self.p1.lock().unwrap().open();
        let s2 = self.p2.lock().unwrap().open();
        Arc::new(Mutex::new(ProductScan::new(s1, s2)))
    }

    // Estimates the number of block accesses in the product.
    fn blocks_accessed(&self) -> i32 {
        self.p1.lock().unwrap().blocks_accessed()
            + (self.p1.lock().unwrap().records_output() * self.p2.lock().unwrap().blocks_accessed())
    }

    // Estimates the number of output records in the product.
    fn records_output(&self) -> i32 {
        self.p1.lock().unwrap().records_output() * self.p2.lock().unwrap().records_output()
    }

    // Estimates the distinct number of field values in the product.
    fn distinct_values(&self, fldname: &str) -> i32 {
        if self
            .p1
            .lock()
            .unwrap()
            .schema()
            .lock()
            .unwrap()
            .has_field(fldname)
        {
            self.p1.lock().unwrap().distinct_values(fldname)
        } else {
            self.p2.lock().unwrap().distinct_values(fldname)
        }
    }

    // Returns the schema of the product.
    fn schema(&self) -> Arc<Mutex<Schema>> {
        self.schema.clone()
    }
}
