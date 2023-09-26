use crate::plan::plan::Plan;
use crate::query::product_scan::ProductScan;
use crate::query::scan::Scan;
use crate::record::schema::Schema;
use std::sync::{Arc, Mutex};

pub struct ProductPlan {
    p1: Arc<dyn Plan>,
    p2: Arc<dyn Plan>,
    schema: Arc<Schema>,
}

impl ProductPlan {
    // Creates a new product node in the query tree,
    // having the two specified subqueries.
    pub fn new(p1: Arc<dyn Plan>, p2: Arc<dyn Plan>) -> Self {
        let mut schema = Schema::new();
        schema.add_all(p1.schema());
        schema.add_all(p2.schema());
        let schema = Arc::new(schema);
        Self { p1, p2, schema }
    }
}

impl Plan for ProductPlan {
    // Creates a product scan for this query.
    fn open(&self) -> Arc<Mutex<dyn Scan>> {
        let s1 = self.p1.open();
        let s2 = self.p2.open();
        Arc::new(Mutex::new(ProductScan::new(s1, s2)))
    }

    // Estimates the number of block accesses in the product.
    fn blocks_accessed(&self) -> i32 {
        self.p1.blocks_accessed() + (self.p1.records_output() * self.p2.blocks_accessed())
    }

    // Estimates the number of output records in the product.
    fn records_output(&self) -> i32 {
        self.p1.records_output() * self.p2.records_output()
    }

    // Estimates the distinct number of field values in the product.
    fn distinct_values(&self, fldname: &str) -> i32 {
        if self.p1.schema().has_field(fldname) {
            self.p1.distinct_values(fldname)
        } else {
            self.p2.distinct_values(fldname)
        }
    }

    // Returns the schema of the product.
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}
