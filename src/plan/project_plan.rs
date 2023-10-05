// no docs
// no comments
// no error handlings
// no variable name edit
use crate::plan::plan::Plan;
use crate::query::project_scan::ProjectScan;
use crate::query::scan::Scan;
use crate::record::schema::Schema;
use std::sync::{Arc, Mutex};

pub struct ProjectPlan {
    p: Arc<Mutex<dyn Plan>>,
    schema: Arc<Mutex<Schema>>,
}

impl ProjectPlan {
    // Creates a new project node in the query tree,
    // having the specified subquery and field list.
    pub fn new(p: Arc<Mutex<dyn Plan>>, fieldlist: Vec<String>) -> Self {
        let mut schema = Schema::new();
        for fldname in fieldlist {
            schema.add(fldname, &p.lock().unwrap().schema().clone().lock().unwrap());
        }
        let schema = Arc::new(Mutex::new(schema));
        Self { p, schema }
    }
}

impl Plan for ProjectPlan {
    // Creates a project scan for this query.
    fn open(&mut self) -> Arc<Mutex<dyn Scan>> {
        let s = self.p.lock().unwrap().open();
        Arc::new(Mutex::new(ProjectScan::new(
            s,
            self.schema.lock().unwrap().get_fields(),
        )))
    }

    // Estimates the number of block accesses in the projection,
    // which is the same as in the underlying query.
    fn blocks_accessed(&self) -> i32 {
        self.p.lock().unwrap().blocks_accessed()
    }

    // Estimates the number of output records in the projection,
    // which is the same as in the underlying query.
    fn records_output(&self) -> i32 {
        self.p.lock().unwrap().records_output()
    }

    // Estimates the number of distinct field values in the projection,
    // which is the same as in the underlying query.
    fn distinct_values(&self, fldname: &str) -> i32 {
        self.p.lock().unwrap().distinct_values(fldname)
    }

    // Returns the schema of the projection,
    // which is taken from the field list.
    fn schema(&self) -> Arc<Mutex<Schema>> {
        self.schema.clone()
    }
}
