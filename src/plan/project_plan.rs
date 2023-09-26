use crate::plan::plan::Plan;
use crate::query::project_scan::ProjectScan;
use crate::query::scan::Scan;
use crate::record::schema::Schema;
use std::sync::{Arc, Mutex};

pub struct ProjectPlan {
    p: Arc<dyn Plan>,
    schema: Arc<Schema>,
}

impl ProjectPlan {
    // Creates a new project node in the query tree,
    // having the specified subquery and field list.
    pub fn new(p: Arc<dyn Plan>, fieldlist: Vec<String>) -> Self {
        let mut schema = Schema::new();
        for fldname in fieldlist {
            schema.add(fldname, p.schema().clone());
        }
        let schema = Arc::new(schema);
        Self { p, schema }
    }
}

impl Plan for ProjectPlan {
    // Creates a project scan for this query.
    fn open(&self) -> Arc<Mutex<dyn Scan>> {
        let s = self.p.open();
        Arc::new(Mutex::new(ProjectScan::new(s, self.schema.get_fields())))
    }

    // Estimates the number of block accesses in the projection,
    // which is the same as in the underlying query.
    fn blocks_accessed(&self) -> i32 {
        self.p.blocks_accessed()
    }

    // Estimates the number of output records in the projection,
    // which is the same as in the underlying query.
    fn records_output(&self) -> i32 {
        self.p.records_output()
    }

    // Estimates the number of distinct field values in the projection,
    // which is the same as in the underlying query.
    fn distinct_values(&self, fldname: &str) -> i32 {
        self.p.distinct_values(fldname)
    }

    // Returns the schema of the projection,
    // which is taken from the field list.
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}
