use crate::query::scan::Scan;
use crate::record::schema::Schema;
use std::sync::{Arc, Mutex};

pub trait Plan {
    // Opens a scan corresponding to this plan.
    // The scan will be positioned before its first record.
    fn open(&self) -> Arc<Mutex<dyn Scan>>;

    // Returns an estimate of the number of block accesses
    // that will occur when the scan is read to completion.
    fn blocks_accessed(&self) -> i32;

    // Returns an estimate of the number of records
    // in the query's output table.
    fn records_output(&self) -> i32;

    // Returns an estimate of the number of distinct values
    // for the specified field in the query's output table.
    fn distinct_values(&self, fldname: &str) -> i32;

    // Returns the schema of the query.
    fn schema(&self) -> Arc<Schema>;
}
