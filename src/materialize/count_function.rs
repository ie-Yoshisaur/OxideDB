use crate::materialize::aggregation_function::AggregationFunction;
use crate::query::constant::Constant;
use crate::query::scan::Scan;
use std::sync::{Arc, Mutex};

pub struct CountFunction {
    fldname: String,
    count: i32,
}

impl CountFunction {
    pub fn new(fldname: String) -> Self {
        Self { fldname, count: 0 }
    }
}

impl AggregationFunction for CountFunction {
    fn process_first(&mut self, _scan: Arc<Mutex<dyn Scan>>) {
        self.count = 1;
    }

    fn process_next(&mut self, _scan: Arc<Mutex<dyn Scan>>) {
        self.count += 1;
    }

    fn field_name(&self) -> String {
        format!("countof{}", self.fldname)
    }

    fn value(&self) -> Constant {
        Constant::Int(self.count)
    }
}
