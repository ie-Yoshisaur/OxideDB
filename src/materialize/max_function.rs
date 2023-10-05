use crate::materialize::aggregation_function::AggregationFunction;
use crate::query::constant::Constant;
use crate::query::scan::Scan;
use std::sync::{Arc, Mutex};

pub struct MaxFunction {
    fldname: String,
    val: Option<Constant>,
}

impl MaxFunction {
    pub fn new(fldname: String) -> Self {
        Self { fldname, val: None }
    }
}

impl AggregationFunction for MaxFunction {
    fn process_first(&mut self, scan: Arc<Mutex<dyn Scan>>) {
        let scan_lock = scan.lock().unwrap();
        self.val = scan_lock.get_value(&self.fldname);
    }

    fn process_next(&mut self, scan: Arc<Mutex<dyn Scan>>) {
        let scan_lock = scan.lock().unwrap();
        let new_val = scan_lock.get_value(&self.fldname).unwrap();
        if let Some(current_val) = &self.val {
            if new_val > *current_val {
                self.val = Some(new_val);
            }
        }
    }

    fn field_name(&self) -> String {
        format!("maxof{}", self.fldname)
    }

    fn value(&self) -> Constant {
        self.val.clone().unwrap()
    }
}
