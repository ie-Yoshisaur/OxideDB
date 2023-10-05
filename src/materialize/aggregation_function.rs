use crate::query::constant::Constant;
use crate::query::scan::Scan;
use std::sync::{Arc, Mutex};

pub trait AggregationFunction {
    fn process_first(&mut self, scan: Arc<Mutex<dyn Scan>>);
    fn process_next(&mut self, scan: Arc<Mutex<dyn Scan>>);
    fn field_name(&self) -> String;
    fn value(&self) -> Constant;
}
