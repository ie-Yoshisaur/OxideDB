use crate::query::scan::Scan;
use std::cmp::Ordering;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct RecordComparator {
    fields: Vec<String>,
}

impl RecordComparator {
    pub fn new(fields: Vec<String>) -> Self {
        Self { fields }
    }

    pub fn compare(&self, s1: Arc<Mutex<dyn Scan>>, s2: Arc<Mutex<dyn Scan>>) -> Ordering {
        for fldname in &self.fields {
            let val1 = s1.lock().unwrap().get_value(fldname);
            let val2 = s2.lock().unwrap().get_value(fldname);
            match val1.cmp(&val2) {
                Ordering::Less => return Ordering::Less,
                Ordering::Greater => return Ordering::Greater,
                Ordering::Equal => continue,
            }
        }
        Ordering::Equal
    }
}
