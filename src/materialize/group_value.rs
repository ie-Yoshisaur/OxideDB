use crate::query::constant::Constant;
use crate::query::scan::Scan;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

pub struct GroupValue {
    vals: HashMap<String, Constant>,
}

impl GroupValue {
    // Initialize a new GroupValue from a scan and a list of fields
    pub fn new(s: Arc<Mutex<dyn Scan>>, fields: &[String]) -> Self {
        let mut vals = HashMap::new();
        for fldname in fields {
            if let Some(val) = s.lock().unwrap().get_value(&fldname) {
                vals.insert(fldname.clone(), val);
            }
        }
        Self { vals }
    }

    // Get the value of the specified field in the group
    pub fn get_value(&self, fldname: &str) -> Option<Constant> {
        self.vals.get(fldname).cloned()
    }
}

impl PartialEq for GroupValue {
    fn eq(&self, other: &Self) -> bool {
        self.vals == other.vals
    }
}

impl Eq for GroupValue {}

impl Hash for GroupValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for value in self.vals.values() {
            value.hash(state);
        }
    }
}
