use crate::materialize::sort_scan::SortScan;
use crate::query::constant::Constant;
use crate::query::predicate::Predicate;
use crate::query::scan::Scan;
use crate::record::record_id::RecordId;
use crate::record::table_scan::TableScan;
use std::sync::{Arc, Mutex};

// no docs
// no comments
// no error handlings
// no variable name edit
pub struct SelectScan {
    s: Arc<Mutex<dyn Scan>>,
    pred: Predicate,
}

impl SelectScan {
    pub fn new(s: Arc<Mutex<dyn Scan>>, pred: Predicate) -> Self {
        SelectScan { s, pred }
    }

    pub fn before_first(&mut self) {
        self.s.lock().unwrap().before_first();
    }

    pub fn next(&mut self) -> bool {
        while self.s.lock().unwrap().next() {
            if self.pred.is_satisfied(self.s.clone()) {
                return true;
            }
        }
        false
    }

    pub fn get_int(&self, fldname: &str) -> Option<i32> {
        self.s.lock().unwrap().get_int(fldname)
    }

    pub fn get_string(&self, fldname: &str) -> Option<String> {
        self.s.lock().unwrap().get_string(fldname)
    }

    pub fn get_value(&self, fldname: &str) -> Option<Constant> {
        self.s.lock().unwrap().get_value(fldname)
    }

    pub fn has_field(&self, fldname: &str) -> bool {
        self.s.lock().unwrap().has_field(fldname)
    }

    pub fn close(&mut self) {
        self.s.lock().unwrap().close();
    }

    // UpdateScan methods

    pub fn set_int(&mut self, fldname: &str, val: i32) {
        self.s.lock().unwrap().set_int(fldname, val);
    }

    pub fn set_string(&mut self, fldname: &str, val: String) {
        self.s.lock().unwrap().set_string(fldname, val);
    }

    pub fn set_value(&mut self, fldname: &str, val: Constant) {
        self.s.lock().unwrap().set_value(fldname, val);
    }

    pub fn delete(&mut self) {
        self.s.lock().unwrap().delete();
    }

    pub fn insert(&mut self) {
        self.s.lock().unwrap().insert();
    }

    pub fn get_record_id(&self) -> RecordId {
        self.s.lock().unwrap().get_record_id()
    }

    pub fn move_to_record_id(&mut self, record_id: RecordId) {
        self.s.lock().unwrap().move_to_record_id(record_id);
    }
}

impl Scan for SelectScan {
    fn before_first(&mut self) {
        self.before_first();
    }

    fn next(&mut self) -> bool {
        self.next()
    }

    fn get_int(&self, fldname: &str) -> Option<i32> {
        self.get_int(fldname)
    }

    fn get_string(&self, fldname: &str) -> Option<String> {
        self.get_string(fldname)
    }

    fn get_value(&self, fldname: &str) -> Option<Constant> {
        self.get_value(fldname)
    }

    fn has_field(&self, fldname: &str) -> bool {
        self.has_field(fldname)
    }

    fn close(&mut self) {
        self.close();
    }

    // For Update
    fn set_value(&mut self, fldname: &str, val: Constant) {
        self.set_value(fldname, val);
    }

    fn set_int(&mut self, fldname: &str, val: i32) {
        self.set_int(fldname, val);
    }

    fn set_string(&mut self, fldname: &str, val: String) {
        self.set_string(fldname, val);
    }

    fn insert(&mut self) {
        self.insert();
    }

    fn delete(&mut self) {
        self.delete();
    }

    fn get_record_id(&self) -> RecordId {
        self.get_record_id()
    }

    fn move_to_record_id(&mut self, record_id: RecordId) {
        self.move_to_record_id(record_id);
    }

    fn as_sort_scan(&self) -> Option<SortScan> {
        None
    }

    fn as_table_scan(&self) -> Option<TableScan> {
        None
    }
}
