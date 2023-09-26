use crate::query::constant::Constant;
use crate::query::predicate::Predicate;
use crate::query::scan::Scan;
use crate::record::record_id::RecordId;
use std::sync::{Arc, Mutex};

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

    pub fn get_val(&self, fldname: &str) -> Option<Constant> {
        self.s.lock().unwrap().get_val(fldname)
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

    pub fn set_val(&mut self, fldname: &str, val: Constant) {
        self.s.lock().unwrap().set_val(fldname, val);
    }

    pub fn delete(&mut self) {
        self.s.lock().unwrap().delete();
    }

    pub fn insert(&mut self) {
        self.s.lock().unwrap().insert();
    }

    pub fn get_rid(&self) -> RecordId {
        self.s.lock().unwrap().get_rid()
    }

    pub fn move_to_rid(&mut self, rid: RecordId) {
        self.s.lock().unwrap().move_to_rid(rid);
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

    fn get_val(&self, fldname: &str) -> Option<Constant> {
        self.get_val(fldname)
    }

    fn has_field(&self, fldname: &str) -> bool {
        self.has_field(fldname)
    }

    fn close(&mut self) {
        self.close();
    }

    // For Update
    fn set_val(&mut self, fldname: &str, val: Constant) {
        self.set_val(fldname, val);
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

    fn get_rid(&self) -> RecordId {
        self.get_rid()
    }

    fn move_to_rid(&mut self, rid: RecordId) {
        self.move_to_rid(rid);
    }
}
