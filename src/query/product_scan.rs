use crate::query::constant::Constant;
use crate::query::scan::Scan;
use crate::record::record_id::RecordId;
use std::sync::{Arc, Mutex};

pub struct ProductScan {
    s1: Arc<Mutex<dyn Scan>>,
    s2: Arc<Mutex<dyn Scan>>,
}

impl ProductScan {
    pub fn new(s1: Arc<Mutex<dyn Scan>>, s2: Arc<Mutex<dyn Scan>>) -> Self {
        let mut ps = ProductScan { s1, s2 };
        ps.before_first();
        ps
    }

    pub fn before_first(&mut self) {
        self.s1.lock().unwrap().before_first();
        self.s1.lock().unwrap().next();
        self.s2.lock().unwrap().before_first();
    }

    pub fn next(&mut self) -> bool {
        if self.s2.lock().unwrap().next() {
            true
        } else {
            self.s2.lock().unwrap().before_first();
            self.s2.lock().unwrap().next() && self.s1.lock().unwrap().next()
        }
    }

    pub fn get_int(&self, fldname: &str) -> i32 {
        if self.s1.lock().unwrap().has_field(fldname) {
            self.s1.lock().unwrap().get_int(fldname).unwrap()
        } else {
            self.s2.lock().unwrap().get_int(fldname).unwrap()
        }
    }

    pub fn get_string(&self, fldname: &str) -> String {
        if self.s1.lock().unwrap().has_field(fldname) {
            self.s1.lock().unwrap().get_string(fldname).unwrap()
        } else {
            self.s2.lock().unwrap().get_string(fldname).unwrap()
        }
    }

    pub fn get_val(&self, fldname: &str) -> Constant {
        if self.s1.lock().unwrap().has_field(fldname) {
            self.s1.lock().unwrap().get_val(fldname).unwrap()
        } else {
            self.s2.lock().unwrap().get_val(fldname).unwrap()
        }
    }

    pub fn has_field(&self, fldname: &str) -> bool {
        self.s1.lock().unwrap().has_field(fldname) || self.s2.lock().unwrap().has_field(fldname)
    }

    pub fn close(&mut self) {
        self.s1.lock().unwrap().close();
        self.s2.lock().unwrap().close();
    }
}

impl Scan for ProductScan {
    fn before_first(&mut self) {
        self.before_first();
    }

    fn next(&mut self) -> bool {
        self.next()
    }

    fn get_int(&self, fldname: &str) -> Option<i32> {
        Some(self.get_int(fldname))
    }

    fn get_string(&self, fldname: &str) -> Option<String> {
        Some(self.get_string(fldname))
    }

    fn get_val(&self, fldname: &str) -> Option<Constant> {
        Some(self.get_val(fldname))
    }

    fn has_field(&self, fldname: &str) -> bool {
        self.has_field(fldname)
    }

    fn close(&mut self) {
        self.close();
    }

    // For Update
    fn set_val(&mut self, _fldname: &str, _val: Constant) {
        unimplemented!()
    }

    fn set_int(&mut self, _fldname: &str, _val: i32) {
        unimplemented!()
    }

    fn set_string(&mut self, _fldname: &str, _val: String) {
        unimplemented!()
    }

    fn insert(&mut self) {
        unimplemented!()
    }

    fn delete(&mut self) {
        unimplemented!()
    }

    fn get_rid(&self) -> RecordId {
        unimplemented!()
    }

    fn move_to_rid(&mut self, _rid: RecordId) {
        unimplemented!()
    }
}
