use crate::materialize::sort_scan::SortScan;
use crate::query::constant::Constant;
use crate::query::scan::Scan;
use crate::record::record_id::RecordId;
use crate::record::table_scan::TableScan;
use std::cmp::Ordering;
use std::sync::{Arc, Mutex};

pub struct MergeJoinScan {
    s1: Arc<Mutex<dyn Scan>>,
    s2: Arc<Mutex<SortScan>>,
    fldname1: String,
    fldname2: String,
    joinval: Option<Constant>,
}

impl MergeJoinScan {
    pub fn new(
        s1: Arc<Mutex<dyn Scan>>,
        s2: Arc<Mutex<SortScan>>,
        fldname1: String,
        fldname2: String,
    ) -> Self {
        Self {
            s1,
            s2,
            fldname1,
            fldname2,
            joinval: None,
        }
    }

    fn before_first(&mut self) {
        self.s1.lock().unwrap().before_first();
        self.s2.lock().unwrap().before_first();
    }

    fn next(&mut self) -> bool {
        let has_more2 = self.s2.lock().unwrap().next();
        if has_more2 && self.s2.lock().unwrap().get_value(&self.fldname2) == self.joinval {
            return true;
        }

        let has_more1 = self.s1.lock().unwrap().next();
        if has_more1 && self.s1.lock().unwrap().get_value(&self.fldname1) == self.joinval {
            self.s2.lock().unwrap().restore_position();
            return true;
        }

        let mut has_more1 = has_more1;
        let mut has_more2 = has_more2;
        while has_more1 && has_more2 {
            let v1 = self.s1.lock().unwrap().get_value(&self.fldname1);
            let v2 = self.s2.lock().unwrap().get_value(&self.fldname2);
            match v1.cmp(&v2) {
                Ordering::Less => has_more1 = self.s1.lock().unwrap().next(),
                Ordering::Greater => has_more2 = self.s2.lock().unwrap().next(),
                Ordering::Equal => {
                    self.s2.lock().unwrap().save_position();
                    self.joinval = v2;
                    return true;
                }
            }
        }
        false
    }

    fn close(&mut self) {
        self.s1.lock().unwrap().close();
        self.s2.lock().unwrap().close();
    }

    fn get_int(&self, fldname: &str) -> Option<i32> {
        if self.s1.lock().unwrap().has_field(fldname) {
            self.s1.lock().unwrap().get_int(fldname)
        } else {
            self.s2.lock().unwrap().get_int(fldname)
        }
    }

    fn get_string(&self, fldname: &str) -> Option<String> {
        if self.s1.lock().unwrap().has_field(fldname) {
            self.s1.lock().unwrap().get_string(fldname)
        } else {
            self.s2.lock().unwrap().get_string(fldname)
        }
    }

    fn get_value(&self, fldname: &str) -> Option<Constant> {
        if self.s1.lock().unwrap().has_field(fldname) {
            self.s1.lock().unwrap().get_value(fldname)
        } else {
            self.s2.lock().unwrap().get_value(fldname)
        }
    }

    fn has_field(&self, fldname: &str) -> bool {
        self.s1.lock().unwrap().has_field(fldname) || self.s2.lock().unwrap().has_field(fldname)
    }
}

impl Scan for MergeJoinScan {
    fn before_first(&mut self) {
        self.before_first();
    }

    fn next(&mut self) -> bool {
        self.next()
    }

    fn close(&mut self) {
        self.close();
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

    fn set_value(&mut self, field_name: &str, value: Constant) {
        unimplemented!()
    }

    fn set_int(&mut self, field_name: &str, value: i32) {
        unimplemented!()
    }

    fn set_string(&mut self, field_name: &str, value: String) {
        unimplemented!()
    }

    fn insert(&mut self) {
        unimplemented!()
    }

    fn delete(&mut self) {
        unimplemented!()
    }

    fn get_record_id(&self) -> RecordId {
        unimplemented!()
    }

    fn move_to_record_id(&mut self, record_id: RecordId) {
        unimplemented!()
    }

    fn as_sort_scan(&self) -> Option<SortScan> {
        None
    }

    fn as_table_scan(&self) -> Option<TableScan> {
        None
    }
}
