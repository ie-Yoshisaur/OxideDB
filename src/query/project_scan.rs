use crate::materialize::sort_scan::SortScan;
use crate::query::constant::Constant;
use crate::query::scan::Scan;
use crate::record::record_id::RecordId;
use crate::record::table_scan::TableScan;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;

// no docs
// no comments
// no error handlings
// no variable name edit
pub struct ProjectScan {
    s: Arc<Mutex<dyn Scan>>,
    fieldlist: HashSet<String>,
}

impl ProjectScan {
    // Create a project scan having the specified
    // underlying scan and field list.
    pub fn new(s: Arc<Mutex<dyn Scan>>, fieldlist: HashSet<String>) -> Self {
        ProjectScan { s, fieldlist }
    }

    pub fn before_first(&mut self) {
        self.s.lock().unwrap().before_first();
    }

    pub fn next(&mut self) -> bool {
        self.s.lock().unwrap().next()
    }

    pub fn get_int(&self, fldname: &str) -> Option<i32> {
        if self.has_field(fldname) {
            self.s.lock().unwrap().get_int(fldname)
        } else {
            panic!("field {} not found.", fldname);
        }
    }

    pub fn get_string(&self, fldname: &str) -> Option<String> {
        if self.has_field(fldname) {
            self.s.lock().unwrap().get_string(fldname)
        } else {
            panic!("field {} not found.", fldname);
        }
    }

    pub fn get_value(&self, fldname: &str) -> Option<Constant> {
        if self.has_field(fldname) {
            self.s.lock().unwrap().get_value(fldname)
        } else {
            panic!("field {} not found.", fldname);
        }
    }

    pub fn has_field(&self, fldname: &str) -> bool {
        self.fieldlist.contains(fldname)
    }

    pub fn close(&mut self) {
        self.s.lock().unwrap().close();
    }
}

impl Scan for ProjectScan {
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
        unimplemented!()
    }

    fn set_int(&mut self, fldname: &str, val: i32) {
        unimplemented!()
    }

    fn set_string(&mut self, fldname: &str, val: String) {
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
