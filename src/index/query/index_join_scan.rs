use crate::index::index::Index;
use crate::materialize::sort_scan::SortScan;
use crate::query::constant::Constant;
use crate::query::scan::Scan;
use crate::record::record_id::RecordId;
use crate::record::table_scan::TableScan;
use std::sync::{Arc, Mutex};

pub struct IndexJoinScan {
    lhs: Arc<Mutex<dyn Scan>>,
    idx: Arc<Mutex<dyn Index>>,
    joinfield: String,
    rhs: Arc<Mutex<TableScan>>,
}

impl IndexJoinScan {
    pub fn new(
        lhs: Arc<Mutex<dyn Scan>>,
        idx: Arc<Mutex<dyn Index>>,
        joinfield: String,
        rhs: Arc<Mutex<TableScan>>,
    ) -> Self {
        let mut scan = Self {
            lhs,
            idx,
            joinfield,
            rhs,
        };
        scan.before_first();
        scan
    }

    fn reset_index(&mut self) {
        let search_key = self.lhs.lock().unwrap().get_value(&self.joinfield).unwrap();
        self.idx.lock().unwrap().before_first(search_key);
    }

    fn before_first(&mut self) {
        self.lhs.lock().unwrap().before_first();
        self.lhs.lock().unwrap().next();
        self.reset_index();
    }

    fn next(&mut self) -> bool {
        loop {
            if self.idx.lock().unwrap().next() {
                let rid = self.idx.lock().unwrap().get_data_rid().unwrap();
                self.rhs.lock().unwrap().move_to_record_id(rid);
                return true;
            }
            if !self.lhs.lock().unwrap().next() {
                return false;
            }
            self.reset_index();
        }
    }

    fn get_int(&self, fldname: &str) -> Option<i32> {
        if self.rhs.lock().unwrap().has_field(fldname) {
            self.rhs.lock().unwrap().get_int(fldname).ok()
        } else {
            self.lhs.lock().unwrap().get_int(fldname)
        }
    }

    fn get_string(&self, fldname: &str) -> Option<String> {
        if self.rhs.lock().unwrap().has_field(fldname) {
            self.rhs.lock().unwrap().get_string(fldname).ok()
        } else {
            self.lhs.lock().unwrap().get_string(fldname)
        }
    }

    fn get_value(&self, fldname: &str) -> Option<Constant> {
        if self.rhs.lock().unwrap().has_field(fldname) {
            self.rhs.lock().unwrap().get_value(fldname)
        } else {
            self.lhs.lock().unwrap().get_value(fldname)
        }
    }

    fn has_field(&self, fldname: &str) -> bool {
        self.rhs.lock().unwrap().has_field(fldname) || self.lhs.lock().unwrap().has_field(fldname)
    }

    fn close(&mut self) {
        self.lhs.lock().unwrap().close();
        self.idx.lock().unwrap().close();
        self.rhs.lock().unwrap().close();
    }
}

impl Scan for IndexJoinScan {
    fn before_first(&mut self) {
        self.before_first()
    }
    fn next(&mut self) -> bool {
        self.next()
    }
    fn get_int(&self, field_name: &str) -> Option<i32> {
        self.get_int(field_name)
    }
    fn get_string(&self, field_name: &str) -> Option<String> {
        self.get_string(field_name)
    }
    fn get_value(&self, field_name: &str) -> Option<Constant> {
        self.get_value(field_name)
    }
    fn has_field(&self, field_name: &str) -> bool {
        self.has_field(field_name)
    }
    fn close(&mut self) {
        self.close()
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
