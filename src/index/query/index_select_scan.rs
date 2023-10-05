use crate::index::index::Index;
use crate::materialize::sort_scan::SortScan;
use crate::query::constant::Constant;
use crate::query::scan::Scan;
use crate::record::record_id::RecordId;
use crate::record::table_scan::TableScan;
use std::sync::{Arc, Mutex};

pub struct IndexSelectScan {
    ts: Arc<Mutex<TableScan>>,
    idx: Arc<Mutex<dyn Index>>,
    val: Constant,
}

impl IndexSelectScan {
    pub fn new(ts: Arc<Mutex<TableScan>>, idx: Arc<Mutex<dyn Index>>, val: Constant) -> Self {
        let mut scan = Self { ts, idx, val };
        scan.before_first();
        scan
    }

    fn before_first(&mut self) {
        self.idx.lock().unwrap().before_first(self.val.clone());
    }

    fn next(&mut self) -> bool {
        let ok = self.idx.lock().unwrap().next();
        if ok {
            let rid = self.idx.lock().unwrap().get_data_rid().unwrap();
            self.ts.lock().unwrap().move_to_record_id(rid);
        }
        ok
    }

    fn get_int(&self, fldname: &str) -> Option<i32> {
        self.ts.lock().unwrap().get_int(fldname).ok()
    }

    fn get_string(&self, fldname: &str) -> Option<String> {
        self.ts.lock().unwrap().get_string(fldname).ok()
    }

    fn get_value(&self, fldname: &str) -> Option<Constant> {
        self.ts.lock().unwrap().get_value(fldname)
    }

    fn has_field(&self, fldname: &str) -> bool {
        self.ts.lock().unwrap().has_field(fldname)
    }

    fn close(&mut self) {
        self.idx.lock().unwrap().close();
        self.ts.lock().unwrap().close();
    }
}

impl Scan for IndexSelectScan {
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
