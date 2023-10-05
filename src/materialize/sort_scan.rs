use crate::materialize::record_comparator::RecordComparator;
use crate::materialize::temporary_table::TemporaryTable;
use crate::query::constant::Constant;
use crate::query::scan::Scan;
use crate::record::record_id::RecordId;
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub enum CurrentScan {
    S1,
    S2,
}

#[derive(Clone)]
pub struct SortScan {
    s1: Arc<Mutex<dyn Scan>>,
    s2: Option<Arc<Mutex<dyn Scan>>>,
    current_scan: Option<CurrentScan>,
    comp: RecordComparator,
    has_more1: bool,
    has_more2: bool,
    saved_position: Option<(RecordId, Option<RecordId>)>,
}

impl SortScan {
    pub fn new(runs: VecDeque<TemporaryTable>, comp: RecordComparator) -> Self {
        let s1 = runs[0].open();
        let has_more1 = s1.lock().unwrap().next();
        let (s2, has_more2) = if runs.len() > 1 {
            let s2 = runs[1].open();
            let has_more2 = s2.lock().unwrap().next();
            (Some(s2), has_more2)
        } else {
            (None, false)
        };

        Self {
            s1,
            s2,
            current_scan: None,
            comp,
            has_more1,
            has_more2,
            saved_position: None,
        }
    }

    pub fn before_first(&mut self) {
        self.current_scan = None;
        self.s1.lock().unwrap().before_first();
        self.has_more1 = self.s1.lock().unwrap().next();
        if let Some(s2) = &mut self.s2 {
            s2.lock().unwrap().before_first();
            self.has_more2 = s2.lock().unwrap().next();
        }
    }

    pub fn next(&mut self) -> bool {
        match &self.current_scan {
            Some(CurrentScan::S1) => {
                self.has_more1 = self.s1.lock().unwrap().next();
            }
            Some(CurrentScan::S2) => {
                if let Some(s2) = &mut self.s2 {
                    self.has_more2 = s2.lock().unwrap().next();
                }
            }
            None => {}
        }

        if !self.has_more1 && !self.has_more2 {
            return false;
        }

        self.current_scan = if self.has_more1 && self.has_more2 {
            match self
                .comp
                .compare(self.s1.clone(), self.s2.as_ref().unwrap().clone())
            {
                Ordering::Less => Some(CurrentScan::S1),
                _ => Some(CurrentScan::S2),
            }
        } else if self.has_more1 {
            Some(CurrentScan::S1)
        } else {
            Some(CurrentScan::S2)
        };

        true
    }

    pub fn close(&mut self) {
        self.s1.lock().unwrap().close();
        if let Some(s2) = &mut self.s2 {
            s2.lock().unwrap().close();
        }
    }

    pub fn get_value(&self, fldname: &str) -> Option<Constant> {
        match self.current_scan {
            Some(CurrentScan::S1) => self.s1.lock().unwrap().get_value(fldname),
            Some(CurrentScan::S2) => self.s2.as_ref().unwrap().lock().unwrap().get_value(fldname),
            None => None,
        }
    }

    pub fn get_int(&self, fldname: &str) -> Option<i32> {
        match self.current_scan {
            Some(CurrentScan::S1) => self.s1.lock().unwrap().get_int(fldname),
            Some(CurrentScan::S2) => self.s2.as_ref().unwrap().lock().unwrap().get_int(fldname),
            None => None,
        }
    }

    pub fn get_string(&self, fldname: &str) -> Option<String> {
        match self.current_scan {
            Some(CurrentScan::S1) => self.s1.lock().unwrap().get_string(fldname),
            Some(CurrentScan::S2) => self
                .s2
                .as_ref()
                .unwrap()
                .lock()
                .unwrap()
                .get_string(fldname),
            None => None,
        }
    }

    pub fn has_field(&self, fldname: &str) -> bool {
        match self.current_scan {
            Some(CurrentScan::S1) => self.s1.lock().unwrap().has_field(fldname),
            Some(CurrentScan::S2) => self.s2.as_ref().unwrap().lock().unwrap().has_field(fldname),
            None => false,
        }
    }

    pub fn save_position(&mut self) {
        let rid1 = self.s1.lock().unwrap().get_record_id();
        let rid2 = if let Some(s2) = self.s2.as_ref() {
            Some(s2.lock().unwrap().get_record_id())
        } else {
            None
        };
        self.saved_position = Some((rid1, rid2));
    }

    pub fn restore_position(&mut self) {
        if let Some((rid1, rid2)) = self.saved_position.take() {
            self.s1.lock().unwrap().move_to_record_id(rid1);
            if let Some(rid2) = rid2 {
                self.s2
                    .as_mut()
                    .unwrap()
                    .lock()
                    .unwrap()
                    .move_to_record_id(rid2);
            }
        }
    }
}

impl Scan for SortScan {
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
        Some(self.clone())
    }
}
