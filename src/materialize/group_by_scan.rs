use crate::materialize::aggregation_function::AggregationFunction;
use crate::materialize::group_value::GroupValue;
use crate::materialize::sort_scan::SortScan;
use crate::query::constant::Constant;
use crate::query::scan::Scan;
use crate::record::record_id::RecordId;
use crate::record::table_scan::TableScan;
use std::sync::{Arc, Mutex};

pub struct GroupByScan {
    s: Arc<Mutex<dyn Scan>>,
    group_fields: Vec<String>,
    agg_fns: Vec<Box<dyn AggregationFunction>>,
    group_val: Option<GroupValue>,
    more_groups: bool,
}

impl GroupByScan {
    pub fn new(
        s: Arc<Mutex<dyn Scan>>,
        group_fields: Vec<String>,
        agg_fns: Vec<Box<dyn AggregationFunction>>,
    ) -> Self {
        Self {
            s,
            group_fields,
            agg_fns,
            group_val: None,
            more_groups: false,
        }
    }

    fn before_first(&mut self) {
        self.s.lock().unwrap().before_first();
        self.more_groups = self.s.lock().unwrap().next();
    }

    fn next(&mut self) -> bool {
        if !self.more_groups {
            return false;
        }

        for fn_box in &mut self.agg_fns {
            fn_box.process_first(self.s.clone());
        }

        {
            self.group_val = Some(GroupValue::new(self.s.clone(), &self.group_fields));
        }

        loop {
            let mut s_lock = self.s.lock().unwrap();
            self.more_groups = s_lock.next();
            if !self.more_groups {
                break;
            }
            let gv = GroupValue::new(self.s.clone(), &self.group_fields);

            if self.group_val != Some(gv) {
                break;
            }

            for fn_box in &mut self.agg_fns {
                fn_box.process_next(self.s.clone());
            }
        }

        true
    }

    fn close(&mut self) {
        self.s.lock().unwrap().close();
    }

    fn get_value(&self, field_name: &str) -> Option<Constant> {
        if self.group_fields.contains(&field_name.to_string()) {
            return self.group_val.as_ref()?.get_value(field_name);
        }

        for fn_box in &self.agg_fns {
            if fn_box.field_name() == field_name {
                return Some(fn_box.value());
            }
        }

        None
    }

    fn get_int(&self, field_name: &str) -> Option<i32> {
        match self.get_value(field_name) {
            Some(value) => Some(value.as_int()),
            None => None,
        }
    }

    fn get_string(&self, field_name: &str) -> Option<String> {
        match self.get_value(field_name) {
            Some(value) => Some(value.as_str().to_string()),
            None => None,
        }
    }

    fn has_field(&self, field_name: &str) -> bool {
        self.group_fields.contains(&field_name.to_string())
            || self
                .agg_fns
                .iter()
                .any(|fn_box| fn_box.field_name() == field_name)
    }
}

impl Scan for GroupByScan {
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
