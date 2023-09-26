use crate::query::constant::Constant;
use crate::record::record_id::RecordId;

pub trait Scan {
    fn before_first(&mut self);
    fn next(&mut self) -> bool;
    fn get_int(&self, fldname: &str) -> Option<i32>;
    fn get_string(&self, fldname: &str) -> Option<String>;
    fn get_val(&self, fldname: &str) -> Option<Constant>;
    fn has_field(&self, fldname: &str) -> bool;
    fn close(&mut self);

    // For Update
    fn set_val(&mut self, fldname: &str, val: Constant);
    fn set_int(&mut self, fldname: &str, val: i32);
    fn set_string(&mut self, fldname: &str, val: String);
    fn insert(&mut self);
    fn delete(&mut self);
    fn get_rid(&self) -> RecordId;
    fn move_to_rid(&mut self, rid: RecordId);
}

