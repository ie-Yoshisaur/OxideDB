use crate::materialize::sort_scan::SortScan;
use crate::query::constant::Constant;
use crate::record::record_id::RecordId;

// no docs
// no comments
// no error handlings
// no variable name edit
pub trait Scan {
    fn before_first(&mut self);
    fn next(&mut self) -> bool;
    fn get_int(&self, field_name: &str) -> Option<i32>;
    fn get_string(&self, field_name: &str) -> Option<String>;
    fn get_value(&self, field_name: &str) -> Option<Constant>;
    fn has_field(&self, field_name: &str) -> bool;
    fn close(&mut self);

    // For Update
    fn set_value(&mut self, field_name: &str, value: Constant);
    fn set_int(&mut self, field_name: &str, value: i32);
    fn set_string(&mut self, field_name: &str, value: String);
    fn insert(&mut self);
    fn delete(&mut self);
    fn get_record_id(&self) -> RecordId;
    fn move_to_record_id(&mut self, record_id: RecordId);

    //Cast
    fn as_sort_scan(&self) -> Option<SortScan>;
}
