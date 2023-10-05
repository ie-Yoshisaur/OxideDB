// no docs
// no comments
// no error handlings
// no variable name edit
use crate::query::constant::Constant;
use crate::record::record_id::RecordId;

pub trait Index {
    fn before_first(&mut self, search_key: Constant);
    fn next(&mut self) -> bool;
    fn get_data_rid(&self) -> Option<RecordId>;
    fn insert(&mut self, data_val: Constant, data_rid: RecordId);
    fn delete(&mut self, data_val: Constant, data_rid: RecordId);
    fn close(&mut self);
}
