// no docs
// no comments
// no error handlings
// no variable name edit
use crate::record::schema::Schema;
use std::sync::Arc;
use std::sync::Mutex;

pub struct CreateTableData {
    tblname: String,
    sch: Arc<Mutex<Schema>>,
}

impl CreateTableData {
    pub fn new(tblname: String, sch: Arc<Mutex<Schema>>) -> Self {
        Self { tblname, sch }
    }

    pub fn table_name(&self) -> &String {
        &self.tblname
    }

    pub fn new_schema(&self) -> Arc<Mutex<Schema>> {
        self.sch.clone()
    }
}
