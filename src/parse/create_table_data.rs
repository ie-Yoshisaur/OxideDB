use crate::record::schema::Schema;
use std::sync::Arc;

pub struct CreateTableData {
    tblname: String,
    sch: Arc<Schema>,
}

impl CreateTableData {
    pub fn new(tblname: String, sch: Arc<Schema>) -> Self {
        Self { tblname, sch }
    }

    pub fn table_name(&self) -> &String {
        &self.tblname
    }

    pub fn new_schema(&self) -> Arc<Schema> {
        self.sch.clone()
    }
}
