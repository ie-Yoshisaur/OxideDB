use crate::query::predicate::Predicate;

pub struct DeleteData {
    tblname: String,
    pred: Predicate,
}

impl DeleteData {
    pub fn new(tblname: String, pred: Predicate) -> Self {
        Self { tblname, pred }
    }

    pub fn table_name(&self) -> String {
        self.tblname.clone()
    }

    pub fn pred(&self) -> Predicate {
        self.pred.clone()
    }
}
