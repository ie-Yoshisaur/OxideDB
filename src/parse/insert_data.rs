// no docs
// no comments
// no error handlings
// no variable name edit
use crate::query::constant::Constant;
use std::vec::Vec;

pub struct InsertData {
    tblname: String,
    flds: Vec<String>,
    vals: Vec<Constant>,
}

impl InsertData {
    pub fn new(tblname: String, flds: Vec<String>, vals: Vec<Constant>) -> Self {
        if flds.len() != vals.len() {
            panic!("Field and value lists must have the same length");
        }
        Self {
            tblname,
            flds,
            vals,
        }
    }

    pub fn table_name(&self) -> String {
        self.tblname.clone()
    }

    pub fn fields(&self) -> &Vec<String> {
        &self.flds
    }

    pub fn vals(&self) -> &Vec<Constant> {
        &self.vals
    }
}
