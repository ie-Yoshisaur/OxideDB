// no docs
// no comments
// no error handlings
// no variable name edit
use crate::query::expression::Expression;
use crate::query::predicate::Predicate;

pub struct ModifyData {
    tblname: String,
    fldname: String,
    newval: Expression,
    pred: Predicate,
}

impl ModifyData {
    pub fn new(tblname: String, fldname: String, newval: Expression, pred: Predicate) -> Self {
        Self {
            tblname,
            fldname,
            newval,
            pred,
        }
    }

    pub fn table_name(&self) -> String {
        self.tblname.clone()
    }

    pub fn target_field(&self) -> &String {
        &self.fldname
    }

    pub fn new_value(&self) -> &Expression {
        &self.newval
    }

    pub fn pred(&self) -> Predicate {
        self.pred.clone()
    }
}
