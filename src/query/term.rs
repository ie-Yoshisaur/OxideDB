use crate::plan::plan::Plan;
use crate::query::constant::Constant;
use crate::query::expression::Expression;
use crate::query::scan::Scan;
use crate::record::schema::Schema;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct Term {
    lhs: Expression,
    rhs: Expression,
}

impl Term {
    pub fn new(lhs: Expression, rhs: Expression) -> Self {
        Self { lhs, rhs }
    }

    pub fn is_satisfied(&self, s: Arc<Mutex<dyn Scan>>) -> bool {
        let lhs_val = self.lhs.evaluate(s.clone());
        let rhs_val = self.rhs.evaluate(s.clone());
        lhs_val == rhs_val
    }

    pub fn reduction_factor(&self, p: &dyn Plan) -> i32 {
        if self.lhs.is_field_name() && self.rhs.is_field_name() {
            let lhs_name = self.lhs.as_field_name().unwrap();
            let rhs_name = self.rhs.as_field_name().unwrap();
            return std::cmp::max(p.distinct_values(&lhs_name), p.distinct_values(&rhs_name));
        }
        if self.lhs.is_field_name() {
            let lhs_name = self.lhs.as_field_name().unwrap();
            return p.distinct_values(&lhs_name);
        }
        if self.rhs.is_field_name() {
            let rhs_name = self.rhs.as_field_name().unwrap();
            return p.distinct_values(&rhs_name);
        }
        if self.lhs.as_constant() == self.rhs.as_constant() {
            return 1;
        }
        std::i32::MAX
    }

    pub fn equates_with_constant(&self, fldname: &str) -> Option<Constant> {
        if self.lhs.is_field_name()
            && self.lhs.as_field_name().as_deref() == Some(fldname)
            && !self.rhs.is_field_name()
        {
            return self.rhs.as_constant();
        } else if self.rhs.is_field_name()
            && self.rhs.as_field_name().as_deref() == Some(fldname)
            && !self.lhs.is_field_name()
        {
            return self.lhs.as_constant();
        }
        None
    }

    pub fn equates_with_field(&self, fldname: &str) -> Option<String> {
        if self.lhs.is_field_name()
            && self.lhs.as_field_name().as_deref() == Some(fldname)
            && self.rhs.is_field_name()
        {
            return self.rhs.as_field_name();
        } else if self.rhs.is_field_name()
            && self.rhs.as_field_name().as_deref() == Some(fldname)
            && self.lhs.is_field_name()
        {
            return self.lhs.as_field_name();
        }
        None
    }

    pub fn applies_to(&self, sch: Arc<Schema>) -> bool {
        self.lhs.applies_to(sch.clone()) && self.rhs.applies_to(sch.clone())
    }
}

impl std::fmt::Display for Term {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} = {}", self.lhs, self.rhs)
    }
}
