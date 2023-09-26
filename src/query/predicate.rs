use crate::plan::plan::Plan;
use crate::query::constant::Constant;
use crate::query::scan::Scan;
use crate::query::term::Term;
use crate::record::schema::Schema;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct Predicate {
    terms: Vec<Term>,
}

impl Predicate {
    pub fn new() -> Self {
        Self { terms: Vec::new() }
    }

    pub fn from_term(t: Term) -> Self {
        Self { terms: vec![t] }
    }

    pub fn conjoin_with(&mut self, pred: Predicate) {
        self.terms.extend(pred.terms);
    }

    pub fn is_satisfied(&self, s: Arc<Mutex<dyn Scan>>) -> bool {
        for term in &self.terms {
            if !term.is_satisfied(s.clone()) {
                return false;
            }
        }
        true
    }

    pub fn reduction_factor(&self, p: &dyn Plan) -> i32 {
        let mut factor = 1;
        for term in &self.terms {
            factor *= term.reduction_factor(p);
        }
        factor
    }

    pub fn select_sub_pred(&self, sch: Arc<Schema>) -> Option<Self> {
        let mut result = Predicate::new();
        for term in &self.terms {
            if term.applies_to(sch.clone()) {
                result.terms.push(term.clone());
            }
        }
        if result.terms.is_empty() {
            None
        } else {
            Some(result)
        }
    }

    pub fn join_sub_pred(&self, sch1: Arc<Schema>, sch2: Arc<Schema>) -> Option<Self> {
        let mut result = Predicate::new();
        let mut new_sch = Schema::new();
        new_sch.add_all(sch1.clone());
        new_sch.add_all(sch2.clone());
        let new_sch = Arc::new(new_sch);
        for term in &self.terms {
            if !term.applies_to(sch1.clone())
                && !term.applies_to(sch2.clone())
                && term.applies_to(new_sch.clone())
            {
                result.terms.push(term.clone());
            }
        }
        if result.terms.is_empty() {
            None
        } else {
            Some(result)
        }
    }

    pub fn equates_with_constant(&self, fldname: &str) -> Option<Constant> {
        for term in &self.terms {
            if let Some(c) = term.equates_with_constant(fldname) {
                return Some(c);
            }
        }
        None
    }

    pub fn equates_with_field(&self, fldname: &str) -> Option<String> {
        for term in &self.terms {
            if let Some(s) = term.equates_with_field(fldname) {
                return Some(s);
            }
        }
        None
    }
}

impl std::fmt::Display for Predicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let terms: Vec<String> = self.terms.iter().map(|t| t.to_string()).collect();
        write!(f, "{}", terms.join(" and "))
    }
}
