use crate::query::constant::Constant;
use crate::query::scan::Scan;
use crate::record::schema::Schema;
use std::sync::{Arc, Mutex};

// no docs
// no comments
// no error handlings
// no variable name edit
#[derive(Clone, Debug)]
pub enum Expression {
    Constant(Constant),
    FieldName(String),
}

impl Expression {
    pub fn evaluate(&self, s: Arc<Mutex<dyn Scan>>) -> Constant {
        match self {
            Self::Constant(val) => val.clone(),
            Self::FieldName(fldname) => s.lock().unwrap().get_value(fldname).unwrap(),
        }
    }

    pub fn is_field_name(&self) -> bool {
        matches!(self, Self::FieldName(_))
    }

    pub fn as_constant(&self) -> Option<Constant> {
        match self {
            Self::Constant(val) => Some(val.clone()),
            _ => None,
        }
    }

    pub fn as_field_name(&self) -> Option<String> {
        match self {
            Self::FieldName(fldname) => Some(fldname.clone()),
            _ => None,
        }
    }

    pub fn applies_to(&self, sch: Arc<Mutex<Schema>>) -> bool {
        match self {
            Self::Constant(_) => true,
            Self::FieldName(fldname) => sch.lock().unwrap().has_field(fldname),
        }
    }
}

impl std::fmt::Display for Expression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Constant(val) => write!(f, "{}", val),
            Self::FieldName(fldname) => write!(f, "{}", fldname),
        }
    }
}
