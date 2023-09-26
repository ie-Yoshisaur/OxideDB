use crate::query::predicate::Predicate;
use std::fmt;

pub struct QueryData {
    fields: Vec<String>,
    tables: Vec<String>,
    pred: Predicate,
}

impl QueryData {
    pub fn new(fields: Vec<String>, tables: Vec<String>, pred: Predicate) -> Self {
        Self {
            fields,
            tables,
            pred,
        }
    }

    pub fn fields(&self) -> Vec<String> {
        self.fields.clone()
    }

    pub fn tables(&self) -> Vec<String> {
        self.tables.clone()
    }

    pub fn pred(&self) -> Predicate {
        self.pred.clone()
    }
}

impl fmt::Display for QueryData {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let fields = self.fields.join(", ");
        let tables = self.tables.join(", ");
        let pred_string = self.pred.to_string();
        let result = if !pred_string.is_empty() {
            format!("select {} from {} where {}", fields, tables, pred_string)
        } else {
            format!("select {} from {}", fields, tables)
        };
        write!(f, "{}", result)
    }
}
