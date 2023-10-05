use crate::materialize::aggregation_function::AggregationFunction;
use crate::materialize::sort_plan::SortPlan;
use crate::plan::plan::Plan;
use crate::record::schema::Schema;
use crate::transaction::transaction::Transaction;
use std::sync::{Arc, Mutex};

pub struct GroupByPlan {
    p: Arc<dyn Plan>,
    group_fields: Vec<String>,
    agg_fns: Vec<Box<dyn AggregationFunction>>,
    sch: Schema,
}

impl GroupByPlan {
    pub fn new(
        tx: Arc<Mutex<Transaction>>,
        p: Arc<Mutex<dyn Plan>>,
        group_fields: Vec<String>,
        agg_fns: Vec<Box<dyn AggregationFunction>>,
    ) -> Self {
        let mut sch = Schema::new();
        let sort_plan = SortPlan::new(tx, p.clone(), group_fields.clone());

        for field in &group_fields {
            sch.add(field.clone(), &p.lock().unwrap().schema().lock().unwrap());
        }

        for fn_box in &agg_fns {
            sch.add_int_field(fn_box.field_name());
        }

        Self {
            p: Arc::new(sort_plan),
            group_fields,
            agg_fns,
            sch,
        }
    }
}
