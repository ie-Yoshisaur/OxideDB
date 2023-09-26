use crate::parse::query_data::QueryData;
use crate::plan::plan::Plan;
use crate::transaction::transaction::Transaction;
use std::sync::Arc;
use std::sync::Mutex;

pub trait QueryPlanner {
    fn create_plan(&self, data: QueryData, tx: Arc<Mutex<Transaction>>) -> Arc<dyn Plan>;
}
