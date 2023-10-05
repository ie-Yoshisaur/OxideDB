// no docs
// no comments
// no error handlings
// no variable name edit
use crate::parse::parser::Parser;
use crate::parse::update_data::UpdateData;
use crate::plan::plan::Plan;
use crate::plan::query_planner::QueryPlanner;
use crate::plan::update_planner::UpdatePlanner;
use crate::transaction::transaction::Transaction;
use std::sync::Arc;
use std::sync::Mutex;

pub struct Planner {
    qplanner: Arc<dyn QueryPlanner>,
    uplanner: Arc<dyn UpdatePlanner>,
}

impl Planner {
    pub fn new(qplanner: Arc<dyn QueryPlanner>, uplanner: Arc<dyn UpdatePlanner>) -> Self {
        Planner { qplanner, uplanner }
    }

    pub fn create_query_plan(
        &self,
        qry: &str,
        tx: Arc<Mutex<Transaction>>,
    ) -> Arc<Mutex<dyn Plan>> {
        let mut parser = Parser::new(qry);
        let data = parser.query();
        self.qplanner.create_plan(data, tx)
    }

    pub fn execute_update(&self, cmd: &str, tx: Arc<Mutex<Transaction>>) -> usize {
        let mut parser = Parser::new(cmd);
        parser.update_cmd().map(|data| match data {
            UpdateData::Insert(data) => self.uplanner.execute_insert(data, tx),
            UpdateData::Delete(data) => self.uplanner.execute_delete(data, tx),
            UpdateData::Modify(data) => self.uplanner.execute_modify(data, tx),
            UpdateData::CreateTable(data) => self.uplanner.execute_create_table(data, tx),
            UpdateData::CreateView(data) => self.uplanner.execute_create_view(data, tx),
            UpdateData::CreateIndex(data) => self.uplanner.execute_create_index(data, tx),
        });
        0
    }
}
