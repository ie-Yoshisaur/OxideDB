// no docs
// no comments
// no error handlings
// no variable name edit
use crate::metadata::metadata_manager::MetadataManager;
use crate::parse::parser::Parser;
use crate::parse::query_data::QueryData;
use crate::plan::plan::Plan;
use crate::plan::product_plan::ProductPlan;
use crate::plan::project_plan::ProjectPlan;
use crate::plan::query_planner::QueryPlanner;
use crate::plan::select_plan::SelectPlan;
use crate::plan::table_plan::TablePlan;
use crate::transaction::transaction::Transaction;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

pub struct BetterQueryPlanner {
    mdm: Arc<MetadataManager>,
}

impl BetterQueryPlanner {
    pub fn new(mdm: Arc<MetadataManager>) -> Self {
        BetterQueryPlanner { mdm }
    }

    pub fn create_plan(
        &self,
        data: QueryData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Arc<Mutex<dyn Plan>> {
        let mut plans: VecDeque<Arc<Mutex<dyn Plan>>> = VecDeque::new();
        for tblname in data.tables() {
            let viewdef = self.mdm.get_view_def(&tblname, tx.clone()).unwrap();
            if let Some(viewdef) = viewdef {
                let mut parser = Parser::new(&viewdef);
                let viewdata = parser.query();
                plans.push_back(self.create_plan(viewdata, tx.clone()));
            } else {
                plans.push_back(Arc::new(Mutex::new(TablePlan::new(
                    tx.clone(),
                    tblname,
                    self.mdm.clone(),
                ))));
            }
        }

        let mut p = plans.pop_front().unwrap();
        for nextplan in plans {
            let choice1 = Arc::new(Mutex::new(ProductPlan::new(nextplan.clone(), p.clone())));
            let choice2 = Arc::new(Mutex::new(ProductPlan::new(p.clone(), nextplan.clone())));
            if choice1.lock().unwrap().blocks_accessed() < choice2.lock().unwrap().blocks_accessed()
            {
                p = choice1;
            } else {
                p = choice2;
            }
        }

        p = Arc::new(Mutex::new(SelectPlan::new(p, data.pred())));
        p = Arc::new(Mutex::new(ProjectPlan::new(p, data.fields())));
        p
    }
}

impl QueryPlanner for BetterQueryPlanner {
    fn create_plan(&self, data: QueryData, tx: Arc<Mutex<Transaction>>) -> Arc<Mutex<dyn Plan>> {
        self.create_plan(data, tx)
    }
}
