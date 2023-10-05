use crate::metadata::metadata_manager::MetadataManager;
use crate::opt::table_planner::TablePlanner;
use crate::parse::query_data::QueryData;
use crate::plan::plan::Plan;
use crate::plan::project_plan::ProjectPlan;
use crate::transaction::transaction::Transaction;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

pub struct HeuristicQueryPlanner {
    table_planners: VecDeque<TablePlanner>,
    mdm: Arc<MetadataManager>,
}

impl HeuristicQueryPlanner {
    pub fn new(mdm: Arc<MetadataManager>) -> Self {
        Self {
            table_planners: VecDeque::new(),
            mdm,
        }
    }

    pub fn create_plan(
        &mut self,
        data: QueryData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Arc<Mutex<dyn Plan>> {
        // Step 1: Create a TablePlanner object for each mentioned table
        for tblname in data.tables() {
            let tp = TablePlanner::new(&tblname, data.pred(), tx.clone(), self.mdm.clone());
            self.table_planners.push_back(tp);
        }

        // Step 2: Choose the lowest-size plan to begin the join order
        let mut current_plan = self.get_lowest_select_plan();

        // Step 3: Repeatedly add a plan to the join order
        while !self.table_planners.is_empty() {
            match self.get_lowest_join_plan(current_plan.clone()) {
                Some(p) => current_plan = p,
                None => current_plan = self.get_lowest_product_plan(current_plan),
            }
        }

        // Step 4. Project on the field names and return
        Arc::new(Mutex::new(ProjectPlan::new(current_plan, data.fields())))
    }

    fn get_lowest_select_plan(&mut self) -> Arc<Mutex<dyn Plan>> {
        let (best_tp_idx, best_plan) = self
            .table_planners
            .iter()
            .enumerate()
            .map(|(idx, tp)| (idx, tp.make_select_plan()))
            .min_by_key(|(_, plan)| plan.lock().unwrap().records_output())
            .unwrap();

        self.table_planners.remove(best_tp_idx).unwrap();
        best_plan
    }

    fn get_lowest_join_plan(
        &mut self,
        current: Arc<Mutex<dyn Plan>>,
    ) -> Option<Arc<Mutex<dyn Plan>>> {
        let result = self
            .table_planners
            .iter()
            .enumerate()
            .filter_map(|(idx, tp)| {
                tp.make_join_plan(current.clone()).map(|plan| {
                    let records_output = plan.lock().unwrap().records_output();
                    (idx, (plan, records_output))
                })
            })
            .min_by_key(|(_, (_, records_output))| *records_output);

        match result {
            Some((idx, (plan, _))) => {
                self.table_planners.remove(idx).unwrap();
                Some(plan)
            }
            None => None,
        }
    }

    fn get_lowest_product_plan(&mut self, current: Arc<Mutex<dyn Plan>>) -> Arc<Mutex<dyn Plan>> {
        let (best_tp_idx, best_plan) = self
            .table_planners
            .iter()
            .enumerate()
            .map(|(idx, tp)| (idx, tp.make_product_plan(current.clone())))
            .min_by_key(|(_, plan)| plan.lock().unwrap().records_output())
            .unwrap();

        self.table_planners.remove(best_tp_idx).unwrap();
        best_plan
    }

    pub fn set_planner(&self, _p: Arc<Mutex<dyn Plan>>) {
        // for use in planning views, which
        // for simplicity this code doesn't do.
    }
}
