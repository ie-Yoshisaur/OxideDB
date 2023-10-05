use crate::index::planner::index_join_plan::IndexJoinPlan;
use crate::index::planner::index_select_plan::IndexSelectPlan;
use crate::metadata::index_information::IndexInformation;
use crate::metadata::metadata_manager::MetadataManager;
use crate::multibuffer::multi_buffer_product_plan::MultibufferProductPlan;
use crate::plan::plan::Plan;
use crate::plan::select_plan::SelectPlan;
use crate::plan::table_plan::TablePlan;
use crate::query::predicate::Predicate;
use crate::record::schema::Schema;
use crate::transaction::transaction::Transaction;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub struct TablePlanner {
    my_plan: Arc<Mutex<TablePlan>>,
    my_pred: Predicate,
    my_schema: Arc<Mutex<Schema>>,
    indexes: HashMap<String, IndexInformation>,
    tx: Arc<Mutex<Transaction>>,
}

impl TablePlanner {
    pub fn new(
        tbl_name: &str,
        my_pred: Predicate,
        tx: Arc<Mutex<Transaction>>,
        mdm: Arc<MetadataManager>,
    ) -> Self {
        let my_plan = Arc::new(Mutex::new(TablePlan::new(
            tx.clone(),
            tbl_name.to_string(),
            mdm.clone(),
        )));
        let my_schema = my_plan.lock().unwrap().schema();
        let indexes = mdm.get_index_information(tbl_name, tx.clone());
        Self {
            my_plan,
            my_pred,
            my_schema,
            indexes,
            tx,
        }
    }

    pub fn make_select_plan(&self) -> Arc<Mutex<dyn Plan>> {
        match self.make_index_select() {
            Some(p) => self.add_select_pred(p),
            None => self.add_select_pred(self.my_plan.clone()),
        }
    }

    pub fn make_join_plan(&self, current: Arc<Mutex<dyn Plan>>) -> Option<Arc<Mutex<dyn Plan>>> {
        let curr_sch = current.clone().lock().unwrap().schema();
        let join_pred = self
            .my_pred
            .join_sub_pred(self.my_schema.clone(), curr_sch.clone());
        if join_pred.is_none() {
            return None;
        }

        match self.make_index_join(current.clone(), curr_sch.clone()) {
            Some(p) => Some(self.add_join_pred(p, curr_sch.clone())),
            None => Some(
                self.add_join_pred(self.make_product_join(current, curr_sch.clone()), curr_sch),
            ),
        }
    }

    pub fn make_product_plan(&self, current: Arc<Mutex<dyn Plan>>) -> Arc<Mutex<dyn Plan>> {
        let p = self.add_select_pred(self.my_plan.clone());
        Arc::new(Mutex::new(MultibufferProductPlan::new(
            self.tx.clone(),
            current,
            p,
        )))
    }

    fn make_index_select(&self) -> Option<Arc<Mutex<dyn Plan>>> {
        for (fld_name, index_info) in &self.indexes {
            if let Some(val) = self.my_pred.equates_with_constant(fld_name) {
                println!("index on {} used", fld_name);
                return Some(Arc::new(Mutex::new(IndexSelectPlan::new(
                    self.my_plan.clone(),
                    Arc::new(Mutex::new(index_info.clone())),
                    val,
                ))));
            }
        }
        None
    }

    fn make_index_join(
        &self,
        current: Arc<Mutex<dyn Plan>>,
        curr_sch: Arc<Mutex<Schema>>,
    ) -> Option<Arc<Mutex<dyn Plan>>> {
        for (fld_name, index_info) in &self.indexes {
            if let Some(outer_field) = self.my_pred.equates_with_field(fld_name) {
                if curr_sch.lock().unwrap().has_field(&outer_field) {
                    let p = Arc::new(Mutex::new(IndexJoinPlan::new(
                        current,
                        self.my_plan.clone(),
                        index_info.clone(),
                        outer_field,
                    )));
                    return Some(self.add_select_pred(p));
                }
            }
        }
        None
    }

    fn make_product_join(
        &self,
        current: Arc<Mutex<dyn Plan>>,
        curr_sch: Arc<Mutex<Schema>>,
    ) -> Arc<Mutex<dyn Plan>> {
        self.add_join_pred(self.make_product_plan(current), curr_sch)
    }

    fn add_select_pred(&self, p: Arc<Mutex<dyn Plan>>) -> Arc<Mutex<dyn Plan>> {
        if let Some(select_pred) = self.my_pred.select_sub_pred(self.my_schema.clone()) {
            Arc::new(Mutex::new(SelectPlan::new(p, select_pred)))
        } else {
            p
        }
    }

    fn add_join_pred(
        &self,
        p: Arc<Mutex<dyn Plan>>,
        curr_sch: Arc<Mutex<Schema>>,
    ) -> Arc<Mutex<dyn Plan>> {
        if let Some(join_pred) = self.my_pred.join_sub_pred(curr_sch, self.my_schema.clone()) {
            Arc::new(Mutex::new(SelectPlan::new(p, join_pred)))
        } else {
            p
        }
    }
}
