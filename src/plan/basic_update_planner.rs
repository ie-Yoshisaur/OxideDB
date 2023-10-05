// no docs
// no comments
// no error handlings
// no variable name edit
use crate::metadata::metadata_manager::MetadataManager;
use crate::parse::create_index_data::CreateIndexData;
use crate::parse::create_table_data::CreateTableData;
use crate::parse::create_view_data::CreateViewData;
use crate::parse::delete_data::DeleteData;
use crate::parse::insert_data::InsertData;
use crate::parse::modify_data::ModifyData;
use crate::plan::plan::Plan;
use crate::plan::select_plan::SelectPlan;
use crate::plan::table_plan::TablePlan;
use crate::plan::update_planner::UpdatePlanner;
use crate::transaction::transaction::Transaction;
use std::sync::Arc;
use std::sync::Mutex;

pub struct BasicUpdatePlanner {
    mdm: Arc<MetadataManager>,
}

impl BasicUpdatePlanner {
    pub fn new(mdm: Arc<MetadataManager>) -> Self {
        BasicUpdatePlanner { mdm }
    }

    pub fn execute_delete(&self, data: DeleteData, tx: Arc<Mutex<Transaction>>) -> usize {
        let mut p: Arc<Mutex<dyn Plan>> = Arc::new(Mutex::new(TablePlan::new(
            tx.clone(),
            data.table_name(),
            self.mdm.clone(),
        )));
        p = Arc::new(Mutex::new(SelectPlan::new(p, data.pred())));
        let us = p.lock().unwrap().open();
        let mut count = 0;
        while us.lock().unwrap().next() {
            us.lock().unwrap().delete();
            count += 1;
        }
        us.lock().unwrap().close();
        count
    }

    pub fn execute_modify(&self, data: ModifyData, tx: Arc<Mutex<Transaction>>) -> usize {
        let mut p: Arc<Mutex<dyn Plan>> = Arc::new(Mutex::new(TablePlan::new(
            tx.clone(),
            data.table_name(),
            self.mdm.clone(),
        )));
        p = Arc::new(Mutex::new(SelectPlan::new(p, data.pred())));
        let us = p.lock().unwrap().open();
        let mut count = 0;
        while us.lock().unwrap().next() {
            let val = data.new_value().evaluate(us.clone());
            us.lock().unwrap().set_value(data.target_field(), val);
            count += 1;
        }
        us.lock().unwrap().close();
        count
    }

    pub fn execute_insert(&self, data: InsertData, tx: Arc<Mutex<Transaction>>) -> usize {
        let mut p: Arc<Mutex<dyn Plan>> = Arc::new(Mutex::new(TablePlan::new(
            tx.clone(),
            data.table_name(),
            self.mdm.clone(),
        )));
        let us = p.lock().unwrap().open();
        us.lock().unwrap().insert();
        for (fldname, val) in data.fields().iter().zip(data.vals().iter()) {
            us.lock().unwrap().set_value(fldname, val.clone());
        }
        us.lock().unwrap().close();
        1
    }

    pub fn execute_create_table(
        &self,
        data: CreateTableData,
        tx: Arc<Mutex<Transaction>>,
    ) -> usize {
        self.mdm
            .create_table(data.table_name(), data.new_schema().clone(), tx)
            .unwrap();
        0
    }

    pub fn execute_create_view(&self, data: CreateViewData, tx: Arc<Mutex<Transaction>>) -> usize {
        self.mdm
            .create_view(data.view_name().as_str(), data.view_def().as_str(), tx)
            .unwrap();
        0
    }

    pub fn execute_create_index(
        &self,
        data: CreateIndexData,
        tx: Arc<Mutex<Transaction>>,
    ) -> usize {
        self.mdm
            .create_index(data.index_name(), data.table_name(), data.field_name(), tx);
        0
    }
}

impl UpdatePlanner for BasicUpdatePlanner {
    fn execute_insert(&self, data: InsertData, tx: Arc<Mutex<Transaction>>) -> usize {
        self.execute_insert(data, tx)
    }

    fn execute_delete(&self, data: DeleteData, tx: Arc<Mutex<Transaction>>) -> usize {
        self.execute_delete(data, tx)
    }

    fn execute_modify(&self, data: ModifyData, tx: Arc<Mutex<Transaction>>) -> usize {
        self.execute_modify(data, tx)
    }

    fn execute_create_table(&self, data: CreateTableData, tx: Arc<Mutex<Transaction>>) -> usize {
        self.execute_create_table(data, tx)
    }

    fn execute_create_view(&self, data: CreateViewData, tx: Arc<Mutex<Transaction>>) -> usize {
        self.execute_create_view(data, tx)
    }

    fn execute_create_index(&self, data: CreateIndexData, tx: Arc<Mutex<Transaction>>) -> usize {
        self.execute_create_index(data, tx)
    }
}
