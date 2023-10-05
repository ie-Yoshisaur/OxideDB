use crate::metadata::metadata_manager::MetadataManager;
use crate::parse::delete_data::DeleteData;
use crate::parse::insert_data::InsertData;
use crate::plan::plan::Plan;
use crate::plan::select_plan::SelectPlan;
use crate::plan::table_plan::TablePlan;
use crate::transaction::transaction::Transaction;
use std::sync::{Arc, Mutex};

pub struct IndexUpdatePlanner {
    mdm: Arc<MetadataManager>,
}

impl IndexUpdatePlanner {
    pub fn new(mdm: Arc<MetadataManager>) -> Self {
        Self { mdm }
    }

    pub fn execute_insert(&self, data: InsertData, tx: Arc<Mutex<Transaction>>) -> i32 {
        let tblname = data.table_name();
        let mut p = TablePlan::new(tx.clone(), tblname.clone(), self.mdm.clone());

        let s = p.open();
        s.lock().unwrap().insert();
        let rid = s.lock().unwrap().get_record_id();

        let indexes = self.mdm.get_index_information(&tblname, tx.clone());
        let mut val_iter = data.vals().into_iter();
        for fldname in data.fields() {
            let val = val_iter.next().unwrap();
            s.lock().unwrap().set_value(&fldname, val.clone());

            if let Some(ii) = indexes.get(fldname) {
                let idx = ii.open();
                idx.lock().unwrap().insert(val.clone(), rid.clone());
                idx.lock().unwrap().close();
            }
        }
        s.lock().unwrap().close();
        1
    }

    pub fn execute_delete(&self, data: DeleteData, tx: Arc<Mutex<Transaction>>) -> i32 {
        let tblname = data.table_name();
        let table_plan = Arc::new(Mutex::new(TablePlan::new(
            tx.clone(),
            tblname.clone(),
            self.mdm.clone(),
        )));
        let select_plan = Arc::new(Mutex::new(SelectPlan::new(table_plan, data.pred())));

        let indexes = self.mdm.get_index_information(&tblname, tx.clone());

        let s = select_plan.lock().unwrap().open();
        let mut count = 0;
        while s.lock().unwrap().next() {
            // first, delete the record's RID from every index
            let rid = s.lock().unwrap().get_record_id();
            for (fldname, index_info) in &indexes {
                let val = s.lock().unwrap().get_value(fldname).unwrap();
                let idx = index_info.open();
                idx.lock().unwrap().delete(val, rid.clone());
                idx.lock().unwrap().close();
            }

            // then delete the record
            s.lock().unwrap().delete();
            count += 1;
        }
        s.lock().unwrap().close();
        count
    }
}
