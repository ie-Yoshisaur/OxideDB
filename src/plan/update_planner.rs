// no docs
// no comments
// no error handlings
// no variable name edit
use crate::parse::create_index_data::CreateIndexData;
use crate::parse::create_table_data::CreateTableData;
use crate::parse::create_view_data::CreateViewData;
use crate::parse::delete_data::DeleteData;
use crate::parse::insert_data::InsertData;
use crate::parse::modify_data::ModifyData;
use crate::transaction::transaction::Transaction;
use std::sync::{Arc, Mutex};

pub trait UpdatePlanner {
    fn execute_insert(&self, data: InsertData, tx: Arc<Mutex<Transaction>>) -> usize;
    fn execute_delete(&self, data: DeleteData, tx: Arc<Mutex<Transaction>>) -> usize;
    fn execute_modify(&self, data: ModifyData, tx: Arc<Mutex<Transaction>>) -> usize;
    fn execute_create_table(&self, data: CreateTableData, tx: Arc<Mutex<Transaction>>) -> usize;
    fn execute_create_view(&self, data: CreateViewData, tx: Arc<Mutex<Transaction>>) -> usize;
    fn execute_create_index(&self, data: CreateIndexData, tx: Arc<Mutex<Transaction>>) -> usize;
}
