use crate::interface::embedded::embedded_statement::EmbeddedStatement;
use crate::transaction::transaction::Transaction;
use std::error::Error;
use std::sync::Arc;
use std::sync::Mutex;

pub trait ConnectionAdapter {
    fn create_statement(&self) -> Result<EmbeddedStatement, Box<dyn Error>>;
    fn close(&mut self) -> Result<(), Box<dyn Error>>;
    fn commit(&mut self) -> Result<(), Box<dyn Error>>;
    fn rollback(&mut self) -> Result<(), Box<dyn Error>>;
    fn get_transaction(&self) -> Arc<Mutex<Transaction>>;
}
