use crate::interface::connection_adapter::ConnectionAdapter;
use crate::interface::embedded::embedded_statement::EmbeddedStatement;
use crate::plan::planner::Planner;
use crate::server::oxide_db::OxideDB;
use crate::transaction::transaction::Transaction;
use std::error::Error;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct EmbeddedConnection {
    db: Arc<Mutex<OxideDB>>,
    current_tx: Arc<Mutex<Transaction>>,
    planner: Arc<Mutex<Planner>>,
}

impl EmbeddedConnection {
    pub fn new(db: Arc<Mutex<OxideDB>>) -> Self {
        let current_tx = Arc::new(Mutex::new(db.lock().unwrap().new_transaction()));
        let planner = db.lock().unwrap().get_planner().clone().unwrap();
        EmbeddedConnection {
            db,
            current_tx,
            planner,
        }
    }
}

impl ConnectionAdapter for EmbeddedConnection {
    fn create_statement(&self) -> Result<EmbeddedStatement, Box<dyn Error>> {
        Ok(EmbeddedStatement::new(self.clone(), self.planner.clone()))
    }

    fn close(&mut self) -> Result<(), Box<dyn Error>> {
        self.current_tx.lock().unwrap().commit()?;
        Ok(())
    }

    fn commit(&mut self) -> Result<(), Box<dyn Error>> {
        self.current_tx.lock().unwrap().commit()?;
        self.current_tx = Arc::new(Mutex::new(self.db.lock().unwrap().new_transaction()));
        Ok(())
    }

    fn rollback(&mut self) -> Result<(), Box<dyn Error>> {
        self.current_tx.lock().unwrap().rollback()?;
        self.current_tx = Arc::new(Mutex::new(self.db.lock().unwrap().new_transaction()));
        Ok(())
    }

    fn get_transaction(&self) -> Arc<Mutex<Transaction>> {
        self.current_tx.clone()
    }
}
