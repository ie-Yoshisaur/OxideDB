use crate::interface::connection_adapter::ConnectionAdapter;
use crate::interface::embedded::embedded_connection::EmbeddedConnection;
use crate::interface::embedded::embedded_result_set::EmbeddedResultSet;
use crate::plan::planner::Planner;
use std::error::Error;
use std::sync::{Arc, Mutex};

pub struct EmbeddedStatement {
    conn: Arc<Mutex<EmbeddedConnection>>,
    planner: Arc<Mutex<Planner>>,
}

impl EmbeddedStatement {
    pub fn new(conn: EmbeddedConnection, planner: Arc<Mutex<Planner>>) -> Self {
        EmbeddedStatement {
            conn: Arc::new(Mutex::new(conn)),
            planner,
        }
    }

    pub fn execute_query(&self, qry: &str) -> Result<EmbeddedResultSet, Box<dyn Error>> {
        let tx = self.conn.lock().unwrap().get_transaction();
        let pln = self
            .planner
            .lock()
            .unwrap()
            .create_query_plan(qry, tx.clone());
        EmbeddedResultSet::new(pln, self.conn.clone())
    }

    pub fn execute_update(&self, cmd: &str) -> Result<i32, Box<dyn Error>> {
        let tx = self.conn.lock().unwrap().get_transaction();
        let result = self.planner.lock().unwrap().execute_update(cmd, tx.clone());
        self.conn.lock().unwrap().commit()?;
        Ok(result as i32)
        // when it fails, execute self.conn.lock().unwrap().rollback()?;
    }

    pub fn close(&self) -> Result<(), Box<dyn Error>> {
        Ok(())
    }
}
