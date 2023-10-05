use crate::interface::connection_adapter::ConnectionAdapter;
use crate::interface::embedded::embedded_connection::EmbeddedConnection;
use crate::interface::embedded::embedded_metadata::EmbeddedMetadata;
use crate::plan::plan::Plan;
use crate::query::scan::Scan;
use crate::record::schema::Schema;
use std::error::Error;
use std::sync::{Arc, Mutex};

pub struct EmbeddedResultSet {
    scan: Arc<Mutex<dyn Scan>>,
    schema: Arc<Mutex<Schema>>,
    conn: Arc<Mutex<EmbeddedConnection>>,
}

impl EmbeddedResultSet {
    pub fn new(
        plan: Arc<Mutex<dyn Plan>>,
        conn: Arc<Mutex<EmbeddedConnection>>,
    ) -> Result<Self, Box<dyn Error>> {
        let scan = plan.lock().unwrap().open();
        let schema = plan.lock().unwrap().schema();
        Ok(EmbeddedResultSet { scan, schema, conn })
    }

    pub fn next(&self) -> Result<bool, Box<dyn Error>> {
        Ok(self.scan.lock().unwrap().next())
        // when it fails, execute self.conn.lock().unwrap().rollback()?;
    }

    pub fn get_int(&mut self, fldname: &str) -> Result<i32, Box<dyn Error>> {
        let fldname = fldname.to_lowercase(); // to ensure case-insensitivity
        if let Some(result) = self.scan.lock().unwrap().get_int(fldname.as_str()) {
            Ok(result)
        } else {
            self.conn.lock().unwrap().rollback()?;
            panic!()
        }
    }

    pub fn get_string(&mut self, fldname: &str) -> Result<String, Box<dyn Error>> {
        let fldname = fldname.to_lowercase(); // to ensure case-insensitivity
        if let Some(result) = self.scan.lock().unwrap().get_string(fldname.as_str()) {
            Ok(result)
        } else {
            self.conn.lock().unwrap().rollback()?;
            panic!()
        }
    }

    pub fn get_meta_data(&self) -> EmbeddedMetadata {
        EmbeddedMetadata::new(self.schema.clone())
    }

    pub fn close(&mut self) -> Result<(), Box<dyn Error>> {
        self.scan.lock().unwrap().close();
        self.conn.lock().unwrap().commit().unwrap();
        Ok(())
    }
}
