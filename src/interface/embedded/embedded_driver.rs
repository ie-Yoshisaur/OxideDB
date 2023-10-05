use crate::interface::embedded::embedded_connection::EmbeddedConnection;
use crate::server::oxide_db::OxideDB;
use std::error::Error;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

pub struct EmbeddedDriver;

impl EmbeddedDriver {
    pub fn new() -> Self {
        Self
    }

    pub fn connect(&self, _url: &str) -> Result<EmbeddedConnection, Box<dyn Error>> {
        let db_name = PathBuf::from("oxidedb");
        let db = Arc::new(Mutex::new(OxideDB::new(db_name).unwrap()));
        Ok(EmbeddedConnection::new(db))
    }
}
