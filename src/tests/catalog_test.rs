use crate::metadata::table_manager::TableManager;
use crate::record::table_scan::TableScan;
use crate::server::oxide_db::OxideDB;
use std::backtrace::Backtrace;
use std::fs::remove_dir_all;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

// This test aims to validate the behavior of catalog tables in the OxideDB.
// Steps for the test are as follows:
// - Initialize OxideDB instance and start a new transaction.
// - Get the layouts for catalog tables "table_catalog" and "field_catalog".
// - Scan through "table_catalog" and print the name and slot size of each table.
// - Scan through "field_catalog" and print the name, field, and offset for each field in all tables.
// - Clean up by removing the test directory.
#[test]
fn catalog_test() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize test directory and OxideDB instance
    let test_directory = PathBuf::from("tabletest");
    let db = OxideDB::new_from_parameters(test_directory.clone(), 400, 8);
    let transaction = Arc::new(Mutex::new(db.new_transaction()));

    // Create TableManager instance
    let table_manager = Arc::new(Mutex::new(
        TableManager::new(false, transaction.clone()).expect(&format!(
            "Failed to create TableManager.\nBacktrace: {:#?}",
            Backtrace::capture()
        )),
    ));

    // Get layout for table_catalog
    let table_catalog_layout = Arc::new(
        table_manager
            .lock()
            .unwrap()
            .get_layout("table_catalog", transaction.clone())
            .expect(&format!(
                "Failed to get layout for 'table_catalog'.\nBacktrace: {:#?}",
                Backtrace::capture()
            )),
    );

    // Scan through table_catalog and print table names and slot sizes
    println!("Here are all the tables and their lengths.");
    let mut table_scan = TableScan::new(
        transaction.clone(),
        "table_catalog",
        table_catalog_layout.clone(),
    )
    .expect(&format!(
        "Failed to initialize table scan for 'table_catalog'.\nBacktrace: {:#?}",
        Backtrace::capture()
    ));

    while table_scan.next().expect(&format!(
        "Failed during table scan of 'table_catalog'.\nBacktrace: {:#?}",
        Backtrace::capture()
    )) {
        let table_name = table_scan.get_string("tblname").expect(&format!(
            "Failed to get 'tblname' from 'table_catalog'.\nBacktrace: {:#?}",
            Backtrace::capture()
        ));
        let slot_size = table_scan.get_int("slotsize").expect(&format!(
            "Failed to get 'slotsize' from 'table_catalog'.\nBacktrace: {:#?}",
            Backtrace::capture()
        ));
        println!("{} {}", table_name, slot_size);
    }

    table_scan.close();

    // Get layout for field_catalog
    println!("\nHere are the fields for each table and their offsets");
    let field_catalog_layout = Arc::new(
        table_manager
            .lock()
            .unwrap()
            .get_layout("field_catalog", transaction.clone())
            .expect(&format!(
                "Failed to get layout for 'field_catalog'.\nBacktrace: {:#?}",
                Backtrace::capture()
            )),
    );

    // Scan through field_catalog and print field details for each table
    let mut field_scan = TableScan::new(transaction, "field_catalog", field_catalog_layout.clone())
        .expect(&format!(
            "Failed to initialize table scan for 'field_catalog'.\nBacktrace: {:#?}",
            Backtrace::capture()
        ));

    while field_scan.next().expect(&format!(
        "Failed during table scan of 'field_catalog'.\nBacktrace: {:#?}",
        Backtrace::capture()
    )) {
        let table_name = field_scan.get_string("tblname").expect(&format!(
            "Failed to get 'tblname' from 'field_catalog'.\nBacktrace: {:#?}",
            Backtrace::capture()
        ));
        let field_name = field_scan.get_string("fldname").expect(&format!(
            "Failed to get 'fldname' from 'field_catalog'.\nBacktrace: {:#?}",
            Backtrace::capture()
        ));
        let offset = field_scan.get_int("offset").expect(&format!(
            "Failed to get 'offset' from 'field_catalog'.\nBacktrace: {:#?}",
            Backtrace::capture()
        ));
        println!("{} {} {}", table_name, field_name, offset);
    }

    field_scan.close();

    // Cleanup
    remove_dir_all(test_directory).expect(&format!(
        "Failed to remove test directory.\nBacktrace: {:#?}",
        Backtrace::capture()
    ));

    Ok(())
}
