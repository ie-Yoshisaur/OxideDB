// no docs
// no comments
// no error handlings
// no variable name edit
use crate::metadata::index_information::IndexInformation;
use crate::metadata::metadata_manager::MetadataManager;
use crate::record::schema::Schema;
use crate::record::table_scan::TableScan;
use crate::server::oxide_db::OxideDB;
use std::backtrace::Backtrace;
use std::collections::HashMap;
use std::error::Error;
use std::fs::remove_dir_all;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

#[test]
fn metadata_manager_test() -> Result<(), Box<dyn Error>> {
    let test_directory = PathBuf::from("metadatamgrtest");
    let db = OxideDB::new_from_parameters(test_directory.clone(), 400, 8);
    let transaction = Arc::new(Mutex::new(db.new_transaction()));
    let metadata_manager = MetadataManager::new(true, transaction.clone()).unwrap();

    let mut schema = Schema::new();
    schema.add_int_field("A".to_string());
    schema.add_string_field("B".to_string(), 9);
    let schema = Arc::new(Mutex::new(schema));

    // Part 1: Table Metadata
    metadata_manager.create_table("MyTable", schema, transaction.clone())?;
    let layout = Arc::new(metadata_manager.get_layout("MyTable", transaction.clone())?);
    let size = layout.get_slot_size();
    let schema2 = layout.get_schema();
    println!("MyTable has slot size {}", size);
    assert_eq!(
        size,
        21,
        "Slot size does not match expected value.\nBacktrace: {:#?}",
        Backtrace::capture()
    );
    {
        println!("Its fields are:");
        let schema_guard = schema2.lock().unwrap();
        for field_name in schema_guard.get_fields() {
            let field_type = schema_guard.get_field_type(&field_name).unwrap();
            println!("{}: {}", field_name, field_type);
        }
    }

    // Part 2: Statistics Metadata
    let mut table_scan = TableScan::new(transaction.clone(), "MyTable", layout.clone()).unwrap();
    for n in 0..50 {
        table_scan.insert().unwrap();
        table_scan.set_int("A", n).unwrap();
        table_scan.set_string("B", format!("rec{}", n)).unwrap();
    }
    let statistics_information = metadata_manager.get_statistics_information(
        "MyTable",
        layout.clone(),
        transaction.clone(),
    )?;
    println!("B(MyTable) = {}", statistics_information.blocks_accessed());
    assert_eq!(
        statistics_information.blocks_accessed(),
        3,
        "Blocks accessed do not match expected value.\nBacktrace: {:#?}",
        Backtrace::capture()
    );
    println!("R(MyTable) = {}", statistics_information.records_output());
    assert_eq!(
        statistics_information.records_output(),
        50,
        "Records output do not match expected value.\nBacktrace: {:#?}",
        Backtrace::capture()
    );
    println!(
        "V(MyTable,A) = {}",
        statistics_information.distinct_values("A")
    );
    println!(
        "V(MyTable,B) = {}",
        statistics_information.distinct_values("B")
    );

    // Part 3: View Metadata
    let view_definition = "select B from MyTable where A = 1";
    metadata_manager.create_view("viewA", view_definition, transaction.clone())?;
    let v = metadata_manager
        .get_view_def("viewA", transaction.clone())?
        .unwrap();
    println!("View def = {}", v);
    assert_eq!(
        v,
        "select B from MyTable where A = 1",
        "View definition does not match expected value.\nBacktrace: {:#?}",
        Backtrace::capture()
    );

    // Part 4: Index Metadata
    metadata_manager.create_index("indexA", "MyTable", "A", transaction.clone());
    metadata_manager.create_index("indexB", "MyTable", "B", transaction.clone());
    let index_map: HashMap<String, IndexInformation> =
        metadata_manager.get_index_information("MyTable", transaction.clone());

    let index_information = index_map.get("A").unwrap();
    println!("B(indexA) = {}", index_information.blocks_accessed());
    println!("R(indexA) = {}", index_information.records_output());
    println!("V(indexA,A) = {}", index_information.distinct_values("A"));
    println!("V(indexA,B) = {}", index_information.distinct_values("B"));

    let index_information = index_map.get("B").unwrap();
    println!("B(indexB) = {}", index_information.blocks_accessed());
    println!("R(indexB) = {}", index_information.records_output());
    println!("V(indexB,A) = {}", index_information.distinct_values("A"));
    println!("V(indexB,B) = {}", index_information.distinct_values("B"));

    transaction.lock().unwrap().commit().unwrap();

    remove_dir_all(test_directory).expect(&format!(
        "Failed to remove test directory.\nBacktrace: {:#?}",
        Backtrace::capture()
    ));
    Ok(())
}
