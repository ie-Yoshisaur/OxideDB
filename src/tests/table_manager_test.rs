use crate::metadata::table_manager::TableManager;
use crate::record::field_type::FieldType;
use crate::record::schema::Schema;
use crate::server::oxide_db::OxideDB;
use std::backtrace::Backtrace;
use std::fs::remove_dir_all;
use std::mem::size_of;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

const I32_SIZE: usize = size_of::<i32>();

// This test aims to validate the behavior of the TableManager class.
// The steps for the test are as follows:
// - Create an instance of the OxideDB database.
// - Start a new transaction.
// - Create a TableManager instance.
// - Define a schema with two fields: "A" as an integer and "B" as a varchar.
// - Use the TableManager to create a new table called "MyTable" with the defined schema.
// - Fetch the layout of "MyTable" and confirm its properties.
// - Commit the transaction.
// - Clean up by removing the test directory.
#[test]
fn table_manager_test() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize test directory and OxideDB instance
    let test_directory = PathBuf::from("tblmgrtest");
    let db = OxideDB::new_from_parameters(test_directory.clone(), 400, 8);
    let transaction = Arc::new(Mutex::new(db.new_transaction()));

    // Create a TableManager instance
    let table_manager = Arc::new(Mutex::new(
        TableManager::new(true, transaction.clone()).expect(&format!(
            "Failed to create TableManager.\nBacktrace: {:#?}",
            Backtrace::capture()
        )),
    ));

    // Define a schema with two fields
    let mut schema = Schema::new();
    schema.add_field("A".to_string(), FieldType::Integer, I32_SIZE);
    schema.add_field("B".to_string(), FieldType::VarChar, 9);
    let schema = Arc::new(schema);

    // Create a new table using the TableManager
    table_manager
        .lock()
        .unwrap()
        .create_table_from_table_manager("MyTable", schema.clone(), transaction.clone())
        .expect(&format!(
            "Failed to create table from TableManager.\nBacktrace: {:#?}",
            Backtrace::capture()
        ));

    // Fetch and check the layout of the table
    let layout = table_manager
        .lock()
        .unwrap()
        .get_layout("MyTable", transaction.clone())
        .expect(&format!(
            "Failed to get layout for 'MyTable'.\nBacktrace: {:#?}",
            Backtrace::capture()
        ));
    let slot_size = layout.get_slot_size();
    let schema2 = layout.get_schema();

    // Output table layout details
    println!("MyTable has slot size {}", slot_size);
    println!("Its fields are:");
    for field_name in schema2.get_fields() {
        let field_type = schema2.get_field_type(&field_name).expect(&format!(
            "Failed to get field type for '{}'.\nBacktrace: {:#?}",
            field_name,
            Backtrace::capture()
        ));
        let type_str = match field_type {
            FieldType::Integer => "int".to_string(),
            FieldType::VarChar => {
                let string_length = schema2.get_length(&field_name).expect(&format!(
                    "Failed to get string length for '{}'.\nBacktrace: {:#?}",
                    field_name,
                    Backtrace::capture()
                ));
                format!("varchar({})", string_length)
            }
        };
        println!("{}: {}", field_name, type_str);
    }

    // Commit the transaction
    transaction.lock().unwrap().commit().expect(&format!(
        "Failed to commit transaction.\nBacktrace: {:#?}",
        Backtrace::capture()
    ));

    // Clean up by removing the test directory
    remove_dir_all(test_directory).expect(&format!(
        "Failed to remove test directory.\nBacktrace: {:#?}",
        Backtrace::capture()
    ));

    Ok(())
}
