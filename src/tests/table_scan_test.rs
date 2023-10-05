use crate::record::layout::Layout;
use crate::record::schema::Schema;
use crate::record::table_scan::TableScan;
use crate::server::oxide_db::OxideDB;
use std::fs::remove_dir_all;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

/// Tests the table scan operations in `TableScan`.
///
/// This test performs the following steps:
/// - Creates a new OxideDB instance and a new transaction.
/// - Initializes a Schema and a Layout with fields "A" (int) and "B" (string).
/// - Creates a new TableScan object.
/// - Inserts 50 records into the table with incrementing values for fields "A" and "B".
/// - Deletes records where the value of field "A" is less than 25.
/// - Checks if the remaining records have "A" values greater than or equal to 25.
/// - Commits the transaction and cleans up the test directory.
#[test]
fn table_scan_test() {
    // Initialize OxideDB and create a new transaction.
    let test_directory = PathBuf::from("tablescantest");
    let block_size = 400;
    let db = OxideDB::new_from_parameters(test_directory.clone(), block_size, 8);
    let transaction = Arc::new(Mutex::new(db.new_transaction()));

    // Initialize Schema and Layout.
    let mut schema = Schema::new();
    schema.add_int_field("A".to_string());
    schema.add_string_field("B".to_string(), 9);
    let schema = Arc::new(Mutex::new(schema));
    let layout = Arc::new(Layout::new(schema.clone()).unwrap());

    // Print field offsets
    for field_name in layout.get_schema().lock().unwrap().get_fields() {
        let offset = layout.get_offset(&field_name).unwrap();
        println!("{} has offset {}", field_name, offset);
    }

    // Create TableScan and insert 50 records with incrementing values
    println!("Filling the table with 50 records.");
    let mut table_scan = TableScan::new(transaction.clone(), "T", layout.clone()).unwrap();
    let mut n = 0;
    for _ in 0..50 {
        table_scan.insert().unwrap();
        table_scan.set_int("A", n).unwrap();
        table_scan.set_string("B", format!("rec{}", n)).unwrap();
        println!(
            "inserting into slot {}: {{ {}, {} }}",
            table_scan.get_record_id(),
            n,
            format!("rec{}", n)
        );
        n += 1;
    }

    let slot_size = layout.get_slot_size();
    let max_slots_per_block = (block_size / slot_size) - 1;

    let mut expected_block_id = 0;
    let mut expected_slot = 0;

    // Delete records where A < 25
    println!("Deleting these records, whose A-values are less than 25.");
    let mut count = 0;
    table_scan.before_first();
    while table_scan.next().unwrap() {
        let a = table_scan.get_int("A").unwrap();
        if a < 25 {
            count += 1;
            println!(
                "slot {}: {{ {}, {} }}",
                table_scan.get_record_id(),
                a,
                table_scan.get_string("B").unwrap()
            );
            table_scan.delete();
            let record_id = table_scan.get_record_id();
            let actual_block_id = record_id.get_block_number();
            let actual_slot = record_id.get_slot_number();

            assert_eq!(expected_block_id, actual_block_id);
            assert_eq!(expected_slot, actual_slot);

            if expected_slot >= max_slots_per_block as i32 {
                expected_block_id += 1;
                expected_slot = 0;
            } else {
                expected_slot += 1;
            }
        }
    }
    println!("{} values under 25 were deleted.", count);

    // Print remaining records
    println!("Here are the remaining records.");
    table_scan.before_first();
    while table_scan.next().unwrap() {
        let a = table_scan.get_int("A").unwrap();
        let b = table_scan.get_string("B").unwrap();
        println!("slot {}: {{ {}, {} }}", table_scan.get_record_id(), a, b);
        let record_id = table_scan.get_record_id();
        let actual_block_id = record_id.get_block_number();
        let actual_slot = record_id.get_slot_number();

        assert_eq!(expected_block_id, actual_block_id);
        assert_eq!(expected_slot, actual_slot);

        if expected_slot >= max_slots_per_block as i32 {
            expected_block_id += 1;
            expected_slot = 0;
        } else {
            expected_slot += 1;
        }
    }

    table_scan.close();
    transaction.lock().unwrap().commit().unwrap();
    remove_dir_all(test_directory).unwrap();
}
