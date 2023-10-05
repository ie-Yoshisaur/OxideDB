use crate::file::block_id::BlockId;
use crate::record::layout::Layout;
use crate::record::record_page::RecordPage;
use crate::record::schema::Schema;
use crate::server::oxide_db::OxideDB;
use std::backtrace::Backtrace;
use std::fs::remove_dir_all;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

/// Tests the record page operations in `RecordPage`.
///
/// This test performs the following steps:
/// - Creates a new OxideDB instance and a new transaction.
/// - Initializes a Schema and a Layout with fields "A" (int) and "B" (string).
/// - Creates a new RecordPage and formats it.
/// - Inserts records into the page until it's full.
/// - Deletes records where the value of field "A" is less than 10.
/// - Checks if the remaining records have "A" values greater than or equal to 10.
/// - Commits the transaction and cleans up the test directory.
#[test]
fn record_test() {
    // Initialize OxideDB and create a new transaction.
    let test_directory = PathBuf::from("recordtest");
    let db = OxideDB::new_from_parameters(test_directory.clone(), 400, 8);
    let transaction = Arc::new(Mutex::new(db.new_transaction()));

    // Initialize Schema and Layout.
    let mut schema = Schema::new();
    schema.add_int_field("A".to_string());
    schema.add_string_field("B".to_string(), 9);
    let schema = Arc::new(Mutex::new(schema));
    let layout = Arc::new(Layout::new(schema.clone()).expect(&format!(
        "Layout creation failed.\nBacktrace: {:#?}",
        Backtrace::capture()
    )));

    for field_name in layout.get_schema().lock().unwrap().get_fields() {
        let offset = layout.get_offset(&field_name).expect(&format!(
            "Field not found.\nBacktrace: {:#?}",
            Backtrace::capture()
        ));
        println!("{} has offset {}", field_name, offset);
    }

    let block = BlockId::new("testfile".to_string(), 0);
    let mut record_page = RecordPage::new(transaction.clone(), block, layout.clone());
    record_page.format().expect(&format!(
        "Page formatting failed.\nBacktrace: {:#?}",
        Backtrace::capture()
    ));

    // Insert records into the page until it's full
    let mut n = 0;
    let mut initial_slot = -1;
    let mut slot = record_page.insert_after(&mut initial_slot).expect(&format!(
        "Insert after failed.\nBacktrace: {:#?}",
        Backtrace::capture()
    ));

    loop {
        if slot < 0 {
            break;
        }

        record_page.set_int(slot as usize, "A", n).expect(&format!(
            "Set int failed.\nBacktrace: {:#?}",
            Backtrace::capture()
        ));
        record_page
            .set_string(slot as usize, "B", format!("rec{}", n))
            .expect(&format!(
                "Set string failed.\nBacktrace: {:#?}",
                Backtrace::capture()
            ));
        println!("inserting into slot {}: {{ {}, rec{} }}", slot, n, n);
        n += 1;
        slot = record_page.insert_after(&mut slot).expect(&format!(
            "Insert after failed.\nBacktrace: {:#?}",
            Backtrace::capture()
        ));
    }

    // Delete records where A < 10
    println!("Deleting these records, whose A-values are less than 10.");
    let mut count = 0;
    let mut slot = -1;

    loop {
        slot = record_page.next_after(&mut slot).expect(&format!(
            "Next after failed.\nBacktrace: {:#?}",
            Backtrace::capture()
        ));

        if slot < 0 {
            break;
        }

        let a = record_page.get_int(slot as usize, "A").expect(&format!(
            "Get int failed.\nBacktrace: {:#?}",
            Backtrace::capture()
        ));
        let b = record_page.get_string(slot as usize, "B").expect(&format!(
            "Get string failed.\nBacktrace: {:#?}",
            Backtrace::capture()
        ));
        if a < 10 {
            count += 1;
            println!("slot {}: {{ {}, {} }}", slot, a, b);
            record_page.delete(slot as usize).expect(&format!(
                "Delete failed. \nBacktrace: {:#?}",
                Backtrace::capture()
            ));
        }
    }

    println!("{} values under 10 were deleted.", count);

    // Check remaining records
    println!("Here are the remaining records.");
    let mut slot = -1;

    loop {
        slot = record_page.next_after(&mut slot).expect(&format!(
            "Next after failed.\nBacktrace: {:#?}",
            Backtrace::capture()
        ));

        if slot < 0 {
            break;
        }

        let a = record_page.get_int(slot as usize, "A").expect(&format!(
            "Get int failed.\nBacktrace: {:#?}",
            Backtrace::capture()
        ));
        let b = record_page.get_string(slot as usize, "B").expect(&format!(
            "Get string failed.\nBacktrace: {:#?}",
            Backtrace::capture()
        ));

        println!("slot {}: {{ {}, {} }}", slot, a, b);
        assert!(
            a >= 10,
            "Assertion failed for remaining records.\nBacktrace: {:#?}",
            Backtrace::capture()
        );
    }

    transaction.lock().unwrap().commit().expect(&format!(
        "Transaction commit failed.\nBacktrace: {:#?}",
        Backtrace::capture()
    ));
    remove_dir_all(test_directory).expect(&format!(
        "Failed to remove test directory.\nBacktrace: {:#?}",
        Backtrace::capture()
    ));
}
