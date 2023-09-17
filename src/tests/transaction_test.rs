use crate::file::block_id::BlockId;
use crate::server::oxide_db::OxideDB;
use crate::transaction::transaction::Transaction;
use fs::remove_dir_all;
use std::backtrace::Backtrace;
use std::fs;
use std::path::PathBuf;

/// This test suite is designed to validate the transactional behavior of the OxideDB system.
/// Specifically, it tests whether transactions can correctly read and write data,
/// and whether commit and rollback operations function as expected.
#[test]
fn transaction_test() {
    // Setup: Initialize the database and related managers
    let test_directory = PathBuf::from("transactiontest");
    let db = OxideDB::new_for_debug(test_directory.clone(), 400, 8);
    let file_manager = db.get_file_manager();
    let log_manager = db.get_log_manager();
    let buffer_manager = db.get_buffer_manager();
    let lock_table = db.get_lock_table();

    // Transaction 0: Sets initial values in a block
    let mut transaction0 = Transaction::new(
        file_manager.clone(),
        log_manager.clone(),
        buffer_manager.clone(),
        lock_table.clone(),
    );
    let block = BlockId::new("testfile".to_string(), 1);
    transaction0.pin(block.clone());
    transaction0
        .set_int(block.clone(), 80, 1, false)
        .expect(&format!(
            "Transaction 0: Error setting int.\nBacktrace: {:#?}",
            Backtrace::capture()
        ));
    transaction0
        .set_string(block.clone(), 40, &"one".to_string(), false)
        .expect(&format!(
            "Transaction 0: Error setting string.\nBacktrace: {:#?}",
            Backtrace::capture()
        ));

    // Commit the changes made by Transaction 0
    transaction0.commit();

    // Transaction 1: Reads the initial values set by Transaction 0
    let mut transaction1 = Transaction::new(
        file_manager.clone(),
        log_manager.clone(),
        buffer_manager.clone(),
        lock_table.clone(),
    );
    transaction1.pin(block.clone());
    let int_value = transaction1
        .get_int(block.clone(), 80)
        .expect(&format!(
            "Transaction 1: Error getting int.\nBacktrace: {:#?}",
            Backtrace::capture()
        ))
        .expect(&format!(
            "Expected Some(int), got None.\nBacktrace: {:#?}",
            Backtrace::capture()
        ));
    let string_value = transaction1
        .get_string(block.clone(), 40)
        .expect(&format!(
            "Transaction 1: Error getting String.\nBacktrace: {:#?}",
            Backtrace::capture()
        ))
        .expect(&format!(
            "Expected Some(String), got None.\nBacktrace: {:#?}",
            Backtrace::capture()
        ));
    println!("initial value at location 80 = {}", int_value);
    println!("initial value at location 40 = {}", string_value);
    assert_eq!(
        int_value,
        1,
        "Assertion failed for int_value.\nBacktrace: {:#?}",
        Backtrace::capture()
    );
    assert_eq!(
        string_value,
        "one",
        "Assertion failed for string_value.\nBacktrace: {:#?}",
        Backtrace::capture()
    );

    let new_int_value = int_value + 1;
    let new_string_value = format!("{}!", string_value);
    transaction1
        .set_int(block.clone(), 80, new_int_value, true)
        .expect(&format!(
            "Transaction 1: Error setting int.\nBacktrace: {:#?}",
            Backtrace::capture()
        ));
    transaction1
        .set_string(block.clone(), 40, &new_string_value, true)
        .expect(&format!(
            "Transaction 1: Error setting string.\nBacktrace: {:#?}",
            Backtrace::capture()
        ));

    // Modify and commit the values read by Transaction 1
    transaction1.commit();

    // Transaction 2: Reads the modified values and performs further modifications
    let mut transaction2 = Transaction::new(
        file_manager.clone(),
        log_manager.clone(),
        buffer_manager.clone(),
        lock_table.clone(),
    );
    transaction2.pin(block.clone());
    let new_int_value_2 = transaction2
        .get_int(block.clone(), 80)
        .expect(&format!(
            "Transaction 2: Error getting int.\nBacktrace: {:#?}",
            Backtrace::capture()
        ))
        .expect("Expected Some(int), got None");

    let new_string_value_2 = transaction2
        .get_string(block.clone(), 40)
        .expect(&format!(
            "Transaction 2: Error getting string.\nBacktrace: {:#?}",
            Backtrace::capture()
        ))
        .expect("Expected Some(string), got None");

    println!("new value at location 80 = {}", new_int_value_2);
    println!("new value at location 40 = {}", new_string_value_2);
    assert_eq!(
        new_int_value_2,
        2,
        "Assertion failed for new_int_value_2.\nBacktrace: {:#?}",
        Backtrace::capture()
    );
    assert_eq!(
        new_string_value_2,
        "one!",
        "Assertion failed for new_string_value_2.\nBacktrace: {:#?}",
        Backtrace::capture()
    );

    transaction2
        .set_int(block.clone(), 80, 9999, true)
        .expect(&format!(
            "Transaction 2: Error setting int.\nBacktrace: {:#?}",
            Backtrace::capture()
        ));

    let pre_rollback_value = transaction2
        .get_int(block.clone(), 80)
        .expect(&format!(
            "Transaction 2: Error getting int.\nBacktrace: {:#?}",
            Backtrace::capture()
        ))
        .expect("Expected Some(int), got None");

    println!("pre-rollback value at location 80 = {}", pre_rollback_value);
    assert_eq!(
        pre_rollback_value,
        9999,
        "Assertion failed for pre_rollback_value.\nBacktrace: {:#?}",
        Backtrace::capture()
    );

    // Rollback the changes made by Transaction 2
    transaction2.rollback();

    // Transaction 3: Verifies that the rollback in Transaction 2 was successful
    let mut transaction3 = Transaction::new(
        file_manager.clone(),
        log_manager.clone(),
        buffer_manager.clone(),
        lock_table.clone(),
    );
    transaction3.pin(block.clone());

    let post_rollback_value = transaction3
        .get_int(block.clone(), 80)
        .expect(&format!(
            "Transaction 3: Error getting int.\nBacktrace: {:#?}",
            Backtrace::capture()
        ))
        .expect("Expected Some(int), got None");

    println!("post-rollback at location 80 = {}", post_rollback_value);
    assert_eq!(
        post_rollback_value,
        2,
        "Assertion failed for post_rollback_value.\nBacktrace: {:#?}",
        Backtrace::capture()
    );

    // Commit the final state of the block
    transaction3.commit();

    // Clean up: remove the test directory
    remove_dir_all(test_directory).expect(&format!(
        "Failed to remove test directory.\nBacktrace: {:#?}",
        Backtrace::capture()
    ));
}
