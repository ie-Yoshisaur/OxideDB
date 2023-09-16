use crate::file::block_id::BlockId;
use crate::server::oxide_db::OxideDB;
use crate::transaction::transaction::Transaction;
use fs::remove_dir_all;
use std::fs;
use std::path::PathBuf;

#[test]
fn transaction_test() {
    let test_directory = PathBuf::from("transactiontest");
    let db = OxideDB::new_for_debug(test_directory.clone(), 400, 8);
    let file_manager = db.get_file_manager();
    let log_manager = db.get_log_manager();
    let buffer_manager = db.get_buffer_manager();
    let lock_table = db.get_lock_table();

    let mut transaction0 = Transaction::new(
        file_manager.clone(),
        log_manager.clone(),
        buffer_manager.clone(),
        lock_table.clone(),
    );
    let block = BlockId::new("testfile".to_string(), 1);
    transaction0.pin(block.clone());
    transaction0.set_int(block.clone(), 80, 1, false);
    transaction0.set_string(block.clone(), 40, &"one".to_string(), false);
    transaction0.commit();

    let mut transaction1 = Transaction::new(
        file_manager.clone(),
        log_manager.clone(),
        buffer_manager.clone(),
        lock_table.clone(),
    );
    transaction1.pin(block.clone());
    let int_value = transaction1.get_int(block.clone(), 80).unwrap();
    let string_value = transaction1.get_string(block.clone(), 40).unwrap();
    println!("initial value at location 80 = {}", int_value);
    println!("initial value at location 40 = {}", string_value);
    assert_eq!(int_value, 1);
    assert_eq!(string_value, "one");

    let new_int_value = int_value + 1;
    let new_string_value = format!("{}!", string_value);
    transaction1.set_int(block.clone(), 80, new_int_value, true);
    transaction1.set_string(block.clone(), 40, &new_string_value, true);
    transaction1.commit();

    let mut transaction2 = Transaction::new(
        file_manager.clone(),
        log_manager.clone(),
        buffer_manager.clone(),
        lock_table.clone(),
    );
    transaction2.pin(block.clone());
    println!(
        "new value at location 80 = {}",
        transaction2.get_int(block.clone(), 80).unwrap()
    );
    println!(
        "new value at location 40 = {}",
        transaction2.get_string(block.clone(), 40).unwrap()
    );
    assert_eq!(transaction2.get_int(block.clone(), 80).unwrap(), 2);
    assert_eq!(transaction2.get_string(block.clone(), 40).unwrap(), "one!");

    transaction2.set_int(block.clone(), 80, 9999, true);
    println!(
        "pre-rollback value at location 80 = {}",
        transaction2.get_int(block.clone(), 80).unwrap()
    );
    assert_eq!(transaction2.get_int(block.clone(), 80).unwrap(), 9999);
    transaction2.rollback();

    let mut transaction3 = Transaction::new(
        file_manager.clone(),
        log_manager.clone(),
        buffer_manager.clone(),
        lock_table.clone(),
    );
    transaction3.pin(block.clone());
    println!(
        "post-rollback at location 80 = {}",
        transaction3.get_int(block.clone(), 80).unwrap()
    );
    assert_eq!(transaction3.get_int(block.clone(), 80).unwrap(), 2);
    transaction3.commit();

    // Clean up: remove the test directory
    remove_dir_all(test_directory).expect("Failed to remove test directory");
}
