use crate::file::block_id::BlockId;
use crate::server::oxide_db::OxideDB;
use crate::transaction::transaction::Transaction;
use std::backtrace::Backtrace;
use std::fs::remove_dir_all;
use std::path::PathBuf;
use std::thread;
use std::thread::sleep;
use std::time::Duration;

/// This test simulates a multi-threaded scenario to test concurrency control mechanisms.
/// It aims to ensure that multiple transactions can safely acquire and release locks without causing data races or deadlocks.
/// Three threads (A, B, C) are spawned to perform different operations on two blocks (block1 and block2).
/// Each thread tries to acquire different types of locks (shared or exclusive) on these blocks.
/// The test verifies that all threads can complete their transactions without any issues.
///
/// Thread A performs the following operations:
/// 1. Pins block1 and block2.
/// 2. Acquires a shared lock on block1.
/// 3. Waits for 1 second.
/// 4. Acquires a shared lock on block2.
/// 5. Commits the transaction.
///
/// Thread B performs the following operations:
/// 1. Pins block1 and block2.
/// 2. Acquires an exclusive lock on block2.
/// 3. Waits for 1 second.
/// 4. Acquires a shared lock on block1.
/// 5. Commits the transaction.
///
/// Thread C performs the following operations:
/// 1. Pins block1 and block2.
/// 2. Waits for 0.5 seconds.
/// 3. Acquires an exclusive lock on block1.
/// 4. Waits for 1 second.
/// 5. Acquires a shared lock on block2.
/// 6. Commits the transaction.
#[test]
fn concurrency_test() {
    let test_directory = PathBuf::from("concurrencytest");
    let db = OxideDB::new_for_debug(test_directory.clone(), 400, 8);
    let file_manager = db.get_file_manager();
    let log_manager = db.get_log_manager();
    let buffer_manager = db.get_buffer_manager();
    let lock_table = db.get_lock_table();

    let handle_a = thread::Builder::new()
        .name("Thread-A".to_string())
        .spawn({
            let file_manager = file_manager.clone();
            let log_manager = log_manager.clone();
            let buffer_manager = buffer_manager.clone();
            let lock_table = lock_table.clone();

            move || {
                let mut transaction_a =
                    Transaction::new(file_manager, log_manager, buffer_manager, lock_table);
                let block1 = BlockId::new("testfile".to_string().to_string(), 1);
                let block2 = BlockId::new("testfile".to_string().to_string(), 2);
                transaction_a.pin(block1.clone());
                transaction_a.pin(block2.clone());
                println!("Transaction A: request slock 1");
                transaction_a.get_int(block1.clone(), 0).expect(&format!(
                    "Transaction A: Error getting int from block1.\nBacktrace: {:#?}",
                    Backtrace::capture()
                ));
                println!("Transaction A: receive slock 1");
                sleep(Duration::from_millis(1000));
                println!("Transaction A: request slock 2");
                transaction_a.get_int(block2.clone(), 0).expect(&format!(
                    "Transaction A: Error getting int from block2.\nBacktrace: {:#?}",
                    Backtrace::capture()
                ));
                println!("Transaction A: receive slock 2");
                transaction_a.commit();
                println!("Transaction A: commit");
            }
        })
        .expect(&format!(
            "Error spawning Thread-A.\nBacktrace: {:#?}",
            Backtrace::capture()
        ));

    let handle_b = thread::Builder::new()
        .name("Thread-B".to_string())
        .spawn({
            let file_manager = file_manager.clone();
            let log_manager = log_manager.clone();
            let buffer_manager = buffer_manager.clone();
            let lock_table = lock_table.clone();

            move || {
                let mut transaction_b =
                    Transaction::new(file_manager, log_manager, buffer_manager, lock_table);
                let block1 = BlockId::new("testfile".to_string(), 1);
                let block2 = BlockId::new("testfile".to_string(), 2);
                transaction_b.pin(block1.clone());
                transaction_b.pin(block2.clone());
                println!("Transaction B: request xlock 2");
                transaction_b
                    .set_int(block2.clone(), 0, 0, false)
                    .expect(&format!(
                        "Transaction B: Error setting int in block2.\nBacktrace: {:#?}",
                        Backtrace::capture()
                    ));
                println!("Transaction B: receive xlock 2");
                sleep(Duration::from_millis(1000));
                println!("Transaction B: request slock 1");
                transaction_b.get_int(block1.clone(), 0).expect(&format!(
                    "Transaction B: Error getting int from block1.\nBacktrace: {:#?}",
                    Backtrace::capture()
                ));
                println!("Transaction B: receive slock 1");
                transaction_b.commit();
                println!("Transaction B: commit");
            }
        })
        .expect(&format!(
            "Error spawning Thread-B.\nBacktrace: {:#?}",
            Backtrace::capture()
        ));
    let handle_c = thread::Builder::new()
        .name("Thread-C".to_string())
        .spawn({
            let file_manager = file_manager.clone();
            let log_manager = log_manager.clone();
            let buffer_manager = buffer_manager.clone();
            let lock_table = lock_table.clone();

            move || {
                let mut transaction_c =
                    Transaction::new(file_manager, log_manager, buffer_manager, lock_table);
                let block1 = BlockId::new("testfile".to_string(), 1);
                let block2 = BlockId::new("testfile".to_string(), 2);
                transaction_c.pin(block1.clone());
                transaction_c.pin(block2.clone());
                sleep(Duration::from_millis(500));
                println!("Transaction C: request xlock 1");
                transaction_c
                    .set_int(block1.clone(), 0, 0, false)
                    .expect(&format!(
                        "Transaction C: Error setting int in block1.\nBacktrace: {:#?}",
                        Backtrace::capture()
                    ));
                println!("Transaction C: receive xlock 1");
                sleep(Duration::from_millis(1000));
                println!("Transaction C: request slock 2");
                transaction_c.get_int(block2.clone(), 0).expect(&format!(
                    "Transaction C: Error getting int from block2.\nBacktrace: {:#?}",
                    Backtrace::capture()
                ));
                println!("Transaction C: receive slock 2");
                transaction_c.commit();
                println!("Transaction C: commit");
            }
        })
        .expect(&format!(
            "Error spawning Thread-B.\nBacktrace: {:#?}",
            Backtrace::capture()
        ));

    handle_a.join().expect(&format!(
        "Error joining Thread-A.\nBacktrace: {:#?}",
        Backtrace::capture()
    ));
    handle_b.join().expect(&format!(
        "Error joining Thread-A.\nBacktrace: {:#?}",
        Backtrace::capture()
    ));
    handle_c.join().expect(&format!(
        "Error joining Thread-A.\nBacktrace: {:#?}",
        Backtrace::capture()
    ));
    remove_dir_all(test_directory).expect(&format!(
        "Failed to remove test directory.\nBacktrace: {:#?}",
        Backtrace::capture()
    ));
}
