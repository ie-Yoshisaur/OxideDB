use crate::file::block_id::BlockId;
use crate::file::page::Page;
use crate::server::oxide_db::OxideDB;
use std::backtrace::Backtrace;
use std::fs::remove_dir_all;
use std::mem::size_of;
use std::path::PathBuf;
use std::sync::Arc;

const I32_SIZE: usize = size_of::<i32>();

// Recovery test for OxideDB.
// This test simulates a scenario where data is initialized, modified, and then recovered.
// It checks if the recovery process correctly restores the data to its committed state.
//
// Recovery test for OxideDB.
// 1. Initialize: Two blocks (block0 and block1) are initialized with integers [0, 4, 8, 12, 16, 20] and strings "abc" and "def".
// 2. Modify: The integers in both blocks are incremented by 100, and the strings are changed to "uvw" and "xyz".
//    - Note: transaction3 modifies block1 but is NOT COMMITTED, so its changes will be UNDONE during recovery
// 3. Recover: The database is recovered, and all changes should be rolled back to the initial state.
#[test]
fn recovery_test() {
    let test_directory = PathBuf::from("recoverytest");
    {
        let db = Arc::new(OxideDB::new_for_debug(test_directory.clone(), 400, 8));
        let block0 = BlockId::new("testfile".to_string(), 0);
        let block1 = BlockId::new("testfile".to_string(), 1);

        initialize(db.clone(), block0.clone(), block1.clone());
        modify(db.clone(), block0.clone(), block1.clone());
    }
    {
        let db = Arc::new(OxideDB::new_for_debug(test_directory.clone(), 400, 8));
        let block0 = BlockId::new("testfile".to_string(), 0);
        let block1 = BlockId::new("testfile".to_string(), 1);

        recover(db.clone(), block0.clone(), block1.clone());
    }
    remove_dir_all(test_directory).expect(&format!(
        "Failed to remove test directory.\nBacktrace: {:#?}",
        Backtrace::capture()
    ));
}

// Initialize the database with sample data.
// This function pins two blocks and writes integers and strings to them.
//
// - Pins two blocks (block0 and block1)
// - Writes integers [0, 4, 8, 12, 16, 20] to both blocks
// - Writes strings "abc" to block0 and "def" to block1
fn initialize(db: Arc<OxideDB>, block0: BlockId, block1: BlockId) {
    // Create new transactions for initializing data
    let mut transaction0 = db.new_transaction();
    let mut transaction1 = db.new_transaction();

    // Pin the blocks to ensure they are loaded into the buffer pool
    transaction0.pin(block0.clone());
    transaction1.pin(block1.clone());

    // Initialize position variable for writing data
    let mut position = 0;

    // Loop to set integers in both blocks
    for _ in 0..6 {
        // Set integers in block0 using transaction0
        transaction0
            .set_int(block0.clone(), position, position, false)
            .expect(&format!(
                "Failed to set integer in transaction0.\nBacktrace: {:#?}",
                Backtrace::capture()
            ));
        // Set integers in block1 using transaction1
        transaction1
            .set_int(block1.clone(), position, position, false)
            .expect(&format!(
                "Failed to set integer in transaction1.\nBacktrace: {:#?}",
                Backtrace::capture()
            ));
        // Increment the position for the next integer
        position += I32_SIZE as i32;
    }

    // Set string "abc" in block0 at position 30
    transaction0
        .set_string(block0.clone(), 30, &"abc".to_string(), false)
        .expect(&format!(
            "Failed to set string in transaction0.\nBacktrace: {:#?}",
            Backtrace::capture()
        ));
    // Set string "def" in block1 at position 30
    transaction1
        .set_string(block1.clone(), 30, &"def".to_string(), false)
        .expect(&format!(
            "Failed to set string in transaction1.\nBacktrace: {:#?}",
            Backtrace::capture()
        ));

    // Commit both transactions to save the changes
    transaction0.commit();
    transaction1.commit();

    // Print and verify the initialized values
    let (init_vec0, init_vec1, init_str0, init_str1) =
        print_values("After Initialization:", &db, block0.clone(), block1.clone());
    assert_eq!(
        init_vec0,
        vec![0, 4, 8, 12, 16, 20],
        "Initialization failed for init_vec0.\nBacktrace: {:#?}",
        Backtrace::capture()
    );
    assert_eq!(
        init_vec1,
        vec![0, 4, 8, 12, 16, 20],
        "Initialization failed for init_vec1.\nBacktrace: {:#?}",
        Backtrace::capture()
    );
    assert_eq!(
        init_str0,
        "abc",
        "Initialization failed for init_str0.\nBacktrace: {:#?}",
        Backtrace::capture()
    );
    assert_eq!(
        init_str1,
        "def",
        "Initialization failed for init_str1.\nBacktrace: {:#?}",
        Backtrace::capture()
    );
}

// Modify the database to simulate changes that need to be recovered.
// - Increments the integers in both blocks by 100 (making them [100, 104, 108, 112, 116, 120])
// - Changes the strings to "uvw" in block0 and "xyz" in block1
// - Note: transaction3 modifies block1 but is NOT COMMITTED, so its changes will be UNDONE during recovery
fn modify(db: Arc<OxideDB>, block0: BlockId, block1: BlockId) {
    // Create new transactions for modifying data
    let mut transaction2 = db.new_transaction();
    let mut transaction3 = db.new_transaction();

    // Pin the blocks to ensure they are loaded into the buffer pool
    transaction2.pin(block0.clone());
    transaction3.pin(block1.clone());

    // Initialize position variable for updating data
    let mut position = 0;

    for _ in 0..6 {
        // Update integers in block0 using transaction2
        transaction2
            .set_int(block0.clone(), position, position + 100, true)
            .expect(&format!(
                "Failed to set integer in transaction2.\nBacktrace: {:#?}",
                Backtrace::capture()
            ));

        // Update integers in block1 using transaction3
        transaction3
            .set_int(block1.clone(), position, position + 100, true)
            .expect(&format!(
                "Failed to set integer in transaction3.\nBacktrace: {:#?}",
                Backtrace::capture()
            ));

        // Increment the position for the next integer
        position += I32_SIZE as i32;
    }

    // Update string in block0 to "uvw" at position 30
    transaction2
        .set_string(block0.clone(), 30, &"uvw".to_string(), true)
        .expect(&format!(
            "Failed to set string in transaction2.\nBacktrace: {:#?}",
            Backtrace::capture()
        ));

    // Update string in block1 to "xyz" at position 30
    transaction3
        .set_string(block1.clone(), 30, &"xyz".to_string(), true)
        .expect(&format!(
            "Failed to set string in transaction3.\nBacktrace: {:#?}",
            Backtrace::capture()
        ));

    // Flush the buffer to ensure data is written
    {
        let locked_buffer_manager = db.get_buffer_manager().lock().unwrap();
        locked_buffer_manager.flush_all(2).expect(&format!(
            "Failed to flush buffer for transaction 2.\nBacktrace: {:#?}",
            Backtrace::capture()
        ));
        locked_buffer_manager.flush_all(3).expect(&format!(
            "Failed to flush buffer for transaction 3.\nBacktrace: {:#?}",
            Backtrace::capture()
        ));
    }

    // Print and verify the modified values
    let (mod_vec0, mod_vec1, mod_str0, mod_str1) =
        print_values("After modification:", &db, block0.clone(), block1.clone());
    assert_eq!(
        mod_vec0,
        vec![100, 104, 108, 112, 116, 120],
        "Modification failed for mod_vec0.\nBacktrace: {:#?}",
        Backtrace::capture()
    );
    assert_eq!(
        mod_vec1,
        vec![100, 104, 108, 112, 116, 120],
        "Modification failed for mod_vec1.\nBacktrace: {:#?}",
        Backtrace::capture()
    );
    assert_eq!(
        mod_str0,
        "uvw",
        "Modification failed for mod_str0.\nBacktrace: {:#?}",
        Backtrace::capture()
    );
    assert_eq!(
        mod_str1,
        "xyz",
        "Modification failed for mod_str1.\nBacktrace: {:#?}",
        Backtrace::capture()
    );

    // Rollback changes made by transaction2
    transaction2.rollback();

    // Note: transaction3 is not committed or rolled back, so its changes will be undone during recovery

    // Print and verify the values
    let (rollback_vec0, rollback_vec1, rollback_str0, rollback_str1) =
        print_values("After rollback:", &db, block0.clone(), block1.clone());
    assert_eq!(
        rollback_vec0,
        vec![0, 4, 8, 12, 16, 20],
        "Rollback failed for rollback_vec0.\nBacktrace: {:#?}",
        Backtrace::capture()
    );
    assert_eq!(
        rollback_vec1,
        vec![100, 104, 108, 112, 116, 120],
        "Rollback failed for rollback_vec1.\nBacktrace: {:#?}",
        Backtrace::capture()
    );
    assert_eq!(
        rollback_str0,
        "abc",
        "Rollback failed for rollback_str0.\nBacktrace: {:#?}",
        Backtrace::capture()
    );
    assert_eq!(
        rollback_str1,
        "xyz",
        "Rollback failed for rollback_str1.\nBacktrace: {:#?}",
        Backtrace::capture()
    );
}

// Perform recovery to restore the database to its initial state.
// - Uses the `recover` method to roll back all changes to the initial state ([0, 4, 8, 12, 16, 20] and "abc" and "def")
// - Note: Changes from any uncommitted transactions (like transaction3) will be UNDONE.
fn recover(db: Arc<OxideDB>, block0: BlockId, block1: BlockId) {
    // Create a new transaction specifically for recovery
    let mut transaction = db.new_transaction();

    // Call the recover method to rollback all uncommitted changes
    transaction.recover().expect(&format!(
        "Failed to recover the transaction.\nBacktrace: {:#?}",
        Backtrace::capture()
    ));

    // Print and verify the recovered values
    let (recover_vec0, recover_vec1, recover_str0, recover_str1) =
        print_values("After recovery:", &db, block0.clone(), block1.clone());
    assert_eq!(
        recover_vec0,
        vec![0, 4, 8, 12, 16, 20],
        "Recovery failed for recover_vec0.\nBacktrace: {:#?}",
        Backtrace::capture()
    );
    assert_eq!(
        recover_vec1,
        vec![0, 4, 8, 12, 16, 20],
        "Recovery failed for recover_vec1.\nBacktrace: {:#?}",
        Backtrace::capture()
    );
    assert_eq!(
        recover_str0,
        "abc",
        "Recovery failed for recover_str0.\nBacktrace: {:#?}",
        Backtrace::capture()
    );
    assert_eq!(
        recover_str1,
        "def",
        "Recovery failed for recover_str1.\nBacktrace: {:#?}",
        Backtrace::capture()
    );
}

// Utility function to print and return the current values in the database.
// This helps in debugging and verifying the test outcomes.
fn print_values(
    msg: &str,
    db: &Arc<OxideDB>,
    block0: BlockId,
    block1: BlockId,
) -> (Vec<i32>, Vec<i32>, String, String) {
    println!("{}", msg);

    let block_size = db.get_file_manager().lock().unwrap().get_block_size();
    let mut page0 = Page::new_from_blocksize(block_size);
    let mut page1 = Page::new_from_blocksize(block_size);
    let mut vec0 = Vec::new();
    let mut vec1 = Vec::new();

    {
        let locked_file_manager = db.get_file_manager().lock().unwrap();
        locked_file_manager
            .read(&block0.clone(), &mut page0)
            .expect(&format!(
                "Failed to read from file for block0.\nBacktrace: {:#?}",
                Backtrace::capture()
            ));
        locked_file_manager
            .read(&block1.clone(), &mut page1)
            .expect(&format!(
                "Failed to read from file for block1.\nBacktrace: {:#?}",
                Backtrace::capture()
            ));
    }

    let mut position = 0;
    for _ in 0..6 {
        vec0.push(page0.get_int(position).expect(&format!(
            "Failed to get integer from page0 at position {}.\nBacktrace: {:#?}",
            position,
            Backtrace::capture()
        )));
        vec1.push(page1.get_int(position).expect(&format!(
            "Failed to get integer from page1 at position {}.\nBacktrace: {:#?}",
            position,
            Backtrace::capture()
        )));
        position += I32_SIZE;
    }

    let str0 = page0.get_string(30).expect(&format!(
        "Failed to get string from page0.\nBacktrace: {:#?}",
        Backtrace::capture()
    ));
    let str1 = page1.get_string(30).expect(&format!(
        "Failed to get string from page1.\nBacktrace: {:#?}",
        Backtrace::capture()
    ));

    (vec0, vec1, str0, str1)
}
