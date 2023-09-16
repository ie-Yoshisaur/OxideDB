use crate::file::block_id::BlockId;
use crate::server::oxide_db::OxideDB;
use std::backtrace::Backtrace;
use std::fs::remove_dir_all;
use std::path::PathBuf;

/// Tests the behavior of the `BufferManager` under different scenarios.
///
/// This test performs the following actions:
/// - Pins the first three blocks and stores their IDs for later use.
/// - Unpins one of the pinned blocks to make space in the buffer.
/// - Pins two more blocks (including repinning one that was unpinned) and stores their IDs.
/// - Checks and displays the number of available buffers.
/// - Attempts to pin a block when no buffers are expected to be available, validating that an exception is raised.
/// - Frees up a buffer by unpinning a block and validates that pinning a new block is now possible.
/// - Outputs the final state of which blocks are pinned to which buffers.
#[test]
fn buffer_manager_test() {
    // Initialize OxideDB with only 3 buffers
    let test_directory = PathBuf::from("buffermanagertest");
    let db = OxideDB::new_for_debug(test_directory.clone(), 400, 3);
    let buffer_manager = db.get_buffer_manager();

    let mut buffers = vec![None; 6]; // Array to hold 6 optional Buffer references

    // Pin blocks 0, 1, and 2
    for i in 0..3 {
        let buffer = buffer_manager
            .lock()
            .unwrap()
            .pin(BlockId::new("testfile".to_string(), i))
            .expect(&format!(
                "Error pinning block.\nBacktrace: {:#?}",
                Backtrace::capture()
            ));
        buffers[i as usize] = Some(buffer);
    }

    // Unpin block 1
    buffer_manager
        .lock()
        .unwrap()
        .unpin(buffers[1].take().unwrap())
        .expect(&format!(
            "Error unpinning block.\nBacktrace: {:#?}",
            Backtrace::capture()
        ));

    // Pin block 0 again and repin block 1
    for i in 0..2 {
        let buffer = buffer_manager
            .lock()
            .unwrap()
            .pin(BlockId::new("testfile".to_string(), i))
            .expect(&format!(
                "Error pinning block.\nBacktrace: {:#?}",
                Backtrace::capture()
            ));
        buffers[3 + i as usize] = Some(buffer);
    }

    // Check and display the number of available buffers
    let available_buffers = *buffer_manager
        .lock()
        .unwrap()
        .get_number_available()
        .lock()
        .unwrap();
    println!("Available buffers: {}", available_buffers);
    assert_eq!(available_buffers, 0, "Expected 0 available buffers");

    // Try to pin block 3 (should fail)
    match buffer_manager
        .lock()
        .unwrap()
        .pin(BlockId::new("testfile".to_string(), 3))
    {
        Ok(_) => panic!("Should not be able to pin block 3"),
        Err(_) => println!("Exception: No available buffers\n"),
    }

    // Unpin block 2
    buffer_manager
        .lock()
        .unwrap()
        .unpin(buffers[2].take().unwrap())
        .expect(&format!(
            "Error unpinning block.\nBacktrace: {:#?}",
            Backtrace::capture()
        ));

    // Now pin block 3 (should succeed)
    buffers[5] = Some(
        buffer_manager
            .lock()
            .unwrap()
            .pin(BlockId::new("testfile".to_string(), 3))
            .expect(&format!(
                "Error pinning block.\nBacktrace: {:#?}",
                Backtrace::capture()
            )),
    );

    // Final buffer allocation
    println!("Final Buffer Allocation:");
    for (i, buffer) in buffers.iter().enumerate() {
        if let Some(buffer) = buffer {
            let locked_buffer = buffer.lock().expect(&format!(
                "Failed to lock buffer.\nBacktrace: {:#?}",
                Backtrace::capture()
            ));
            let block = locked_buffer.get_block().expect(&format!(
                "Error getting block.\nBacktrace: {:#?}",
                Backtrace::capture()
            ));
            println!("buffer[{}] pinned to block {}", i, block);

            // Assertions to check the final state of the buffers
            match i {
                0 => assert_eq!(block.get_block_number(), 0, "Expected block 0"),
                3 => assert_eq!(block.get_block_number(), 0, "Expected block 0"),
                4 => assert_eq!(block.get_block_number(), 1, "Expected block 1"),
                5 => assert_eq!(block.get_block_number(), 3, "Expected block 3"),
                _ => panic!("Unexpected block number for buffer[{}]", i),
            }
        }
    }

    // Cleanup
    remove_dir_all(test_directory).expect(&format!(
        "Failed to remove test directory.\nBacktrace: {:#?}",
        Backtrace::capture()
    ));
}
