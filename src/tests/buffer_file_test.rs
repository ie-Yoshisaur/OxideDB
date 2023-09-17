use crate::file::block_id::BlockId;
use crate::file::page::Page;
use crate::server::oxide_db::OxideDB;
use std::backtrace::Backtrace;
use std::fs::remove_dir_all;
use std::path::PathBuf;

/// Tests buffer management operations in `BufferManager`.
///
/// This test does the following:
/// - Pins a block and writes a string and an integer to specific positions within the block.
/// - Marks the block as modified and unpins it.
/// - Pins the same block again and reads back the data to verify that it matches the written values.
#[test]
fn buffer_file_test() -> Result<(), Box<dyn std::error::Error>> {
    // Create a test directory for OxideDB with a block size of 400 and only 3 buffers.
    let test_directory = PathBuf::from("bufferfiletest");
    let db = OxideDB::new_for_debug(test_directory.clone(), 400, 8);

    // Obtain the BufferManager from OxideDB.
    let buffer_manager = db.get_buffer_manager().lock().expect(&format!(
        "Failed to lock BufferManager.\nBacktrace: {:#?}",
        Backtrace::capture()
    ));

    // Create a BlockId for testing.
    let block_id = BlockId::new("testfile".to_string(), 2);

    // Pin the block for writing data.
    let buffer1 = buffer_manager.pin(block_id.clone()).expect(&format!(
        "Failed to pin block.\nBacktrace: {:#?}",
        Backtrace::capture()
    ));

    let mut locked_buffer1 = buffer1.lock().expect(&format!(
        "Failed to lock buffer1.\nBacktrace: {:#?}",
        Backtrace::capture()
    ));
    let page1 = locked_buffer1.get_contents();

    // Define where to write the data within the block.
    let position1: usize = 88;

    // Write a string to the block at position 88.
    page1
        .set_string(position1, "abcdefghijklm")
        .expect(&format!(
            "Failed to set string.\nBacktrace: {:#?}",
            Backtrace::capture()
        ));

    // Calculate next position to write an integer.
    let size = Page::max_length("abcdefghijklm".len());
    let position2 = position1 + size;

    // Write an integer to the block at the calculated position.
    page1
        .set_int(position2, 345)
        .map_err(|_| format!("Failed to set int.\nBacktrace: {:#?}", Backtrace::capture()))?;

    // Mark the block as modified.
    locked_buffer1.set_modified(1, 0);

    // Unpin the block to make it available for other operations.
    drop(locked_buffer1);
    buffer_manager.unpin(buffer1);

    // Pin the same block again for reading the written data.
    let buffer2 = buffer_manager.pin(block_id.clone()).expect(&format!(
        "Failed to pin block again.\nBacktrace: {:#?}",
        Backtrace::capture()
    ));

    let mut locked_buffer2 = buffer2.lock().expect(&format!(
        "Failed to lock buffer2.\nBacktrace: {:#?}",
        Backtrace::capture()
    ));

    let page2 = locked_buffer2.get_contents();

    // Read and verify the string and integer written to the block.
    let read_int = page2
        .get_int(position2)
        .map_err(|_| format!("Failed to get int.\nBacktrace: {:#?}", Backtrace::capture()))?;
    let read_str = page2.get_string(position1).expect(&format!(
        "Failed to get string.\nBacktrace: {:#?}",
        Backtrace::capture()
    ));

    // Assertion checks
    assert_eq!(
        read_int,
        345,
        "Integer value mismatch at position {}. Backtrace: {:#?}",
        position2,
        Backtrace::capture()
    );

    assert_eq!(
        read_str,
        "abcdefghijklm",
        "String value mismatch at position {}. Backtrace: {:#?}",
        position1,
        Backtrace::capture()
    );

    println!("offset {} contains {}", position2, read_int);
    println!("offset {} contains {}", position1, read_str);

    // Unpin the block after reading.
    drop(locked_buffer2);
    buffer_manager.unpin(buffer2);

    // Remove test directory and cleanup.
    remove_dir_all(test_directory).expect(&format!(
        "Failed to remove test directory.\nBacktrace: {:#?}",
        Backtrace::capture()
    ));

    Ok(())
}
