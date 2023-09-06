use crate::file::block_id::BlockId;
use crate::file::page::Page;
use crate::server::oxide_db::OxideDB;
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
    let buffer_manager = db.get_buffer_manager();

    // Pin the block with id ("testfile", 2) to write data
    let block_id = BlockId::new("testfile".to_string(), 2);
    let mut buffer1 = buffer_manager.pin(block_id.clone())?;
    let page1 = buffer1.get_contents();
    let position1: usize = 88; // Starting position for writing data

    // Write a string and an integer to the pinned block
    page1.set_string(position1, "abcdefghijklm")?;
    let size = Page::max_length("abcdefghijklm".len());
    let position2 = position1 + size;
    page1.set_int(position2, 345)?;

    // Mark the block as modified
    buffer1.set_modified(1, 0);

    // Unpin the block after modification
    buffer_manager.unpin(buffer1)?;

    // Pin the same block again to read the data
    let mut buffer2 = buffer_manager.pin(block_id.clone())?;
    let page2 = buffer2.get_contents();

    // Read and verify the set values
    let read_int = page2.get_int(position2)?;
    let read_str = page2.get_string(position1)?;

    // Verify that the read integer is as expected
    assert_eq!(
        read_int, 345,
        "Integer value mismatch at position {}",
        position2
    );

    // Verify that the read string is as expected
    assert_eq!(
        read_str, "abcdefghijklm",
        "String value mismatch at position {}",
        position1
    );

    println!("offset {} contains {}", position2, read_int);
    println!("offset {} contains {}", position1, read_str);

    // Unpin the block after reading
    buffer_manager.unpin(buffer2)?;

    // Clean up the test environment
    remove_dir_all(test_directory)?;
    Ok(())
}
