use crate::file::block_id::BlockId;
use crate::file::page::Page;
use crate::server::oxide_db::OxideDB;
use std::fs::remove_dir_all;
use std::path::PathBuf;

/// Tests various operations on a `Buffer`, including pinning, modifying, and flushing.
///
/// This test performs the following actions:
/// - Pins a block ("testfile", 1) and retrieves its content.
/// - Reads an integer at a specific offset (80), modifies it, and marks the buffer as modified.
/// - Unpins the modified buffer.
/// - Pins additional blocks, triggering a flush operation for the modified buffer.
/// - Re-pins the flushed block to verify if the modifications are preserved.
/// - Further modifies the block and marks it as modified.
/// - Reads the block directly from disk to confirm that the original modification was flushed correctly.
#[test]
fn buffer_test() -> Result<(), Box<dyn std::error::Error>> {
    // Create a test directory for OxideDB with a block size of 400 and only 3 buffers.
    let test_direcotry = PathBuf::from("buffertest");
    let db = OxideDB::new_for_debug(test_direcotry.clone(), 400, 3);
    let buffer_manager = db.get_buffer_manager();

    // Pin a block with id ("testfile", 1)
    let mut buffer1 = buffer_manager.pin(BlockId::new("testfile".to_string(), 1))?;
    let page1 = buffer1.get_contents();

    // Read, modify and write back an integer value at offset 80
    let number1 = page1.get_int(80)?;
    page1.set_int(80, number1 + 1)?;
    buffer1.set_modified(1, 0);
    println!("The new value is {}", number1 + 1);

    // Unpin the modified block
    buffer_manager.unpin(buffer1)?;

    // Pin additional blocks. One of these will trigger a flush for buffer1.
    let block2 = {
        let buffer2 = buffer_manager.pin(BlockId::new("testfile".to_string(), 2))?;
        buffer2.get_block().ok_or("Failed to get block")?.clone()
    };
    {
        let _buffer3 = buffer_manager.pin(BlockId::new("testfile".to_string(), 3))?;
    }
    {
        let _buffer4 = buffer_manager.pin(BlockId::new("testfile".to_string(), 4))?;
    }

    // Re-read the flushed block to check if the modification is preserved
    if let Some(mut buffer2) = buffer_manager.find_existing_buffer(&block2)? {
        buffer_manager.unpin(buffer2)?;
        buffer2 = buffer_manager.pin(BlockId::new("testfile".to_string(), 1))?;
        let page2 = buffer2.get_contents();
        let old_value = page2.get_int(80)?;
        println!("Old value before set: {}", old_value);

        // Modify the block again
        page2.set_int(80, 9999)?;
        let new_value = page2
            .get_int(80)
            .map_err(|_| "Failed to get integer at offset 80")?;
        buffer2.set_modified(1, 0);
        println!("New value after set: {}", new_value);

        // Read the block directly from disk to confirm that the original modification was flushed
        let file_manager = db
            .get_file_manager()
            .lock()
            .map_err(|_| "Failed to lock file manager")?;
        let mut page_to_check = Page::new_from_blocksize(400);
        let block_to_check = BlockId::new("testfile".to_string(), 1);
        file_manager.read(&block_to_check, &mut page_to_check)?;
        let flushed_value = page_to_check.get_int(80)?;

        assert_eq!(flushed_value, number1 + 1); // Assertion to verify flushed value
    }

    // Cleanup the test directory
    remove_dir_all(test_direcotry)?;
    Ok(())
}
