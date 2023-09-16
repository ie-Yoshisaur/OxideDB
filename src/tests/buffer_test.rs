use crate::file::block_id::BlockId;
use crate::file::page::Page;
use crate::server::oxide_db::OxideDB;
use std::backtrace::Backtrace;
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
    // Initialize OxideDB with only 3 buffers
    let test_directory = PathBuf::from("buffertest");
    let db = OxideDB::new_for_debug(test_directory.clone(), 400, 3);
    let buffer_manager = db.get_buffer_manager().lock().expect(&format!(
        "Locking Buffer Manager Failed\nBacktrace: {:?}",
        Backtrace::capture()
    ));

    // Pin buffer 1
    let block1 = BlockId::new("testfile".to_string(), 1);
    let buffer1 = buffer_manager.pin(block1.clone()).expect(&format!(
        "Error Pinning Block 1\nBacktrace: {:?}",
        Backtrace::capture()
    ));
    let number1 = {
        let mut locked_buffer1 = buffer1.lock().unwrap(); // handle the lock
        let page1 = locked_buffer1.get_contents();
        let mut number1 = page1.get_int(80).expect(&format!(
            "Error Reading int at Offset 80 in Block 1\nBacktrace: {:?}",
            Backtrace::capture()
        ));
        number1 += 1;
        page1.set_int(80, number1).expect(&format!(
            "Error Writing int at Offset 80 in Block 1\nBacktrace: {:?}",
            Backtrace::capture()
        ));
        locked_buffer1.set_modified(1, 0); // Placeholder values
        println!("The new value is {}", number1 + 1);
        number1
    };

    // Unpin the modified block
    buffer_manager.unpin(buffer1).expect(&format!(
        "Error Unpinning Block 1\nBacktrace: {:?}",
        Backtrace::capture()
    ));

    // Pin additional blocks. One of these will trigger a flush for buffer1.
    let block2 = BlockId::new("testfile".to_string(), 2);
    let mut buffer2 = buffer_manager.pin(block2).expect(&format!(
        "Error Pinning Block 2\nBacktrace: {:?}",
        Backtrace::capture()
    ));
    let block3 = BlockId::new("testfile".to_string(), 3);
    let _buffer3 = buffer_manager.pin(block3).expect(&format!(
        "Error Pinning Block 3\nBacktrace: {:?}",
        Backtrace::capture()
    ));
    let block4 = BlockId::new("testfile".to_string(), 4);
    let _buffer4 = buffer_manager.pin(block4).expect(&format!(
        "Error Pinning Block 4\nBacktrace: {:?}",
        Backtrace::capture()
    ));

    // Unpin buffer 2 and pin buffer 1 again
    buffer_manager.unpin(buffer2).expect(&format!(
        "Error Unpinning Block 2\nBacktrace: {:?}",
        Backtrace::capture()
    ));
    buffer2 = buffer_manager.pin(block1).expect(&format!(
        "Error Pinning Block 1\nBacktrace: {:?}",
        Backtrace::capture()
    ));
    {
        let mut locked_buffer2 = buffer2.lock().unwrap();
        let page2 = locked_buffer2.get_contents();
        let old_value = page2.get_int(80).expect(&format!(
            "Error Reading int at Offset 80 in Block 1\nBacktrace: {:?}",
            Backtrace::capture()
        ));

        // Assert that the value read is the same as the old value
        assert_eq!(
            old_value, number1,
            "Read value does not match the old value"
        );

        page2.set_int(80, 9999).expect(&format!(
            "Error Writing int at Offset 80 in Block 1\nBacktrace: {:?}",
            Backtrace::capture()
        )); // This modification
        let new_value = page2
            .get_int(80)
            .map_err(|_| "Failed to get integer at offset 80")?;
        // Assert that the value read is the same as the new value
        assert_eq!(new_value, 9999, "Read value does not match the new value");
        locked_buffer2.set_modified(1, 0); // won't get written to disk

        // Read the block directly from disk to confirm that the original modification was flushed
        let file_manager = db
            .get_file_manager()
            .lock()
            .map_err(|_| "Failed to lock file manager")?;
        let mut page_to_check = Page::new_from_blocksize(400);
        let block_to_check = BlockId::new("testfile".to_string(), 1);
        file_manager.read(&block_to_check, &mut page_to_check)?;
        let value_on_disk = page_to_check.get_int(80)?;
        assert_eq!(value_on_disk, number1);
    }

    // Cleanup
    remove_dir_all(test_directory)?;

    Ok(())
}
