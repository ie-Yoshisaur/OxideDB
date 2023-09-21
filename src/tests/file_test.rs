use crate::file::block_id::BlockId;
use crate::file::page::Page;
use crate::server::oxide_db::OxideDB;
use std::backtrace::Backtrace;
use std::fs::remove_dir_all;
use std::path::PathBuf;

/// Tests file read and write operations in `FileManager`.
///
/// This test does the following:
/// - Creates a new OxideDB instance for debugging with a given path and block size.
/// - Writes a string and an integer to a page.
/// - Writes the page to a file.
/// - Reads back the page from the file.
/// - Checks if the read values match the written values.
#[test]
fn file_test() -> Result<(), Box<dyn std::error::Error>> {
    // Create a test directory for OxideDB with a block size of 400 and only 3 buffers.
    let test_directory = PathBuf::from("filetest");
    let db = OxideDB::new_from_parameters(test_directory.clone(), 400, 8);
    let file_manager = db.get_file_manager().lock().unwrap();

    // Create a block ID for testing
    let block = BlockId::new("testfile".to_string(), 2);

    // Initialize a new page with the block size from FileManager
    let mut page1 = Page::new_from_blocksize(file_manager.get_block_size());

    // Define a position and set a string at that position in the page
    let position1: usize = 88;
    page1.set_string(position1, "abcdefghijklm")?;

    // Calculate the maximum length for the string and set an integer next to it
    let size = Page::max_length("abcdefghijklm".len());
    let position2 = position1 + size;
    page1.set_int(position2, 345)?;

    // Write the page to the block in the file
    file_manager.write(&block, &mut page1).expect(&format!(
        "Error writing to file.\nBacktrace: {:#?}",
        Backtrace::capture()
    ));

    // Initialize another page to read back the data
    let mut page2 = Page::new_from_blocksize(file_manager.get_block_size());

    // Read the page from the block in the file
    file_manager.read(&block, &mut page2).expect(&format!(
        "Error reading from file.\nBacktrace: {:#?}",
        Backtrace::capture()
    ));

    // Assert that the read integer matches the written integer
    assert_eq!(page2.get_int(position2)?, 345);

    // Assert that the read string matches the written string
    assert_eq!(page2.get_string(position1)?, "abcdefghijklm");

    // Cleanup the test directory.
    remove_dir_all(test_directory).expect(&format!(
        "Failed to remove test directory.\nBacktrace: {:#?}",
        Backtrace::capture()
    ));
    Ok(())
}
