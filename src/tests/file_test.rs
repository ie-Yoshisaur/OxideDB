use crate::file::block_id::BlockId;
use crate::file::file_manager::FileManager;
use crate::file::page::Page;
use crate::server::oxide_db::OxideDB;
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
    // Initialize a new OxideDB instance for debugging
    let db = OxideDB::new_for_debug(PathBuf::from("filetest"), 400);

    // Acquire a FileManager to perform file operations
    let file_manager = db.get_file_manager();

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
    file_manager.write(&block, &mut page1)?;

    // Initialize another page to read back the data
    let mut page2 = Page::new_from_blocksize(file_manager.get_block_size());

    // Read the page from the block in the file
    file_manager.read(&block, &mut page2)?;

    // Assert that the read integer matches the written integer
    assert_eq!(page2.get_int(position2)?, 345);

    // Assert that the read string matches the written string
    assert_eq!(page2.get_string(position1)?, "abcdefghijklm");

    Ok(())
}
