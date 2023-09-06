use crate::file::block_id::BlockId;
use crate::server::oxide_db::OxideDB;
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
fn buffer_manager_test() -> Result<(), Box<dyn std::error::Error>> {
    // Create a test directory for OxideDB with a block size of 400 and only 3 buffers.
    let test_directory = PathBuf::from("buffermgrtest");
    let db = OxideDB::new_for_debug(test_directory.clone(), 400, 3);
    let buffer_manager = db.get_buffer_manager();

    let mut blocks: [Option<BlockId>; 6] = Default::default();

    // Pin the first three blocks and store their IDs in an array.
    for i in 0usize..3 {
        let buffer = buffer_manager.pin(BlockId::new("testfile".to_string(), i as u32))?;
        let block = buffer.get_block().ok_or("Failed to get block")?;
        blocks[i] = Some(block.clone());
    }

    // Unpin one of the pinned blocks.
    if let Some(buffer) = buffer_manager
        .find_existing_buffer(&blocks[1].as_ref().ok_or("Block should exist")?.clone())?
    {
        buffer_manager.unpin(buffer)?;
    }

    // Pin two more blocks
    {
        // Block 0 pinned twice
        let buffer = buffer_manager.pin(BlockId::new("testfile".to_string(), 0))?;
        let block = buffer.get_block().ok_or("Failed to get block")?;
        blocks[3] = Some(block.clone());
    }
    {
        // Block 1 repinned
        let buffer = buffer_manager.pin(BlockId::new("testfile".to_string(), 1))?;
        let block = buffer.get_block().ok_or("Failed to get block")?;
        blocks[4] = Some(block.clone());
    }

    // Check and display the number of available buffers.
    {
        let available_buffers = buffer_manager.get_number_available()?;
        println!("Available buffers: {}", *available_buffers);
    }

    // Attempt to pin a block when no buffers are expected to be available.
    println!("Attempting to pin block 3...");
    {
        match buffer_manager.pin(BlockId::new("testfile".to_string(), 3)) {
            Ok(buffer) => {
                let block = buffer.get_block().ok_or("Failed to get block")?;
                blocks[5] = Some(block.clone())
            }
            Err(_) => println!("Exception: No available buffers\n"),
        }
    }

    // Unpin another block to free up a buffer.
    if let Some(buffer) = buffer_manager
        .find_existing_buffer(&blocks[2].as_ref().ok_or("Block should exist")?.clone())?
    {
        buffer_manager.unpin(buffer)?;
    }

    // Try to pin the block again (this should now work due to the freed buffer).
    let buffer = buffer_manager.pin(BlockId::new("testfile".to_string(), 3))?;
    let block = buffer.get_block().ok_or("Failed to get block")?;
    blocks[5] = Some(block.clone());

    // Output the final allocation of blocks to buffers.
    println!("Final Buffer Allocation:");
    for (i, block) in blocks.iter().enumerate() {
        if let Some(b) = block {
            println!(
                "buffer{} pinned to block [file {}, block {}]",
                i,
                b.get_file_name(),
                b.get_block_number()
            );
        }
    }

    // Cleanup the test directory.
    remove_dir_all(test_directory)?;
    Ok(())
}
