use crate::file::page::Page;
use crate::log::err::LogError;
use crate::log::log_manager::LogManager;
use crate::server::oxide_db::OxideDB;
use std::fs::remove_dir_all;
use std::path::PathBuf;

/// Tests log management operations in `LogManager`.
///
/// This test does the following:
/// - Creates a new OxideDB instance for debugging with a given path and block size.
/// - Creates log records and appends them to the log file.
/// - Flushes the log records up to a certain LSN.
/// - Reads back the log records and checks if they match the expected records.
#[test]
fn log_test() -> Result<(), Box<dyn std::error::Error>> {
    // Create a test directory for OxideDB with a block size of 400 and only 3 buffers.
    let test_directory = PathBuf::from("logtest");
    let mut db = OxideDB::new_for_debug(test_directory.clone(), 400, 8);
    let mut log_manager = db.get_log_manager().lock().unwrap();

    // Print the initial empty log file
    print_log_records(&mut log_manager, "The initial empty log file:", &[])?;

    // Create and append log records from 1 to 35
    create_records(&mut log_manager, 1, 35).map_err(|e| LogError::IOError(e.to_string()))?;

    // Define expected records after the first batch and print them
    let expected_after_first_batch = (1..=35)
        .rev()
        .map(|i| (format!("record{}", i), i + 100))
        .collect::<Vec<_>>();
    print_log_records(
        &mut log_manager,
        "The log file now has these records:",
        &expected_after_first_batch,
    )?;

    // Create and append log records from 36 to 70
    create_records(&mut log_manager, 36, 70)?;

    // Flush log records up to LSN 65
    log_manager.flush_by_lsn(65)?;

    // Define expected records after the second batch and print them
    let expected_after_second_batch = (1..=70)
        .rev()
        .map(|i| (format!("record{}", i), i + 100))
        .collect::<Vec<_>>();
    print_log_records(
        &mut log_manager,
        "The log file now has these records:",
        &expected_after_second_batch,
    )?;

    // Cleanup the test directory.
    remove_dir_all(test_directory);
    Ok(())
}

/// Prints log records and checks if they match the expected records.
///
/// # Arguments
///
/// * `log_manager`: The `LogManager` to use for reading log records.
/// * `msg`: The message to print before printing log records.
/// * `expected_records`: The expected log records.
///
/// # Errors
///
/// Returns a `LogError` if reading or matching fails.
fn print_log_records(
    log_manager: &mut LogManager,
    msg: &str,
    expected_records: &[(String, i32)],
) -> Result<(), LogError> {
    println!("{}", msg);
    let mut index = 0;
    let iterator = log_manager.iterator()?;
    for record in iterator {
        match record {
            Ok(bytes) => {
                let mut page = Page::new_from_bytes(bytes);
                let string = page
                    .get_string(0)
                    .map_err(|e| LogError::PageError(e.to_string()))?;
                let number_position = Page::max_length(string.len());
                let value = page
                    .get_int(number_position)
                    .map_err(|e| LogError::PageError(e.to_string()))?;

                assert_eq!(
                    (string, value),
                    expected_records[index],
                    "Record does not match expected value"
                );
                index += 1;
            }
            Err(e) => {
                return Err(e);
            }
        }
    }
    assert_eq!(
        index,
        expected_records.len(),
        "Number of records does not match expected count"
    );
    Ok(())
}

/// Creates and appends log records to the log file using `LogManager`.
///
/// # Arguments
///
/// * `log_manager`: The `LogManager` to use for appending log records.
/// * `start`: The starting integer value for creating log records.
/// * `end`: The ending integer value for creating log records.
///
/// # Errors
///
/// Returns a `LogError` if appending a log record fails.
fn create_records(log_manager: &mut LogManager, start: i32, end: i32) -> Result<(), LogError> {
    println!("Creating records: ");
    for i in start..=end {
        let record = create_log_record(format!("record{}", i), i + 100)?;
        let lsn = log_manager.append(&record)?;
        println!("{} ", lsn);
    }
    println!();
    Ok(())
}

/// Creates a log record consisting of a string and an integer.
///
/// # Arguments
///
/// * `s`: The string to include in the log record.
/// * `n`: The integer to include in the log record.
///
/// # Errors
///
/// Returns a `LogError` if setting the string or integer in the page fails.
fn create_log_record(s: String, n: i32) -> Result<Vec<u8>, LogError> {
    let string_position = 0;
    let number_position = string_position + Page::max_length(s.len());
    let blocksize = number_position + std::mem::size_of::<i32>();
    let mut page = Page::new_from_blocksize(blocksize);
    page.set_string(string_position, &s)
        .map_err(|e| LogError::PageError(e.to_string()))?;
    page.set_int(number_position, n)
        .map_err(|e| LogError::PageError(e.to_string()))?;
    page.read_bytes(0, blocksize)
        .map_err(|e| LogError::PageError(e.to_string()))
}
