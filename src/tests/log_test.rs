use crate::file::page::Page;
use crate::log::log_manager::LogManager;
use crate::server::oxide_db::OxideDB;
use std::backtrace::Backtrace;
use std::error::Error;
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
fn log_test() -> Result<(), Box<dyn Error>> {
    // Create a test directory for OxideDB with a block size of 400 and only 3 buffers.
    let test_directory = PathBuf::from("logtest");
    let db = OxideDB::new_from_parameters(test_directory.clone(), 400, 8);
    let mut log_manager = db.get_log_manager().lock().expect(&format!(
        "Error while locking log manager\nBacktrace: {:#?}",
        Backtrace::capture()
    ));

    // Print the initial empty log file
    print_log_records(&mut log_manager, "The initial empty log file:", &[])?;

    // Create and append log records from 1 to 35
    create_records(&mut log_manager, 1, 35)?;

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
    log_manager.flush_by_lsn(65).expect(&format!(
        "Error during flushing log\nBacktrace: {:#?}",
        Backtrace::capture()
    ));

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

    // Remove test directory and cleanup.
    remove_dir_all(test_directory).expect(&format!(
        "Failed to remove test directory.\nBacktrace: {:#?}",
        Backtrace::capture()
    ));

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
/// Returns a `Box<dyn Error>` if reading or matching fails, including a backtrace.
fn print_log_records(
    log_manager: &mut LogManager,
    msg: &str,
    expected_records: &[(String, i32)],
) -> Result<(), Box<dyn Error>> {
    println!("{}", msg);
    let mut index = 0;
    let iterator = log_manager.iterator()?;
    for record in iterator {
        match record {
            Ok(bytes) => {
                let mut page = Page::new_from_bytes(bytes);
                let string = page.get_string(0)?;
                let number_position = Page::max_length(string.len());
                let value = page.get_int(number_position)?;

                assert_eq!(
                    (string, value),
                    expected_records[index],
                    "Record does not match expected value.\nBacktrace: {:#?}",
                    Backtrace::capture()
                );
                index += 1;
            }
            Err(_) => {
                return Err(format!("An error occurred: {:#?}", Backtrace::capture()).into());
            }
        }
    }
    assert_eq!(
        index,
        expected_records.len(),
        "Number of records does not match expected count.\nBacktrace: {:#?}",
        Backtrace::capture()
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
/// Returns a `Box<dyn Error>` if appending a log record fails, including a backtrace.
fn create_records(
    log_manager: &mut LogManager,
    start: i32,
    end: i32,
) -> Result<(), Box<dyn Error>> {
    println!("Creating records: ");
    for i in start..=end {
        let record = create_log_record(format!("record{}", i), i + 100).expect(&format!(
            "Error while creating record\nBacktrace: {:#?}",
            Backtrace::capture()
        ));
        let lsn = log_manager.append(&record).expect(&format!(
            "Error while appending to log\nBacktrace: {:#?}",
            Backtrace::capture()
        ));
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
/// Returns a `Box<dyn Error>` if setting the string or integer in the page fails, including a backtrace.
fn create_log_record(s: String, n: i32) -> Result<Vec<u8>, Box<dyn Error>> {
    // Initialize positions and blocksize
    let string_position = 0;
    let number_position = string_position + Page::max_length(s.len());
    let blocksize = number_position + std::mem::size_of::<i32>();

    // Create new page
    let mut page = Page::new_from_blocksize(blocksize);

    // Set string and integer on the page
    page.set_string(string_position, &s).expect(&format!(
        "Error while setting string in page\nBacktrace: {:#?}",
        Backtrace::capture()
    ));
    page.set_int(number_position, n).expect(&format!(
        "Error while setting integer in page\nBacktrace: {:#?}",
        Backtrace::capture()
    ));

    // Read bytes from page
    let bytes = page.read_bytes(0, blocksize).expect(&format!(
        "Error while reading bytes from page\nBacktrace: {:#?}",
        Backtrace::capture()
    ));

    Ok(bytes)
}
