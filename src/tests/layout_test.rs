use crate::record::err::LayoutError;
use crate::record::layout::Layout;
use crate::record::schema::Schema;
use std::backtrace::Backtrace;
use std::sync::{Arc, Mutex};

/// Tests the layout creation and offset calculation in `Layout`.
///
/// This test does the following:
/// - Creates a new Schema instance and adds an integer field "A" and a string field "B" with length 9.
/// - Wraps the schema in an Arc<Mutex<>> and creates a new Layout instance.
/// - Initializes a HashMap to store the offsets of the fields.
/// - Iterates through the schema fields to get their offsets and stores them in the HashMap.
/// - Checks if the offsets are correctly calculated by asserting their expected values.
#[test]
fn layout_test() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize a new Schema and add fields "A" and "B".
    let mut schema = Schema::new();
    schema.add_int_field("A".to_string());
    schema.add_string_field("B".to_string(), 9);

    let schema = Arc::new(Mutex::new(schema));
    let layout = Layout::new(schema).map_err(|e| {
        eprintln!(
            "Layout creation failed.\nBacktrace: {:#?}",
            Backtrace::capture()
        );
        e
    })?;

    // Initialize a HashMap to store the offsets of the fields.
    let mut offsets = std::collections::HashMap::new();

    // Iterate through the schema fields to get their offsets.
    for field_name in layout.get_schema().lock().unwrap().get_fields() {
        let offset = layout.get_offset(&field_name).ok_or_else(|| {
            eprintln!("Field not found.\nBacktrace: {:#?}", Backtrace::capture());
            LayoutError::FieldNotFoundError
        })?;
        offsets.insert(field_name, offset);
    }

    // Assert the calculated offsets.
    if let Some(a_offset) = offsets.get("A") {
        if let Some(b_offset) = offsets.get("B") {
            if a_offset < b_offset {
                assert_eq!(
                    *a_offset,
                    4,
                    "Assertion failed for A offset.\nBacktrace: {:#?}",
                    Backtrace::capture()
                );
                assert_eq!(
                    *b_offset,
                    8,
                    "Assertion failed for B offset.\nBacktrace: {:#?}",
                    Backtrace::capture()
                );
            } else {
                assert_eq!(
                    *b_offset,
                    4,
                    "Assertion failed for B offset.\nBacktrace: {:#?}",
                    Backtrace::capture()
                );
                assert_eq!(
                    *a_offset,
                    17,
                    "Assertion failed for A offset.\nBacktrace: {:#?}",
                    Backtrace::capture()
                );
            }
        }
    }
    Ok(())
}
