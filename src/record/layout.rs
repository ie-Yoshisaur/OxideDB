use crate::file::page::Page;
use crate::record::err::LayoutError;
use crate::record::field_type::FieldType;
use crate::record::schema::Schema;
use std::collections::HashMap;
use std::mem::size_of;
use std::sync::{Arc, Mutex, MutexGuard};

/// The size of an i32 in bytes.
const I32_SIZE: usize = size_of::<i32>();

/// Represents the layout of a table's records.
///
/// Contains the schema, field offsets, and slot size.
#[derive(Debug, Clone)]
pub struct Layout {
    /// The schema of the table's records.
    schema: Arc<Mutex<Schema>>,
    /// A map from field names to their offsets within a record.
    offsets: HashMap<String, usize>,
    /// The size of a slot in bytes.
    slot_size: usize,
}

impl Layout {
    /// Creates a new `Layout` from a given schema.
    ///
    /// # Arguments
    ///
    /// * `schema` - An Arc<Mutex<Schema>> object.
    ///
    /// # Returns
    ///
    /// * `Result<Self, LayoutError>` - Returns `Ok` if the layout is successfully created, otherwise returns `Err`.
    pub fn new(schema: Arc<Mutex<Schema>>) -> Result<Self, LayoutError> {
        let mut offsets = HashMap::new();
        let mut position = I32_SIZE;
        let schema_clone = schema.clone();
        let schema_guard = schema_clone.lock().unwrap();
        for field_name in &schema_guard.get_fields() {
            let length = Layout::get_length_in_bytes(&schema_guard, field_name)?;
            offsets.insert(field_name.clone(), position);
            position += length;
        }
        Ok(Self {
            schema,
            offsets,
            slot_size: position,
        })
    }

    /// Creates a new `Layout` from given metadata.
    ///
    /// # Arguments
    ///
    /// * `schema` - An `Arc` wrapped `Schema` object.
    /// * `offsets` - A `HashMap` containing field offsets.
    /// * `slot_size` - The size of a slot in bytes.
    ///
    /// # Returns
    ///
    /// * `Self` - A new `Layout` object.
    pub fn new_from_metadata(
        schema: Arc<Mutex<Schema>>,
        offsets: HashMap<String, usize>,
        slot_size: usize,
    ) -> Self {
        Self {
            schema,
            offsets,
            slot_size,
        }
    }

    /// Returns the schema of the table's records.
    ///
    /// # Returns
    ///
    /// * `Arc<Mutex<Schema>>` - An Arc<Mutex<Schema>> object.
    pub fn get_schema(&self) -> Arc<Mutex<Schema>> {
        self.schema.clone()
    }

    /// Returns the offset of a specified field within a record.
    ///
    /// # Arguments
    ///
    /// * `field_name` - The name of the field.
    ///
    /// # Returns
    ///
    /// * `Option<usize>` - The offset of the field within a record, if it exists.
    pub fn get_offset(&self, field_name: &str) -> Option<usize> {
        self.offsets.get(field_name).cloned()
    }

    /// Returns the size of a slot, in bytes.
    ///
    /// # Returns
    ///
    /// * `usize` - The size of a slot in bytes.
    pub fn get_slot_size(&self) -> usize {
        self.slot_size
    }

    /// Calculates the length in bytes of a given field within the schema.
    ///
    /// # Arguments
    ///
    /// * `schema_guard` - A reference to a `MutexGuard` wrapping a `Schema` object.
    /// * `field_name` - The name of the field to find the length for.
    ///
    /// # Returns
    ///
    /// * `Result<usize, LayoutError>` - Returns `Ok` with the length in bytes if the field is found, otherwise returns `Err`.
    fn get_length_in_bytes(
        schema_guard: &MutexGuard<Schema>,
        field_name: &str,
    ) -> Result<usize, LayoutError> {
        let field_type = schema_guard
            .get_field_type(field_name)
            .ok_or(LayoutError::FieldNotFoundError)?;
        match field_type {
            FieldType::Integer => Ok(I32_SIZE),
            FieldType::VarChar => {
                let length = schema_guard
                    .get_length(field_name)
                    .ok_or(LayoutError::FieldNotFoundError)?;
                Ok(Page::max_length(length))
            }
        }
    }
}
