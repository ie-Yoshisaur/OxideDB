use crate::record::field_type::FieldType;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};

/// Information about a field, including its type and length.
#[derive(Debug, Clone)]
struct FieldInfo {
    /// The type of the field.
    field_type: FieldType,
    /// The length of the field.
    length: usize,
}

/// Represents the schema of a table.
///
/// A schema contains the name and type of each field of the table,
/// as well as the length of each variable character field.
#[derive(Debug, Default)]
pub struct Schema {
    /// A set of field names in the schema.
    fields: HashSet<String>,
    /// A map from field names to their corresponding information.
    info: HashMap<String, FieldInfo>,
}

impl Schema {
    /// Creates a new, empty schema.
    pub fn new() -> Self {
        Self {
            fields: HashSet::new(),
            info: HashMap::new(),
        }
    }

    /// Returns a clone of the set of field names in the schema.
    pub fn get_fields(&self) -> HashSet<String> {
        self.fields.clone()
    }

    /// Returns the type of a given field, if it exists.
    pub fn get_field_type(&self, field_name: &str) -> Option<FieldType> {
        self.info
            .get(field_name)
            .map(|info| info.field_type.clone())
    }

    /// Adds a field to the schema.
    ///
    /// # Arguments
    ///
    /// * `field_name` - The name of the field.
    /// * `field_type` - The type of the field.
    /// * `length` - The length of the field.
    pub fn add_field(&mut self, field_name: String, field_type: FieldType, length: usize) {
        self.fields.insert(field_name.clone());
        self.info
            .insert(field_name, FieldInfo { field_type, length });
    }

    /// Adds an integer field to the schema.
    ///
    /// # Arguments
    ///
    /// * `field_name` - The name of the field.
    pub fn add_int_field(&mut self, field_name: String) {
        self.add_field(field_name, FieldType::Integer, 0);
    }

    /// Adds a string field to the schema.
    ///
    /// # Arguments
    ///
    /// * `field_name` - The name of the field.
    /// * `length` - The length of the field.
    pub fn add_string_field(&mut self, field_name: String, length: usize) {
        self.add_field(field_name, FieldType::VarChar, length);
    }

    /// Adds a field to the schema based on another schema's locked guard.
    ///
    /// # Arguments
    ///
    /// * `field_name` - The name of the field.
    /// * `schema_guard` - The locked guard of the other schema.
    pub fn add(&mut self, field_name: String, schema_guard: &MutexGuard<Schema>) {
        if let Some(field_info) = schema_guard.info.get(&field_name) {
            self.add_field(field_name, field_info.field_type.clone(), field_info.length);
        }
    }

    /// Adds all fields from another schema to this schema.
    ///
    /// # Arguments
    ///
    /// * `schema` - The other schema.
    pub fn add_all(&mut self, schema: Arc<Mutex<Schema>>) {
        let schema_guard = schema.lock().unwrap();
        for field_name in &schema_guard.fields {
            self.add(field_name.clone(), &schema_guard);
        }
    }

    /// Checks if a field exists in the schema.
    ///
    /// # Arguments
    ///
    /// * `field_name` - The name of the field.
    ///
    /// # Returns
    ///
    /// * `true` if the field exists, `false` otherwise.
    pub fn has_field(&self, field_name: &str) -> bool {
        self.fields.contains(field_name)
    }

    /// Returns the length of a given field, if it exists.
    ///
    /// # Arguments
    ///
    /// * `field_name` - The name of the field.
    ///
    /// # Returns
    ///
    /// * An `Option` containing the length of the field.
    pub fn get_length(&self, field_name: &str) -> Option<usize> {
        self.info.get(field_name).map(|info| info.length)
    }
}
