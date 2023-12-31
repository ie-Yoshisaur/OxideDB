use std::fmt;

/// Represents the type of a field in a schema.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldType {
    /// Represents an integer field.
    ///
    /// This variant is associated with the `i32` value of 4.
    Integer = 4,
    /// Represents a variable character field with a given length.
    ///
    /// This variant is associated with the `i32` value of 12.
    VarChar = 12,
}

impl FieldType {
    /// Converts an `i32` value to its corresponding `FieldType`.
    ///
    /// The function returns `Some(FieldType)` if the given `i32` corresponds to a valid
    /// `FieldType`, and `None` otherwise.
    pub fn from_i32(value: i32) -> Option<FieldType> {
        match value {
            4 => Some(FieldType::Integer),
            12 => Some(FieldType::VarChar),
            _ => None,
        }
    }
}

impl fmt::Display for FieldType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Match each enum variant to its displayable string
        match self {
            FieldType::Integer => write!(f, "int"),
            FieldType::VarChar => write!(f, "varchar"),
        }
    }
}
