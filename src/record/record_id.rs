use std::fmt;

/// Represents an identifier for a record within a file.
///
/// A `RecordId` consists of the block number in the file,
/// and the location of the record in that block.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct RecordId {
    /// The block number where the record resides.
    block_number: i32,
    /// The location of the record within the block.
    slot_number: i32,
}

impl RecordId {
    /// Creates a new `RecordId` for the record having the
    /// specified location in the specified block.
    ///
    /// # Arguments
    ///
    /// * `block_number` - The block number where the record resides.
    /// * `slot_number` - The location of the record within the block.
    ///
    /// # Returns
    ///
    /// * `Self` - A new `RecordId` object.
    pub fn new(block_number: i32, slot_number: i32) -> Self {
        Self {
            block_number,
            slot_number,
        }
    }

    /// Returns the block number associated with this `RecordId`.
    ///
    /// # Returns
    ///
    /// * `i32` - The block number.
    pub fn get_block_number(&self) -> i32 {
        self.block_number
    }

    /// Returns the slot number associated with this `RecordId`.
    ///
    /// # Returns
    ///
    /// * `i32` - The slot number.
    pub fn get_slot_number(&self) -> i32 {
        self.slot_number
    }
}

/// Implements the `Display` trait for `RecordId`.
impl fmt::Display for RecordId {
    /// Formats the `RecordId` for display purposes.
    ///
    /// # Arguments
    ///
    /// * `f` - A mutable reference to a `fmt::Formatter`.
    ///
    /// # Returns
    ///
    /// * `fmt::Result` - The result of the formatting operation.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}, {}]", self.block_number, self.slot_number)
    }
}
