use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Represents a unique identifier for a block within a file.
///
/// A `BlockId` is made up of a file name (`String`) and a block number (`i32`).
#[derive(Debug, Clone)]
pub struct BlockId {
    file_name: String,
    block_number: i32,
}

impl BlockId {
    /// Creates a new `BlockId` with the given `file_name` and `block_number`.
    ///
    /// # Arguments
    ///
    /// * `file_name` - The name of the file.
    /// * `block_number` - The block number within the file.
    pub fn new(file_name: String, block_number: i32) -> Self {
        Self {
            file_name,
            block_number,
        }
    }

    /// Returns the file name associated with this `BlockId`.
    pub fn get_file_name(&self) -> &str {
        &self.file_name
    }

    /// Returns the block number associated with this `BlockId`.
    pub fn get_block_number(&self) -> i32 {
        self.block_number
    }

    /// Checks for equality between two `BlockId` instances.
    ///
    /// Internally uses the `PartialEq` implementation.
    ///
    /// # Arguments
    ///
    /// * `other` - The other `BlockId` to compare with.
    pub fn equals(&self, other: &Self) -> bool {
        self == other
    }

    /// Calculates and returns the hash code of the `BlockId`.
    ///
    /// This is done using the `Hash` trait implementation for `BlockId`.
    pub fn hash_code(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

/// Implementing the `PartialEq` trait for `BlockId`.
///
/// This allows us to use the `==` operator to compare two `BlockId` instances.
impl PartialEq for BlockId {
    fn eq(&self, other: &Self) -> bool {
        self.file_name == other.file_name && self.block_number == other.block_number
    }
}

/// Implementing the `Eq` marker trait for `BlockId`.
///
/// This is empty because `Eq` is a marker trait that signals that
/// every value is reflexively equal to itself. It inherits from `PartialEq`.
impl Eq for BlockId {}

/// Implementing the `Hash` trait for `BlockId`.
///
/// This calculates a hash code for a `BlockId` instance.
impl Hash for BlockId {
    /// Overrides the `hash` method from the `Hash` trait.
    ///
    /// # Arguments
    ///
    /// * `state` - A mutable reference to a `Hasher` object, which is used internally to compute the hash.
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.file_name.hash(state);
        self.block_number.hash(state);
    }
}

/// Implementing the `Display` trait for `BlockId`.
///
/// Allows a `BlockId` instance to be converted to a string representation.
impl std::fmt::Display for BlockId {
    /// Formats the `BlockId` for display.
    ///
    /// # Arguments
    ///
    /// * `f` - A mutable reference to a `Formatter` object.
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "[file {}, block {}]", self.file_name, self.block_number)
    }
}
