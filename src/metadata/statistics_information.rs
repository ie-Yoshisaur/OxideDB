/// A StatInfo struct holds statistical information about a table:
/// the number of blocks and the number of records.
#[derive(Clone)]
pub struct StatisticsInformation {
    number_blocks: i32,
    number_records: i32,
}

impl StatisticsInformation {
    /// Creates a new StatInfo object.
    ///
    /// # Arguments
    ///
    /// * `number_blocks` - The number of blocks in the table.
    /// * `number_records` - The number of records in the table.
    pub fn new(number_blocks: i32, number_records: i32) -> Self {
        Self {
            number_blocks,
            number_records,
        }
    }

    /// Returns the estimated number of blocks in the table.
    pub fn blocks_accessed(&self) -> i32 {
        self.number_blocks
    }

    /// Returns the estimated number of records in the table.
    pub fn records_output(&self) -> i32 {
        self.number_records
    }

    /// Returns an estimate of the number of distinct values for the specified field.
    /// This is a complete guess.
    ///
    /// # Arguments
    ///
    /// * `field_name` - The name of the field.
    pub fn distinct_values(&self, _field_name: &str) -> i32 {
        1 + (self.number_records / 3)
    }
}
