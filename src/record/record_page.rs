use crate::file::block_id::BlockId;
use crate::record::err::RecordPageError;
use crate::record::field_type::FieldType;
use crate::record::layout::Layout;
use crate::transaction::transaction::Transaction;
use std::sync::{Arc, Mutex};

/// Constants to indicate whether a slot is empty or used.
pub const EMPTY: i32 = 0;
pub const USED: i32 = 1;

/// Manages the storage of records within a block.
pub struct RecordPage {
    transaction: Arc<Mutex<Transaction>>,
    block: BlockId,
    layout: Arc<Layout>,
}

impl RecordPage {
    /// Creates a new RecordPage.
    ///
    /// # Arguments
    ///
    /// * `transaction` - The transaction performing the operations.
    /// * `block` - The block ID.
    /// * `layout` - The layout of the records.
    pub fn new(transaction: Arc<Mutex<Transaction>>, block: BlockId, layout: Arc<Layout>) -> Self {
        transaction.lock().unwrap().pin(block.clone());
        Self {
            transaction,
            block,
            layout,
        }
    }

    /// Gets an integer value from a specified field and slot.
    ///
    /// # Arguments
    ///
    /// * `slot` - The slot number.
    /// * `field_name` - The name of the field.
    ///
    /// # Returns
    ///
    /// * `Result<i32, RecordPageError>` - The integer value or an error.
    pub fn get_int(&self, slot: usize, field_name: &str) -> Result<i32, RecordPageError> {
        let field_position = self.get_offset(slot)
            + self
                .layout
                .get_offset(field_name)
                .ok_or(RecordPageError::OffsetNotFoundError)?;
        self.transaction
            .lock()
            .unwrap()
            .get_int(self.block.clone(), field_position as i32)
            .map_err(|e| RecordPageError::TransactionError(e))?
            .ok_or(RecordPageError::BufferNotFoundError)
    }

    /// Gets a string value from a specified field and slot.
    ///
    /// # Arguments
    ///
    /// * `slot` - The slot number.
    /// * `field_name` - The name of the field.
    ///
    /// # Returns
    ///
    /// * `Result<String, RecordPageError>` - The string value or an error.
    pub fn get_string(&self, slot: usize, field_name: &str) -> Result<String, RecordPageError> {
        let field_position = self.get_offset(slot)
            + self
                .layout
                .get_offset(field_name)
                .ok_or(RecordPageError::OffsetNotFoundError)?;
        self.transaction
            .lock()
            .unwrap()
            .get_string(self.block.clone(), field_position as i32)
            .map_err(|e| RecordPageError::TransactionError(e))?
            .ok_or(RecordPageError::BufferNotFoundError)
    }

    /// Sets an integer value to a specified field and slot.
    ///
    /// # Arguments
    ///
    /// * `slot` - The slot number.
    /// * `field_name` - The name of the field.
    /// * `value` - The integer value to set.
    ///
    /// # Returns
    ///
    /// * `Result<(), RecordPageError>` - Ok or an error.
    pub fn set_int(
        &mut self,
        slot: usize,
        field_name: &str,
        value: i32,
    ) -> Result<(), RecordPageError> {
        let field_position = self.get_offset(slot)
            + self
                .layout
                .get_offset(field_name)
                .ok_or(RecordPageError::OffsetNotFoundError)?;
        self.transaction
            .lock()
            .unwrap()
            .set_int(self.block.clone(), field_position as i32, value, true)
            .map_err(|e| RecordPageError::TransactionError(e))?;
        Ok(())
    }

    /// Sets a string value to a specified field and slot.
    ///
    /// # Arguments
    ///
    /// * `slot` - The slot number.
    /// * `field_name` - The name of the field.
    /// * `value` - The string value to set.
    ///
    /// # Returns
    ///
    /// * `Result<(), RecordPageError>` - Ok or an error.
    pub fn set_string(
        &mut self,
        slot: usize,
        field_name: &str,
        value: String,
    ) -> Result<(), RecordPageError> {
        let field_position = self.get_offset(slot)
            + self
                .layout
                .get_offset(field_name)
                .ok_or(RecordPageError::OffsetNotFoundError)?;
        self.transaction
            .lock()
            .unwrap()
            .set_string(self.block.clone(), field_position as i32, &value, true)
            .map_err(|e| RecordPageError::TransactionError(e))?;
        Ok(())
    }

    /// Deletes a record at a given slot by setting its flag to EMPTY.
    ///
    /// # Arguments
    ///
    /// * `slot` - The slot number to delete.
    pub fn delete(&mut self, slot: usize) {
        self.set_flag(slot, EMPTY);
    }

    /// Formats the block by setting all slots to EMPTY and initializing fields.
    ///
    /// # Returns
    ///
    /// * `Result<(), RecordPageError>` - Ok or an error.
    pub fn format(&mut self) -> Result<(), RecordPageError> {
        let mut slot = 0;
        while self.is_valid_slot(slot) {
            self.set_flag(slot, EMPTY);
            let schema = self.layout.get_schema();
            for field_name in schema.get_fields() {
                let field_position = self.get_offset(slot)
                    + self
                        .layout
                        .get_offset(&field_name)
                        .ok_or(RecordPageError::OffsetNotFoundError)?;
                match schema
                    .get_field_type(&field_name)
                    .ok_or(RecordPageError::FieldNotFoundError)?
                {
                    FieldType::Integer => self
                        .transaction
                        .lock()
                        .unwrap()
                        .set_int(self.block.clone(), field_position as i32, 0, false)
                        .map_err(|e| RecordPageError::TransactionError(e))?,
                    FieldType::VarChar => self
                        .transaction
                        .lock()
                        .unwrap()
                        .set_string(
                            self.block.clone(),
                            field_position as i32,
                            &"".to_string(),
                            false,
                        )
                        .map_err(|e| RecordPageError::TransactionError(e))?,
                }
            }
            slot += 1;
        }
        Ok(())
    }

    /// Finds the next slot after the given slot that is USED.
    ///
    /// # Arguments
    ///
    /// * `slot` - The slot number to start the search from.
    ///
    /// # Returns
    ///
    /// * `Result<i32, RecordPageError>` - The next slot number or an error.
    pub fn next_after(&self, slot: i32) -> Result<i32, RecordPageError> {
        self.search_after(slot, USED)
    }

    /// Inserts a new record after the given slot by setting its flag to USED.
    ///
    /// # Arguments
    ///
    /// * `slot` - The slot number to start the search from.
    ///
    /// # Returns
    ///
    /// * `Result<i32, RecordPageError>` - The new slot number or an error.
    pub fn insert_after(&mut self, slot: i32) -> Result<i32, RecordPageError> {
        let new_slot = self.search_after(slot, EMPTY)?;
        if new_slot >= 0 {
            self.set_flag(new_slot as usize, USED);
        }
        Ok(new_slot)
    }

    /// Gets the block ID associated with this RecordPage.
    ///
    /// # Returns
    ///
    /// * `BlockId` - The block ID.
    pub fn get_block(&self) -> BlockId {
        self.block.clone()
    }

    /// Sets the flag (EMPTY or USED) for a given slot.
    ///
    /// # Arguments
    ///
    /// * `slot` - The slot number.
    /// * `flag` - The flag to set (EMPTY or USED).
    ///
    /// # Returns
    ///
    /// * `Result<(), RecordPageError>` - Ok or an error.
    fn set_flag(&mut self, slot: usize, flag: i32) -> Result<(), RecordPageError> {
        let flag_position = self.get_offset(slot);
        self.transaction
            .lock()
            .unwrap()
            .set_int(self.block.clone(), flag_position as i32, flag, true)
            .map_err(|e| RecordPageError::TransactionError(e))?;
        Ok(())
    }

    /// Searches for a slot with a given flag after the specified slot.
    ///
    /// # Arguments
    ///
    /// * `slot` - The slot number to start the search from.
    /// * `flag` - The flag to search for (EMPTY or USED).
    ///
    /// # Returns
    ///
    /// * `Result<i32, RecordPageError>` - The found slot number or an error.
    fn search_after(&self, slot: i32, flag: i32) -> Result<i32, RecordPageError> {
        let mut slot = slot + 1;
        while self.is_valid_slot(slot as usize) {
            if self
                .transaction
                .lock()
                .unwrap()
                .get_int(self.block.clone(), self.get_offset(slot as usize) as i32)
                .map_err(|e| RecordPageError::TransactionError(e))?
                .ok_or(RecordPageError::BufferNotFoundError)?
                == flag
            {
                return Ok(slot as i32);
            }
            slot += 1;
        }
        Ok(-1)
    }

    /// Checks if a slot is valid based on the block size.
    ///
    /// # Arguments
    ///
    /// * `slot` - The slot number.
    ///
    /// # Returns
    ///
    /// * `bool` - True if the slot is valid, otherwise false.
    fn is_valid_slot(&self, slot: usize) -> bool {
        self.get_offset(slot + 1) <= self.transaction.lock().unwrap().block_size()
    }

    /// Gets the offset for a given slot.
    ///
    /// # Arguments
    ///
    /// * `slot` - The slot number.
    ///
    /// # Returns
    ///
    /// * `usize` - The offset.
    fn get_offset(&self, slot: usize) -> usize {
        slot * self.layout.get_slot_size()
    }
}
