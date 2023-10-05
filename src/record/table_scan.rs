use crate::file::block_id::BlockId;
use crate::materialize::sort_scan::SortScan;
use crate::query::constant::Constant;
use crate::query::scan::Scan;
use crate::record::err::TableScanError;
use crate::record::field_type::FieldType;
use crate::record::layout::Layout;
use crate::record::record_id::RecordId;
use crate::record::record_page::RecordPage;
use crate::transaction::transaction::Transaction;
use std::sync::{Arc, Mutex};

// TableScan provides methods for scanning a table.
pub struct TableScan {
    transaction: Arc<Mutex<Transaction>>,
    layout: Arc<Layout>,
    record_page: RecordPage,
    file_name: String,
    current_slot: i32,
}

impl TableScan {
    /// Creates a new TableScan instance.
    ///
    /// # Arguments
    ///
    /// * `transaction` - The transaction context.
    /// * `table_name` - The name of the table to scan.
    /// * `layout` - The layout of the table.
    ///
    /// # Returns
    ///
    /// * `Result<Self, TableScanError>` - A new TableScan instance or an error.
    pub fn new(
        transaction: Arc<Mutex<Transaction>>,
        table_name: &str,
        layout: Arc<Layout>,
    ) -> Result<Self, TableScanError> {
        let file_name = format!("{}.tbl", table_name);
        let record_page = if transaction.lock().unwrap().get_size(&file_name).unwrap() == 0 {
            Self::create_record_page_at_new_block(transaction.clone(), &file_name, layout.clone())?
        } else {
            Self::create_record_page_at_block(transaction.clone(), &file_name, 0, layout.clone())
        };

        Ok(Self {
            transaction,
            layout,
            record_page,
            file_name,
            current_slot: -1,
        })
    }

    /// Moves to the first block in the table.
    pub fn before_first(&mut self) {
        self.move_to_block(0);
    }

    /// Moves to the next record in the table.
    ///
    /// # Returns
    ///
    /// * `Result<bool, TableScanError>` - True if moved to the next record, false otherwise.
    pub fn next(&mut self) -> Result<bool, TableScanError> {
        self.current_slot = self
            .record_page
            .next_after(&mut self.current_slot)
            .map_err(|e| TableScanError::RecordPageError(e))?;
        while self.current_slot < 0 {
            if self.at_last_block() {
                return Ok(false);
            }
            self.move_to_block(self.record_page.get_block().get_block_number() + 1);
            self.current_slot = self
                .record_page
                .next_after(&mut self.current_slot)
                .map_err(|e| TableScanError::RecordPageError(e))?;
        }
        Ok(true)
    }

    /// Gets the integer value of a specified field in the current record.
    ///
    /// # Arguments
    ///
    /// * `field_name` - The name of the field to retrieve.
    ///
    /// # Returns
    ///
    /// * `Result<i32, TableScanError>` - The integer value or an error.
    pub fn get_int(&self, field_name: &str) -> Result<i32, TableScanError> {
        self.record_page
            .get_int(self.current_slot as usize, field_name)
            .map_err(|e| TableScanError::RecordPageError(e))
    }

    /// Gets the string value of a specified field in the current record.
    ///
    /// # Arguments
    ///
    /// * `field_name` - The name of the field to retrieve.
    ///
    /// # Returns
    ///
    /// * `Result<String, TableScanError>` - The string value or an error.
    pub fn get_string(&self, field_name: &str) -> Result<String, TableScanError> {
        self.record_page
            .get_string(self.current_slot as usize, field_name)
            .map_err(|e| TableScanError::RecordPageError(e))
    }

    /// Gets the value of a specified field in the current record as a `Constant`.
    ///
    /// # Arguments
    ///
    /// * `field_name` - The name of the field to retrieve.
    ///
    /// # Returns
    ///
    /// * `Option<Constant>` - The value wrapped as a `Constant` if the field exists, None otherwise.
    pub fn get_value(&self, field_name: &str) -> Option<Constant> {
        if self
            .layout
            .get_schema()
            .lock()
            .unwrap()
            .get_field_type(field_name)
            .unwrap()
            == FieldType::Integer
        {
            return Some(Constant::Int(self.get_int(field_name).unwrap()));
        } else {
            return Some(Constant::Str(self.get_string(field_name).unwrap()));
        }
    }

    /// Sets the integer value of a specified field in the current record.
    ///
    /// # Arguments
    ///
    /// * `field_name` - The name of the field to set.
    /// * `value` - The integer value to set.
    ///
    /// # Returns
    ///
    /// * `Result<(), TableScanError>` - Ok or an error.
    pub fn set_int(&mut self, field_name: &str, value: i32) -> Result<(), TableScanError> {
        self.record_page
            .set_int(self.current_slot as usize, field_name, value)
            .map_err(|e| TableScanError::RecordPageError(e))?;
        Ok(())
    }

    /// Sets the string value of a specified field in the current record.
    ///
    /// # Arguments
    ///
    /// * `field_name` - The name of the field to set.
    /// * `value` - The string value to set.
    ///
    /// # Returns
    ///
    /// * `Result<(), TableScanError>` - Ok or an error.
    pub fn set_string(&mut self, field_name: &str, value: String) -> Result<(), TableScanError> {
        self.record_page
            .set_string(self.current_slot as usize, field_name, value)
            .map_err(|e| TableScanError::RecordPageError(e))?;
        Ok(())
    }

    /// Inserts a new record into the table.
    ///
    /// # Returns
    ///
    /// * `Result<(), TableScanError>` - Ok or an error.
    pub fn insert(&mut self) -> Result<(), TableScanError> {
        self.current_slot = self
            .record_page
            .insert_after(&mut self.current_slot)
            .map_err(|e| TableScanError::RecordPageError(e))?;
        while self.current_slot < 0 {
            if self.at_last_block() {
                self.move_to_new_block()?;
            } else {
                self.move_to_block(self.record_page.get_block().get_block_number() + 1);
            }
            self.current_slot = self
                .record_page
                .insert_after(&mut self.current_slot)
                .map_err(|e| TableScanError::RecordPageError(e))?;
        }
        Ok(())
    }

    /// Deletes the current record from the table.
    pub fn delete(&mut self) {
        self.record_page.delete(self.current_slot as usize).unwrap();
    }

    /// Moves to a specific record identified by a RecordId.
    ///
    /// # Arguments
    ///
    /// * `record_id` - The RecordId of the record to move to.
    pub fn move_to_record_id(&mut self, record_id: RecordId) {
        self.close();
        let block = BlockId::new(self.file_name.clone(), record_id.get_block_number());
        self.record_page = RecordPage::new(self.transaction.clone(), block, self.layout.clone());
        self.current_slot = record_id.get_slot_number();
    }

    /// Gets the RecordId of the current record.
    ///
    /// # Returns
    ///
    /// * `RecordId` - The RecordId of the current record.
    pub fn get_record_id(&self) -> RecordId {
        RecordId::new(
            self.record_page.get_block().get_block_number(),
            self.current_slot,
        )
    }

    /// Closes the TableScan, releasing any pinned blocks.
    pub fn close(&mut self) {
        self.transaction
            .lock()
            .unwrap()
            .unpin(self.record_page.get_block());
    }

    /// Sets the value of a specified field in the current record.
    ///
    /// # Arguments
    ///
    /// * `field_name` - The name of the field to set.
    /// * `value` - The value to set as a `Constant`.
    ///
    /// This function will automatically determine the appropriate field type (integer or string)
    /// based on the schema and set the value accordingly.
    fn set_value(&mut self, field_name: &str, value: Constant) {
        if self
            .layout
            .get_schema()
            .lock()
            .unwrap()
            .get_field_type(field_name)
            .unwrap()
            == FieldType::Integer
        {
            self.set_int(field_name, value.as_int()).unwrap();
        } else {
            self.set_string(field_name, value.as_str().to_string())
                .unwrap();
        }
    }

    /// Moves to a specific block in the table.
    ///
    /// # Arguments
    ///
    /// * `block_num` - The block number to move to.
    fn move_to_block(&mut self, block_num: i32) {
        self.record_page = Self::create_record_page_at_block(
            self.transaction.clone(),
            &self.file_name,
            block_num,
            self.layout.clone(),
        );
        self.current_slot = -1;
    }

    /// Moves to a new block in the table.
    ///
    /// # Returns
    ///
    /// * `Result<(), TableScanError>` - Ok or an error.
    fn move_to_new_block(&mut self) -> Result<(), TableScanError> {
        self.record_page = Self::create_record_page_at_new_block(
            self.transaction.clone(),
            &self.file_name,
            self.layout.clone(),
        )?;
        self.current_slot = -1;
        Ok(())
    }

    /// Creates a new RecordPage at a specified block.
    ///
    /// # Arguments
    ///
    /// * `transaction` - The transaction context.
    /// * `file_name` - The name of the file.
    /// * `block_num` - The block number.
    /// * `layout` - The layout of the table.
    ///
    /// # Returns
    ///
    /// * `RecordPage` - The new RecordPage.
    fn create_record_page_at_block(
        transaction: Arc<Mutex<Transaction>>,
        file_name: &str,
        block_num: i32,
        layout: Arc<Layout>,
    ) -> RecordPage {
        let block = BlockId::new(file_name.to_string(), block_num);
        RecordPage::new(transaction.clone(), block, layout)
    }

    /// Creates a new RecordPage at a new block.
    ///
    /// # Arguments
    ///
    /// * `transaction` - The transaction context.
    /// * `file_name` - The name of the file.
    /// * `layout` - The layout of the table.
    ///
    /// # Returns
    ///
    /// * `Result<RecordPage, TableScanError>` - The new RecordPage or an error.
    fn create_record_page_at_new_block(
        transaction: Arc<Mutex<Transaction>>,
        file_name: &str,
        layout: Arc<Layout>,
    ) -> Result<RecordPage, TableScanError> {
        let block = transaction
            .lock()
            .unwrap()
            .append(file_name)
            .map_err(|e| TableScanError::TransactionError(e))?;
        let mut record_page = RecordPage::new(transaction.clone(), block, layout);
        record_page
            .format()
            .map_err(|e| TableScanError::RecordPageError(e))?;
        Ok(record_page)
    }

    /// Checks if the current block is the last block in the table.
    ///
    /// # Returns
    ///
    /// * `bool` - True if at the last block, false otherwise.
    fn at_last_block(&self) -> bool {
        self.record_page.get_block().get_block_number() as usize
            == self
                .transaction
                .lock()
                .unwrap()
                .get_size(&self.file_name)
                .unwrap()
                - 1
    }
}

impl Scan for TableScan {
    fn before_first(&mut self) {
        self.before_first();
    }

    fn next(&mut self) -> bool {
        self.next().unwrap_or(false)
    }

    fn get_int(&self, field_name: &str) -> Option<i32> {
        self.get_int(field_name).ok()
    }

    fn get_string(&self, field_name: &str) -> Option<String> {
        self.get_string(field_name).ok()
    }

    fn get_value(&self, field_name: &str) -> Option<Constant> {
        self.get_value(field_name)
    }

    fn has_field(&self, field_name: &str) -> bool {
        self.layout
            .get_schema()
            .lock()
            .unwrap()
            .has_field(field_name)
    }

    fn close(&mut self) {
        self.close();
    }

    fn set_value(&mut self, field_name: &str, value: Constant) {
        self.set_value(field_name, value);
    }

    fn set_int(&mut self, field_name: &str, value: i32) {
        self.set_int(field_name, value).unwrap();
    }

    fn set_string(&mut self, field_name: &str, value: String) {
        self.set_string(field_name, value).unwrap();
    }

    fn insert(&mut self) {
        self.insert().unwrap();
    }

    fn delete(&mut self) {
        self.delete();
    }

    fn get_record_id(&self) -> RecordId {
        self.get_record_id()
    }

    fn move_to_record_id(&mut self, record_id: RecordId) {
        self.move_to_record_id(record_id);
    }

    fn as_sort_scan(&self) -> Option<SortScan> {
        None
    }
}
