use crate::metadata::err::TableManagerError;
use crate::record::field_type::FieldType;
use crate::record::layout::Layout;
use crate::record::schema::Schema;
use crate::record::table_scan::TableScan;
use crate::transaction::transaction::Transaction;
use std::sync::{Arc, Mutex};

use std::collections::HashMap;

pub const MAX_NAME: usize = 16;

/// Manages the tables and associated metadata.
/// Provides functionality to create a new table, save metadata in the catalog,
/// and retrieve the metadata of previously created tables.
pub struct TableManager {
    table_catalog_layout: Arc<Layout>,
    field_catalog_layout: Arc<Layout>,
}

impl TableManager {
    /// Creates a new instance of `TableManager`.
    ///
    /// # Arguments
    ///
    /// * `is_new` - Indicates whether the database is new. If true, creates the catalog tables.
    /// * `transaction` - The startup transaction.
    ///
    /// # Returns
    ///
    /// A result containing the `TableManager` instance or an error.
    pub fn new(
        is_new: bool,
        transaction: Arc<Mutex<Transaction>>,
    ) -> Result<Self, TableManagerError> {
        let table_catalog_schema = Arc::new(Mutex::new({
            let mut schema = Schema::new();
            schema.add_string_field("table_name".to_string(), MAX_NAME);
            schema.add_int_field("slot_size".to_string());
            schema
        }));

        let table_catalog_layout = Arc::new(
            Layout::new(table_catalog_schema.clone())
                .map_err(|e| TableManagerError::LayoutError(e))?,
        );

        let field_catalog_schema = Arc::new(Mutex::new({
            let mut schema = Schema::new();
            schema.add_string_field("table_name".to_string(), MAX_NAME);
            schema.add_string_field("field_name".to_string(), MAX_NAME);
            schema.add_int_field("type".to_string());
            schema.add_int_field("length".to_string());
            schema.add_int_field("offset".to_string());
            schema
        }));

        let field_catalog_layout = Arc::new(
            Layout::new(field_catalog_schema.clone())
                .map_err(|e| TableManagerError::LayoutError(e))?,
        );

        if is_new {
            Self::create_table(
                "table_catalog",
                table_catalog_schema.clone(),
                table_catalog_layout.clone(),
                field_catalog_layout.clone(),
                transaction.clone(),
            )?;

            Self::create_table(
                "field_catalog",
                field_catalog_schema.clone(),
                table_catalog_layout.clone(),
                field_catalog_layout.clone(),
                transaction.clone(),
            )?;
        }

        Ok(Self {
            table_catalog_layout,
            field_catalog_layout,
        })
    }

    /// Internal logic to create a table.
    ///
    /// # Arguments
    ///
    /// * `table_name` - The name of the table to create.
    /// * `schema` - The schema for the table.
    /// * `table_catalog_layout` - The layout for the table catalog.
    /// * `fieald_catalog_layout` - The layout for the field catalog.
    /// * `transaction` - The transaction creating the table.
    ///
    /// # Returns
    ///
    /// A result containing either `()` on successful execution or an error.
    pub fn create_table(
        table_name: &str,
        schema: Arc<Mutex<Schema>>,
        table_catalog_layout: Arc<Layout>,
        field_catalog_layout: Arc<Layout>,
        transaction: Arc<Mutex<Transaction>>,
    ) -> Result<(), TableManagerError> {
        let layout = Layout::new(schema.clone()).map_err(|e| TableManagerError::LayoutError(e))?;

        let mut table_catalog = TableScan::new(
            transaction.clone(),
            "table_catalog",
            table_catalog_layout.clone(),
        )
        .map_err(|e| TableManagerError::TableScanError(e))?;

        table_catalog
            .insert()
            .map_err(|e| TableManagerError::TableScanError(e))?;
        table_catalog
            .set_string("table_name", table_name.to_string())
            .map_err(|e| TableManagerError::TableScanError(e))?;
        table_catalog
            .set_int("slot_size", layout.get_slot_size() as i32)
            .map_err(|e| TableManagerError::TableScanError(e))?;
        table_catalog.close();

        let mut field_catalog = TableScan::new(
            transaction.clone(),
            "field_catalog",
            field_catalog_layout.clone(),
        )
        .map_err(|e| TableManagerError::TableScanError(e))?;

        let schema_guard = schema.lock().unwrap();
        let fields = schema_guard.get_fields();
        for field_name in fields.iter() {
            field_catalog
                .insert()
                .map_err(|e| TableManagerError::TableScanError(e))?;
            field_catalog
                .set_string("table_name", table_name.to_string())
                .map_err(|e| TableManagerError::TableScanError(e))?;
            field_catalog
                .set_string("field_name", field_name.clone())
                .map_err(|e| TableManagerError::TableScanError(e))?;
            field_catalog
                .set_int(
                    "type",
                    schema_guard
                        .get_field_type(field_name)
                        .ok_or(TableManagerError::FieldNotFoundError)? as i32,
                )
                .map_err(|e| TableManagerError::TableScanError(e))?;
            field_catalog
                .set_int(
                    "length",
                    schema_guard
                        .get_length(field_name)
                        .ok_or(TableManagerError::FieldNotFoundError)? as i32,
                )
                .map_err(|e| TableManagerError::TableScanError(e))?;
            field_catalog
                .set_int(
                    "offset",
                    layout
                        .get_offset(field_name)
                        .ok_or(TableManagerError::FieldNotFoundError)? as i32,
                )
                .map_err(|e| TableManagerError::TableScanError(e))?;
        }
        field_catalog.close();
        Ok(())
    }

    /// Creates a new table with the given schema.
    ///
    /// # Arguments
    ///
    /// * `table_name` - The name of the new table.
    /// * `schema` - The schema for the new table.
    /// * `transaction` - The transaction creating the table.
    ///
    /// # Returns
    ///
    /// A result containing either `()` on successful execution or an error.
    pub fn create_table_from_table_manager(
        &self,
        table_name: &str,
        schema: Arc<Mutex<Schema>>,
        transaction: Arc<Mutex<Transaction>>,
    ) -> Result<(), TableManagerError> {
        Self::create_table(
            table_name,
            schema,
            self.table_catalog_layout.clone(),
            self.field_catalog_layout.clone(),
            transaction,
        )?;
        Ok(())
    }

    /// Retrieves the layout of a specified table.
    ///
    /// # Arguments
    ///
    /// * `table_name` - The name of the table whose layout is to be retrieved.
    /// * `transaction` - The transaction.
    ///
    /// # Returns
    ///
    /// A result containing either the `Layout` on successful retrieval or an error.
    pub fn get_layout(
        &self,
        table_name: &str,
        transaction: Arc<Mutex<Transaction>>,
    ) -> Result<Layout, TableManagerError> {
        let mut size = -1;
        let mut table_catalog = TableScan::new(
            transaction.clone(),
            "table_catalog",
            self.table_catalog_layout.clone(),
        )
        .map_err(|e| TableManagerError::TableScanError(e))?;
        while table_catalog
            .next()
            .map_err(|e| TableManagerError::TableScanError(e))?
        {
            if table_catalog
                .get_string("table_name")
                .map_err(|e| TableManagerError::TableScanError(e))?
                == table_name
            {
                size = table_catalog
                    .get_int("slot_size")
                    .map_err(|e| TableManagerError::TableScanError(e))?;
                break;
            }
        }
        table_catalog.close();

        let mut schema = Schema::new();
        let mut offsets = HashMap::new();
        let mut field_catalog = TableScan::new(
            transaction.clone(),
            "field_catalog",
            self.field_catalog_layout.clone(),
        )
        .map_err(|e| TableManagerError::TableScanError(e))?;
        while field_catalog
            .next()
            .map_err(|e| TableManagerError::TableScanError(e))?
        {
            if field_catalog
                .get_string("table_name")
                .map_err(|e| TableManagerError::TableScanError(e))?
                == table_name
            {
                let field_name = field_catalog
                    .get_string("field_name")
                    .map_err(|e| TableManagerError::TableScanError(e))?;
                let field_type = FieldType::from_i32(
                    field_catalog
                        .get_int("type")
                        .map_err(|e| TableManagerError::TableScanError(e))?,
                )
                .ok_or(TableManagerError::InvalidIntergerError)?;
                let field_length = field_catalog
                    .get_int("length")
                    .map_err(|e| TableManagerError::TableScanError(e))?;
                let offset = field_catalog
                    .get_int("offset")
                    .map_err(|e| TableManagerError::TableScanError(e))?;
                offsets.insert(field_name.clone(), offset as usize);
                schema.add_field(field_name, field_type, field_length as usize);
            }
        }
        let schema = Arc::new(Mutex::new(schema));
        field_catalog.close();
        Ok(Layout::new_from_metadata(schema, offsets, size as usize))
    }
}
