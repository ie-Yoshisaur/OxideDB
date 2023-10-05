use crate::record::field_type::FieldType;
use crate::record::schema::Schema;
use std::error::Error;
use std::sync::{Arc, Mutex};

pub struct EmbeddedMetadata {
    schema: Arc<Mutex<Schema>>,
}

impl EmbeddedMetadata {
    pub fn new(schema: Arc<Mutex<Schema>>) -> Self {
        Self { schema }
    }

    pub fn get_column_count(&self) -> usize {
        self.schema.lock().unwrap().get_fields().len()
    }

    pub fn get_column_name(&self, column: usize) -> Result<String, Box<dyn Error>> {
        let mut fields_vec: Vec<String> = self
            .schema
            .lock()
            .unwrap()
            .get_fields()
            .iter()
            .cloned()
            .collect();
        fields_vec.sort();
        fields_vec
            .get(column - 1)
            .ok_or_else(|| Box::<dyn Error>::from(format!("Column {} does not exist", column)))
            .map(|s| s.clone())
    }

    pub fn get_column_type(&self, column: usize) -> Result<FieldType, Box<dyn Error>> {
        let field_name = self.get_column_name(column)?;
        Ok(self
            .schema
            .lock()
            .unwrap()
            .get_field_type(field_name.as_str())
            .unwrap())
    }

    pub fn get_column_display_size(&self, column: usize) -> Result<usize, Box<dyn Error>> {
        const INTEGER_DISPLAY_SIZE: usize = 6;

        let field_name = self.get_column_name(column)?;
        let field_type = self
            .schema
            .lock()
            .unwrap()
            .get_field_type(field_name.as_str())
            .unwrap();

        let field_length = if field_type == FieldType::Integer {
            INTEGER_DISPLAY_SIZE
        } else {
            self.schema
                .lock()
                .unwrap()
                .get_length(field_name.as_str())
                .unwrap()
        };

        Ok(std::cmp::max(field_name.len(), field_length) + 10)
    }
}
