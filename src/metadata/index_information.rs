use crate::index::btree::btree_index::BTreeIndex;
use crate::index::index::Index;
use crate::metadata::statistics_information::StatisticsInformation;
use crate::record::field_type::FieldType;
use crate::record::layout::Layout;
use crate::record::schema::Schema;
use crate::transaction::transaction::Transaction;
use std::sync::{Arc, Mutex};

// no docs
// no comments
// no error handlings
// no variable name edit
pub struct IndexInformation {
    index_name: String,
    field_name: String,
    transaction: Arc<Mutex<Transaction>>,
    table_schema: Arc<Mutex<Schema>>,
    index_layout: Layout,
    statistics_information: StatisticsInformation,
}

impl IndexInformation {
    pub fn new(
        index_name: String,
        field_name: String,
        table_schema: Arc<Mutex<Schema>>,
        transaction: Arc<Mutex<Transaction>>,
        statistics_information: StatisticsInformation,
    ) -> Self {
        let index_layout = Self::create_index_layout(table_schema.clone(), &field_name);
        Self {
            index_name,
            field_name,
            transaction,
            table_schema,
            index_layout,
            statistics_information,
        }
    }

    pub fn open(&self) -> Box<dyn Index> {
        Box::new(BTreeIndex::new(
            self.transaction.clone(),
            &self.index_name,
            self.index_layout.clone(),
        ))
    }

    pub fn blocks_accessed(&self) -> i32 {
        let rpb: i32 = (self.transaction.lock().unwrap().block_size()
            / self.index_layout.get_slot_size()) as i32;
        let num_blocks: i32 = self.statistics_information.records_output() / rpb;
        BTreeIndex::search_cost(num_blocks, rpb) as i32
    }

    pub fn records_output(&self) -> i32 {
        self.statistics_information.records_output()
            / self
                .statistics_information
                .distinct_values(&self.field_name)
    }

    pub fn distinct_values(&self, fname: &str) -> i32 {
        if self.field_name == fname {
            1
        } else {
            self.statistics_information
                .distinct_values(&self.field_name)
        }
    }

    fn create_index_layout(table_schema: Arc<Mutex<Schema>>, field_name: &str) -> Layout {
        let mut schema = Schema::new();
        schema.add_int_field("block".to_string());
        schema.add_int_field("id".to_string());
        if table_schema
            .lock()
            .unwrap()
            .get_field_type(field_name)
            .unwrap()
            == FieldType::Integer
        {
            schema.add_int_field("data_value".to_string());
        } else {
            let field_len = table_schema.lock().unwrap().get_length(field_name).unwrap();
            schema.add_string_field("data_value".to_string(), field_len);
        }
        let schema = Arc::new(Mutex::new(schema));
        Layout::new(schema).unwrap()
    }
}
