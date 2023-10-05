// no docs
// no comments
// no error handlings
// no variable name edit
use crate::file::block_id::BlockId;
use crate::index::btree::btree_directory::BTreeDir;
use crate::index::btree::btree_leaf::BTreeLeaf;
use crate::index::btree::btree_page::BTPage;
use crate::index::index::Index;
use crate::query::constant::Constant;
use crate::record::field_type::FieldType;
use crate::record::layout::Layout;
use crate::record::record_id::RecordId;
use crate::record::schema::Schema;
use crate::transaction::transaction::Transaction;
use std::sync::{Arc, Mutex};

pub struct BTreeIndex {
    tx: Arc<Mutex<Transaction>>,
    dir_layout: Layout,
    leaf_layout: Layout,
    leaf_tbl: String,
    leaf: Option<BTreeLeaf>,
    root_blk: BlockId,
}

impl BTreeIndex {
    pub fn new(tx: Arc<Mutex<Transaction>>, idx_name: &str, leaf_layout: Layout) -> BTreeIndex {
        let tx = tx.clone();
        let leaf_tbl = format!("{}leaf", idx_name);
        let leaf_layout = leaf_layout.clone();
        if tx.lock().unwrap().get_size(&leaf_tbl).unwrap() == 0 {
            let blk = tx.lock().unwrap().append(&leaf_tbl).unwrap();
            let mut node = BTPage::new(tx.clone(), blk.clone(), leaf_layout.clone());
            node.format(&blk, -1);
        }

        let mut dir_schema = Schema::new();
        dir_schema.add(
            "block".to_string(),
            &leaf_layout.get_schema().lock().unwrap(),
        );
        dir_schema.add(
            "dataval".to_string(),
            &leaf_layout.get_schema().lock().unwrap(),
        );
        let dir_tbl = format!("{}dir", idx_name);
        let dir_schema = Arc::new(Mutex::new(dir_schema));
        let dir_layout = Layout::new(dir_schema.clone()).unwrap();
        let root_blk = BlockId::new(dir_tbl.clone(), 0);
        if tx.lock().unwrap().get_size(&dir_tbl).unwrap() == 0 {
            tx.lock().unwrap().append(&dir_tbl).unwrap();
            let mut node = BTPage::new(tx.clone(), root_blk.clone(), dir_layout.clone());
            node.format(&root_blk, 0);
            let fld_type = dir_schema
                .lock()
                .unwrap()
                .get_field_type("dataval")
                .unwrap();
            let min_val = if fld_type == FieldType::Integer {
                Constant::Int(i32::MIN)
            } else {
                Constant::Str(String::from(""))
            };
            node.insert_dir(0, min_val, 0);
            node.close();
        }

        BTreeIndex {
            tx,
            dir_layout,
            leaf_layout,
            leaf_tbl,
            leaf: None,
            root_blk,
        }
    }

    pub fn before_first(&mut self, search_key: Constant) {
        self.close();
        let mut root = BTreeDir::new(
            self.tx.clone(),
            self.root_blk.clone(),
            self.dir_layout.clone(),
        );
        let blk_num = root.search(search_key.clone());
        root.close();
        let leaf_blk = BlockId::new(self.leaf_tbl.clone(), blk_num);
        self.leaf = Some(BTreeLeaf::new(
            self.tx.clone(),
            leaf_blk,
            self.leaf_layout.clone(),
            search_key,
        ));
    }

    pub fn next(&mut self) -> bool {
        self.leaf.as_mut().map_or(false, |l| l.next())
    }

    pub fn get_data_rid(&self) -> Option<RecordId> {
        self.leaf.as_ref().map(|l| l.get_data_rid())
    }

    pub fn insert(&mut self, data_val: Constant, data_rid: RecordId) {
        self.before_first(data_val.clone());
        if let Some(mut leaf) = self.leaf.take() {
            let e = leaf.insert(data_rid.clone());
            leaf.close();
            if let Some(e) = e {
                let mut root = BTreeDir::new(
                    self.tx.clone(),
                    self.root_blk.clone(),
                    self.dir_layout.clone(),
                );
                let e2 = root.insert(e);
                if let Some(e2) = e2 {
                    root.make_new_root(e2);
                }
                root.close();
            }
        }
    }

    pub fn delete(&mut self, data_val: Constant, data_rid: RecordId) {
        self.before_first(data_val.clone());
        if let Some(mut leaf) = self.leaf.take() {
            leaf.delete(data_rid);
            leaf.close();
        }
    }

    pub fn close(&mut self) {
        if let Some(mut leaf) = self.leaf.take() {
            leaf.close();
        }
    }

    pub fn search_cost(num_blocks: i32, rpb: i32) -> i32 {
        1 + (num_blocks as f64).log(rpb as f64).floor() as i32
    }
}

impl Index for BTreeIndex {
    fn before_first(&mut self, search_key: Constant) {
        self.before_first(search_key);
    }

    fn next(&mut self) -> bool {
        self.next()
    }

    fn get_data_rid(&self) -> Option<RecordId> {
        self.get_data_rid()
    }

    fn insert(&mut self, data_val: Constant, data_rid: RecordId) {
        self.insert(data_val, data_rid);
    }

    fn delete(&mut self, data_val: Constant, data_rid: RecordId) {
        self.delete(data_val, data_rid);
    }

    fn close(&mut self) {
        self.close();
    }
}
