// no docs
// no comments
// no error handlings
// no variable name edit
use crate::file::block_id::BlockId;
use crate::index::btree::btree_page::BTPage;
use crate::index::btree::directory_entry::DirEntry;
use crate::query::constant::Constant;
use crate::record::layout::Layout;
use crate::transaction::transaction::Transaction;
use std::sync::{Arc, Mutex};

pub struct BTreeDir {
    tx: Arc<Mutex<Transaction>>,
    layout: Layout,
    contents: BTPage,
    filename: String,
}

impl BTreeDir {
    pub fn new(tx: Arc<Mutex<Transaction>>, blk: BlockId, layout: Layout) -> Self {
        let contents = BTPage::new(tx.clone(), blk.clone(), layout.clone());
        let filename = blk.get_file_name().to_string();
        BTreeDir {
            tx,
            layout,
            contents,
            filename,
        }
    }

    pub fn close(&mut self) {
        self.contents.close();
    }

    pub fn search(&mut self, searchkey: Constant) -> i32 {
        let mut child_block = self.find_child_block(searchkey.clone());
        while self.contents.get_flag() > 0 {
            self.contents.close();
            self.contents = BTPage::new(self.tx.clone(), child_block, self.layout.clone());
            child_block = self.find_child_block(searchkey.clone());
        }
        child_block.get_block_number()
    }

    pub fn make_new_root(&mut self, e: DirEntry) {
        let first_val = self.contents.get_data_val(0);
        let level = self.contents.get_flag();
        let new_blk = self.contents.split(0, level);
        let old_root = DirEntry::new(first_val, new_blk.get_block_number());
        self.insert_entry(old_root);
        self.insert_entry(e.clone());
        self.contents.set_flag(level + 1);
    }

    pub fn insert(&mut self, e: DirEntry) -> Option<DirEntry> {
        if self.contents.get_flag() == 0 {
            return self.insert_entry(e);
        }
        let child_blk = self.find_child_block(e.data_val());
        let mut child = BTreeDir::new(self.tx.clone(), child_blk, self.layout.clone());
        let my_entry = child.insert(e);
        child.close();
        if let Some(entry) = my_entry {
            return self.insert_entry(entry);
        }
        None
    }

    fn insert_entry(&mut self, e: DirEntry) -> Option<DirEntry> {
        let new_slot = 1 + self.contents.find_slot_before(e.data_val());
        self.contents
            .insert_dir(new_slot, e.data_val(), e.block_number());
        if !self.contents.is_full() {
            return None;
        }
        let level = self.contents.get_flag();
        let split_pos = self.contents.get_num_recs() / 2;
        let split_val = self.contents.get_data_val(split_pos);
        let new_blk = self.contents.split(split_pos, level);
        Some(DirEntry::new(split_val, new_blk.get_block_number()))
    }

    fn find_child_block(&self, searchkey: Constant) -> BlockId {
        let slot = self.contents.find_slot_before(searchkey.clone());
        let slot = if self.contents.get_data_val(slot + 1) == searchkey {
            slot + 1
        } else {
            slot
        };
        let blk_num = self.contents.get_child_num(slot);
        BlockId::new(self.filename.clone(), blk_num)
    }
}
