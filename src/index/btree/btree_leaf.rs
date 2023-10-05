// no docs
// no comments
// no error handlings
// no variable name edit
use crate::file::block_id::BlockId;
use crate::index::btree::btree_page::BTPage;
use crate::index::btree::directory_entry::DirEntry;
use crate::query::constant::Constant;
use crate::record::layout::Layout;
use crate::record::record_id::RecordId;
use crate::transaction::transaction::Transaction;
use std::sync::{Arc, Mutex};

pub struct BTreeLeaf {
    tx: Arc<Mutex<Transaction>>,
    layout: Layout,
    searchkey: Constant,
    contents: BTPage,
    current_slot: i32,
    filename: String,
}

impl BTreeLeaf {
    pub fn new(
        tx: Arc<Mutex<Transaction>>,
        blk: BlockId,
        layout: Layout,
        searchkey: Constant,
    ) -> Self {
        let contents = BTPage::new(tx.clone(), blk.clone(), layout.clone());
        let current_slot = contents.find_slot_before(searchkey.clone());
        let filename = blk.get_file_name().to_string();
        BTreeLeaf {
            tx,
            layout,
            searchkey,
            contents,
            current_slot,
            filename,
        }
    }

    pub fn close(&mut self) {
        self.contents.close();
    }

    pub fn next(&mut self) -> bool {
        self.current_slot += 1;
        if self.current_slot >= self.contents.get_num_recs() {
            self.try_overflow()
        } else if self.contents.get_data_val(self.current_slot) == self.searchkey {
            true
        } else {
            self.try_overflow()
        }
    }

    pub fn get_data_rid(&self) -> RecordId {
        self.contents.get_data_rid(self.current_slot)
    }

    pub fn delete(&mut self, datarid: RecordId) {
        while self.next() {
            if self.get_data_rid() == datarid {
                self.contents.delete(self.current_slot);
                return;
            }
        }
    }

    pub fn insert(&mut self, datarid: RecordId) -> Option<DirEntry> {
        if self.contents.get_flag() >= 0 && self.contents.get_data_val(0) > self.searchkey {
            let first_val = self.contents.get_data_val(0);
            let new_blk = self.contents.split(0, self.contents.get_flag());
            self.current_slot = 0;
            self.contents.set_flag(-1);
            self.contents
                .insert_leaf(self.current_slot, self.searchkey.clone(), datarid);
            return Some(DirEntry::new(first_val, new_blk.get_block_number()));
        }

        self.current_slot += 1;
        self.contents
            .insert_leaf(self.current_slot, self.searchkey.clone(), datarid);

        if !self.contents.is_full() {
            return None;
        }

        // else page is full, so split it
        let first_key = self.contents.get_data_val(0);
        let last_key = self.contents.get_data_val(self.contents.get_num_recs() - 1);

        if last_key == first_key {
            let new_blk = self.contents.split(1, self.contents.get_flag());
            self.contents.set_flag(new_blk.get_block_number());
            return None;
        } else {
            let mut split_pos = self.contents.get_num_recs() / 2;
            let mut split_key = self.contents.get_data_val(split_pos);

            if split_key == first_key {
                while self.contents.get_data_val(split_pos) == split_key {
                    split_pos += 1;
                }
                split_key = self.contents.get_data_val(split_pos);
            } else {
                while self.contents.get_data_val(split_pos - 1) == split_key {
                    split_pos -= 1;
                }
            }
            let new_blk = self.contents.split(split_pos, -1);
            return Some(DirEntry::new(split_key, new_blk.get_block_number()));
        }
    }

    fn try_overflow(&mut self) -> bool {
        let first_key = self.contents.get_data_val(0);
        let flag = self.contents.get_flag();

        if self.searchkey != first_key || flag < 0 {
            return false;
        }

        self.contents.close();
        let next_blk = BlockId::new(self.filename.clone(), flag);
        self.contents = BTPage::new(self.tx.clone(), next_blk, self.layout.clone());
        self.current_slot = 0;
        true
    }
}
