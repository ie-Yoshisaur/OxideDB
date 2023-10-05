// no docs
// no comments
// no error handlings
// no variable name edit
use crate::index::index::Index;
use crate::query::constant::Constant;
use crate::query::scan::Scan;
use crate::record::layout::Layout;
use crate::record::record_id::RecordId;
use crate::record::table_scan::TableScan;
use crate::transaction::transaction::Transaction;
use std::sync::{Arc, Mutex};

const NUM_BUCKETS: usize = 100;

pub struct HashIndex {
    tx: Arc<Mutex<Transaction>>,
    idxname: String,
    layout: Arc<Layout>,
    searchkey: Option<Constant>,
    ts: Option<TableScan>,
}

impl HashIndex {
    pub fn new(tx: Arc<Mutex<Transaction>>, idxname: String, layout: Arc<Layout>) -> Self {
        Self {
            tx,
            idxname,
            layout,
            searchkey: None,
            ts: None,
        }
    }

    pub fn before_first(&mut self, searchkey: Constant) {
        self.close();

        self.searchkey = Some(searchkey.clone());
        let bucket = searchkey.hash_code() as usize % NUM_BUCKETS;
        let tblname = format!("{}{}", self.idxname, bucket);

        self.ts = Some(TableScan::new(self.tx.clone(), &tblname, self.layout.clone()).unwrap());
    }

    pub fn next(&mut self) -> bool {
        while let Some(ts) = &mut self.ts {
            if ts.next().unwrap() {
                if ts.get_value("dataval") == self.searchkey {
                    return true;
                }
            } else {
                return false;
            }
        }
        false
    }

    pub fn get_data_rid(&self) -> Option<RecordId> {
        if let Some(ts) = &self.ts {
            let blknum = ts.get_int("block").unwrap();
            let id = ts.get_int("id").unwrap();
            Some(RecordId::new(blknum, id))
        } else {
            None
        }
    }

    pub fn insert(&mut self, val: Constant, rid: RecordId) {
        self.before_first(val.clone());
        if let Some(ts) = &mut self.ts {
            ts.insert().unwrap();
            ts.set_int("block", rid.get_block_number()).unwrap();
            ts.set_int("id", rid.get_slot_number()).unwrap();
            ts.set_value("dataval", val);
        }
    }

    pub fn delete(&mut self, val: Constant, rid: RecordId) {
        self.before_first(val.clone());
        while self.next() {
            if let Some(data_rid) = self.get_data_rid() {
                if data_rid == rid {
                    if let Some(ts) = &mut self.ts {
                        ts.delete();
                    }
                    return;
                }
            }
        }
    }

    pub fn close(&mut self) {
        if let Some(mut ts) = self.ts.take() {
            ts.close();
        }
    }

    pub fn search_cost(numblocks: usize, _rpb: usize) -> usize {
        numblocks / NUM_BUCKETS
    }
}

impl Index for HashIndex {
    fn before_first(&mut self, search_key: Constant) {
        self.before_first(search_key);
    }

    fn next(&mut self) -> bool {
        self.next()
    }

    fn get_data_rid(&self) -> Option<RecordId> {
        self.get_data_rid()
    }

    fn insert(&mut self, data_value: Constant, data_rid: RecordId) {
        self.insert(data_value, data_rid);
    }

    fn delete(&mut self, data_value: Constant, data_rid: RecordId) {
        self.delete(data_value, data_rid);
    }

    fn close(&mut self) {
        self.close();
    }
}
