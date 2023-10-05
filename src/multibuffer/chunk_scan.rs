use crate::file::block_id::BlockId;
use crate::materialize::sort_scan::SortScan;
use crate::query::constant::Constant;
use crate::query::scan::Scan;
use crate::record::field_type::FieldType;
use crate::record::layout::Layout;
use crate::record::record_id::RecordId;
use crate::record::record_page::RecordPage;
use crate::record::table_scan::TableScan;
use crate::transaction::transaction::Transaction;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

pub struct ChunkScan {
    buffs: VecDeque<RecordPage>,
    tx: Arc<Mutex<Transaction>>,
    filename: String,
    layout: Arc<Layout>,
    startbnum: i32,
    endbnum: i32,
    currentbnum: i32,
    rp: RecordPage,
    currentslot: i32,
}

impl ChunkScan {
    pub fn new(
        tx: Arc<Mutex<Transaction>>,
        filename: String,
        layout: Arc<Layout>,
        startbnum: i32,
        endbnum: i32,
    ) -> Self {
        let mut buffs = VecDeque::new();
        for i in startbnum..=endbnum {
            let blk = BlockId::new(filename.clone(), i);
            buffs.push_back(RecordPage::new(tx.clone(), blk, layout.clone()));
        }
        let rp = buffs[0].clone();
        Self {
            buffs,
            tx,
            filename,
            layout,
            startbnum,
            endbnum,
            currentbnum: startbnum,
            rp,
            currentslot: -1,
        }
    }

    fn move_to_block(&mut self, blknum: i32) {
        self.currentbnum = blknum;
        self.rp = self.buffs[(self.currentbnum - self.startbnum) as usize].clone();
        self.currentslot = -1;
    }

    fn close(&mut self) {
        for i in 0..self.buffs.len() {
            let blk = BlockId::new(self.filename.clone(), self.startbnum + i as i32);
            self.tx.lock().unwrap().unpin(blk);
        }
    }

    fn before_first(&mut self) {
        self.move_to_block(self.startbnum);
    }

    fn next(&mut self) -> bool {
        self.currentslot = self.rp.next_after(&mut self.currentslot).unwrap();
        while self.currentslot < 0 {
            if self.currentbnum == self.endbnum {
                return false;
            }
            self.move_to_block(self.rp.get_block().get_block_number() + 1);
            self.currentslot = self.rp.next_after(&mut self.currentslot).unwrap();
        }
        true
    }

    fn get_int(&self, fldname: &str) -> Option<i32> {
        self.rp.get_int(self.currentslot as usize, fldname).ok()
    }

    fn get_string(&self, fldname: &str) -> Option<String> {
        self.rp.get_string(self.currentslot as usize, fldname).ok()
    }

    fn get_value(&self, fldname: &str) -> Option<Constant> {
        if self
            .layout
            .get_schema()
            .lock()
            .unwrap()
            .get_field_type(fldname)
            .unwrap()
            == FieldType::Integer
        {
            Some(Constant::Int(self.get_int(fldname).unwrap()))
        } else {
            Some(Constant::Str(self.get_string(fldname).unwrap()))
        }
    }

    fn has_field(&self, fldname: &str) -> bool {
        self.layout.get_schema().lock().unwrap().has_field(fldname)
    }
}

impl Scan for ChunkScan {
    fn before_first(&mut self) {
        self.before_first()
    }
    fn next(&mut self) -> bool {
        self.next()
    }
    fn get_int(&self, field_name: &str) -> Option<i32> {
        self.get_int(field_name)
    }
    fn get_string(&self, field_name: &str) -> Option<String> {
        self.get_string(field_name)
    }
    fn get_value(&self, field_name: &str) -> Option<Constant> {
        self.get_value(field_name)
    }
    fn has_field(&self, field_name: &str) -> bool {
        self.has_field(field_name)
    }
    fn close(&mut self) {
        self.close()
    }

    fn set_value(&mut self, field_name: &str, value: Constant) {
        unimplemented!()
    }
    fn set_int(&mut self, field_name: &str, value: i32) {
        unimplemented!()
    }
    fn set_string(&mut self, field_name: &str, value: String) {
        unimplemented!()
    }
    fn insert(&mut self) {
        unimplemented!()
    }
    fn delete(&mut self) {
        unimplemented!()
    }
    fn get_record_id(&self) -> RecordId {
        unimplemented!()
    }
    fn move_to_record_id(&mut self, record_id: RecordId) {
        unimplemented!()
    }

    fn as_sort_scan(&self) -> Option<SortScan> {
        None
    }

    fn as_table_scan(&self) -> Option<TableScan> {
        None
    }
}
