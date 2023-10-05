// no docs
// no comments
// no error handlings
// no variable name edit
use crate::file::block_id::BlockId;
use crate::query::constant::Constant;
use crate::record::field_type::FieldType;
use crate::record::layout::Layout;
use crate::record::record_id::RecordId;
use crate::transaction::transaction::Transaction;
use std::sync::Arc;
use std::sync::Mutex;

pub struct BTPage {
    tx: Arc<Mutex<Transaction>>,
    current_blk: Option<BlockId>,
    layout: Layout,
}

impl BTPage {
    pub fn new(tx: Arc<Mutex<Transaction>>, current_blk: BlockId, layout: Layout) -> Self {
        tx.lock().unwrap().pin(current_blk.clone());

        Self {
            tx,
            current_blk: Some(current_blk),
            layout,
        }
    }

    pub fn find_slot_before(&self, searchkey: Constant) -> i32 {
        let mut slot = 0;
        while slot < self.get_num_recs() && self.get_data_val(slot) < searchkey {
            slot += 1;
        }
        slot as i32 - 1
    }

    pub fn close(&mut self) {
        if let Some(current_blk) = self.current_blk.take() {
            self.tx.lock().unwrap().unpin(current_blk);
        }
        self.current_blk = None;
    }

    pub fn is_full(&self) -> bool {
        self.slotpos(self.get_num_recs() + 1) as usize >= self.tx.lock().unwrap().block_size()
    }

    pub fn split(&mut self, split_pos: i32, flag: i32) -> BlockId {
        let new_blk = self.append_new(flag);
        let mut new_page = BTPage::new(self.tx.clone(), new_blk.clone(), self.layout.clone());

        self.transfer_recs(split_pos, &mut new_page);

        new_page.set_flag(flag);
        new_page.close();

        new_blk
    }

    pub fn get_data_val(&self, _slot: i32) -> Constant {
        unimplemented!()
    }

    pub fn get_flag(&self) -> i32 {
        self.tx
            .lock()
            .unwrap()
            .get_int(self.current_blk.as_ref().unwrap().clone(), 0)
            .unwrap()
            .unwrap()
    }

    pub fn set_flag(&mut self, val: i32) {
        self.tx
            .lock()
            .unwrap()
            .set_int(self.current_blk.as_ref().unwrap().clone(), 0, val, true)
            .unwrap();
    }

    fn append_new(&mut self, flag: i32) -> BlockId {
        let filename = self.current_blk.as_ref().unwrap().get_file_name();
        let new_blk = self.tx.lock().unwrap().append(filename).unwrap();

        self.tx.lock().unwrap().pin(new_blk.clone());

        self.format(&new_blk, flag);

        new_blk
    }

    pub fn format(&mut self, blk: &BlockId, flag: i32) {
        self.tx
            .lock()
            .unwrap()
            .set_int(blk.clone(), 0, flag, false)
            .unwrap();
        self.tx
            .lock()
            .unwrap()
            .set_int(blk.clone(), std::mem::size_of::<i32>() as i32, 0, false)
            .unwrap();

        let recsize = self.layout.get_slot_size() as i32;
        let block_size = self.tx.lock().unwrap().block_size() as i32;

        let mut pos = 2 * std::mem::size_of::<i32>() as i32;
        while pos + recsize <= block_size {
            self.make_default_record(blk, pos);
            pos += recsize;
        }
    }

    fn make_default_record(&self, blk: &BlockId, pos: i32) {
        for fldname in self.layout.get_schema().lock().unwrap().get_fields() {
            let offset = self.layout.get_offset(&fldname).unwrap() as i32;
            if self
                .layout
                .get_schema()
                .lock()
                .unwrap()
                .get_field_type(&fldname)
                .unwrap()
                == FieldType::Integer
            {
                self.tx
                    .lock()
                    .unwrap()
                    .set_int(blk.clone(), pos + offset, 0, false)
                    .unwrap();
            } else {
                self.tx
                    .lock()
                    .unwrap()
                    .set_string(blk.clone(), pos + offset, &"".to_string(), false)
                    .unwrap();
            }
        }
    }

    pub fn get_child_num(&self, slot: i32) -> i32 {
        self.get_int(slot, "block")
    }

    pub fn insert_dir(&mut self, slot: i32, val: Constant, blknum: i32) {
        self.insert(slot);
        self.set_val(slot, "dataval", val);
        self.set_int(slot, "block", blknum);
    }

    pub fn get_data_rid(&self, slot: i32) -> RecordId {
        RecordId::new(self.get_int(slot, "block"), self.get_int(slot, "id"))
    }

    pub fn insert_leaf(&mut self, slot: i32, val: Constant, rid: RecordId) {
        self.insert(slot);
        self.set_val(slot, "dataval", val);
        self.set_int(slot, "block", rid.get_block_number());
        self.set_int(slot, "id", rid.get_slot_number());
    }

    pub fn delete(&mut self, slot: i32) {
        for i in (slot + 1)..self.get_num_recs() {
            self.copy_record(i, i - 1);
        }
        self.set_num_recs(self.get_num_recs() - 1);
    }

    pub fn get_num_recs(&self) -> i32 {
        self.tx
            .lock()
            .unwrap()
            .get_int(
                self.current_blk.as_ref().unwrap().clone(),
                std::mem::size_of::<i32>() as i32,
            )
            .unwrap()
            .unwrap()
    }

    fn get_int(&self, slot: i32, fldname: &str) -> i32 {
        let pos = self.fldpos(slot, fldname);
        self.tx
            .lock()
            .unwrap()
            .get_int(self.current_blk.as_ref().unwrap().clone(), pos)
            .unwrap()
            .unwrap()
    }

    fn get_string(&self, slot: i32, fldname: &str) -> String {
        let pos = self.fldpos(slot, fldname);
        self.tx
            .lock()
            .unwrap()
            .get_string(self.current_blk.as_ref().unwrap().clone(), pos)
            .unwrap()
            .unwrap()
    }

    fn get_val(&self, slot: i32, fldname: &str) -> Constant {
        let type_ = self
            .layout
            .get_schema()
            .lock()
            .unwrap()
            .get_field_type(fldname)
            .unwrap();
        if type_ == FieldType::Integer {
            Constant::Int(self.get_int(slot, fldname))
        } else {
            Constant::Str(self.get_string(slot, fldname))
        }
    }

    fn set_int(&mut self, slot: i32, fldname: &str, val: i32) {
        let pos = self.fldpos(slot, fldname);
        self.tx
            .lock()
            .unwrap()
            .set_int(self.current_blk.as_ref().unwrap().clone(), pos, val, true)
            .unwrap();
    }

    fn set_string(&mut self, slot: i32, fldname: &str, val: String) {
        let pos = self.fldpos(slot, fldname);
        self.tx
            .lock()
            .unwrap()
            .set_string(self.current_blk.as_ref().unwrap().clone(), pos, &val, true)
            .unwrap();
    }

    fn set_val(&mut self, slot: i32, fldname: &str, val: Constant) {
        let type_ = self
            .layout
            .get_schema()
            .lock()
            .unwrap()
            .get_field_type(fldname)
            .unwrap();
        if type_ == FieldType::Integer {
            self.set_int(slot, fldname, val.as_int())
        } else {
            self.set_string(slot, fldname, val.as_str().to_string())
        }
    }

    fn set_num_recs(&mut self, n: i32) {
        self.tx
            .lock()
            .unwrap()
            .set_int(
                self.current_blk.as_ref().unwrap().clone(),
                std::mem::size_of::<i32>() as i32,
                n,
                true,
            )
            .unwrap();
    }

    fn insert(&mut self, slot: i32) {
        for i in (slot + 1..self.get_num_recs()).rev() {
            self.copy_record(i, i - 1);
        }
        self.set_num_recs(self.get_num_recs() + 1);
    }

    fn copy_record(&mut self, from: i32, to: i32) {
        let sch = self.layout.get_schema();
        for fldname in sch.lock().unwrap().get_fields() {
            let val = self.get_val(from, &fldname);
            self.set_val(to, &fldname, val);
        }
    }

    fn transfer_recs(&mut self, slot: i32, dest: &mut BTPage) {
        let mut destslot = 0;
        while slot < self.get_num_recs() {
            dest.insert(destslot);
            let sch = self.layout.get_schema();
            for fldname in sch.lock().unwrap().get_fields() {
                let val = self.get_val(slot, &fldname);
                dest.set_val(destslot, &fldname, val);
            }
            self.delete(slot);
            destslot += 1;
        }
    }

    fn fldpos(&self, slot: i32, fldname: &str) -> i32 {
        let offset = self.layout.get_offset(fldname).unwrap();
        self.slotpos(slot) + offset as i32
    }

    fn slotpos(&self, slot: i32) -> i32 {
        let slotsize = self.layout.get_slot_size();
        (std::mem::size_of::<i32>() * 2) as i32 + (slot * slotsize as i32)
    }
}
