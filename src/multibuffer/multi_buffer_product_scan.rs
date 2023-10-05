use crate::materialize::sort_scan::SortScan;
use crate::multibuffer::buffer_needs::BufferNeeds;
use crate::multibuffer::chunk_scan::ChunkScan;
use crate::query::constant::Constant;
use crate::query::product_scan::ProductScan;
use crate::query::scan::Scan;
use crate::record::layout::Layout;
use crate::record::record_id::RecordId;
use crate::record::table_scan::TableScan;
use crate::transaction::transaction::Transaction;
use std::sync::{Arc, Mutex};

pub struct MultibufferProductScan {
    tx: Arc<Mutex<Transaction>>,
    lhsscan: Arc<Mutex<dyn Scan>>,
    rhsscan: Option<Arc<Mutex<dyn Scan>>>,
    prodscan: Option<Arc<Mutex<dyn Scan>>>,
    filename: String,
    layout: Arc<Layout>,
    chunksize: usize,
    nextblknum: usize,
    filesize: usize,
}

impl MultibufferProductScan {
    pub fn new(
        tx: Arc<Mutex<Transaction>>,
        lhsscan: Arc<Mutex<dyn Scan>>,
        tblname: &str,
        layout: Arc<Layout>,
    ) -> Self {
        let filename = format!("{}.tbl", tblname);
        let filesize = tx.lock().unwrap().get_size(&filename).unwrap();
        let available = tx.lock().unwrap().available_buffers();
        let chunksize = BufferNeeds::best_factor(available, filesize as i32) as usize;
        Self {
            tx,
            lhsscan,
            rhsscan: None,
            prodscan: None,
            filename,
            layout,
            chunksize,
            nextblknum: 0,
            filesize,
        }
    }

    pub fn before_first(&mut self) {
        self.nextblknum = 0;
        self.use_next_chunk();
    }

    pub fn next(&mut self) -> bool {
        while !self.prodscan.as_ref().unwrap().lock().unwrap().next() {
            if !self.use_next_chunk() {
                return false;
            }
        }
        true
    }

    pub fn close(&mut self) {
        self.prodscan.as_ref().unwrap().lock().unwrap().close();
    }

    pub fn get_value(&self, fldname: &str) -> Option<Constant> {
        self.prodscan
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .get_value(fldname)
    }

    pub fn get_int(&self, fldname: &str) -> Option<i32> {
        self.prodscan
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .get_int(fldname)
    }

    pub fn get_string(&self, fldname: &str) -> Option<String> {
        self.prodscan
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .get_string(fldname)
    }

    pub fn has_field(&self, fldname: &str) -> bool {
        self.prodscan
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .has_field(fldname)
    }

    fn use_next_chunk(&mut self) -> bool {
        if self.nextblknum >= self.filesize {
            return false;
        }
        if let Some(scan) = &self.rhsscan {
            scan.lock().unwrap().close();
        }
        let end = self.nextblknum + self.chunksize - 1;
        let end = if end >= self.filesize {
            self.filesize - 1
        } else {
            end
        };
        self.rhsscan = Some(Arc::new(Mutex::new(ChunkScan::new(
            self.tx.clone(),
            self.filename.clone(),
            self.layout.clone(),
            self.nextblknum as i32,
            end as i32,
        ))));
        self.lhsscan.lock().unwrap().before_first();
        self.prodscan = Some(Arc::new(Mutex::new(ProductScan::new(
            self.lhsscan.clone(),
            self.rhsscan.clone().unwrap(),
        ))));
        self.nextblknum = end + 1;
        true
    }
}

impl Scan for MultibufferProductScan {
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
