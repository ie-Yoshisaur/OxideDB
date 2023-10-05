use crate::materialize::materialize_plan::MaterializePlan;
use crate::materialize::record_comparator::RecordComparator;
use crate::materialize::sort_scan::SortScan;
use crate::materialize::temporary_table::TemporaryTable;
use crate::plan::plan::Plan;
use crate::query::scan::Scan;
use crate::record::schema::Schema;
use crate::transaction::transaction::Transaction;
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

pub struct SortPlan {
    tx: Arc<Mutex<Transaction>>,
    p: Arc<Mutex<dyn Plan>>,
    sch: Arc<Mutex<Schema>>,
    comp: RecordComparator,
}

impl SortPlan {
    pub fn new(
        tx: Arc<Mutex<Transaction>>,
        p: Arc<Mutex<dyn Plan>>,
        sort_fields: Vec<String>,
    ) -> Self {
        let sch = p.lock().unwrap().schema();
        let comp = RecordComparator::new(sort_fields);

        Self { tx, p, sch, comp }
    }

    pub fn open(&mut self) -> Arc<Mutex<dyn Scan>> {
        let scan = self.p.lock().unwrap().open();
        let mut runs = self.split_into_runs(scan.clone());
        scan.lock().unwrap().close();
        while runs.len() > 2 {
            runs = self.do_a_merge_iteration(&mut runs);
        }
        Arc::new(Mutex::new(SortScan::new(runs, self.comp.clone())))
    }

    pub fn blocks_accessed(&self) -> i32 {
        let mp = MaterializePlan::new(self.tx.clone(), self.p.clone());
        mp.blocks_accessed()
    }

    pub fn records_output(&self) -> i32 {
        self.p.lock().unwrap().records_output()
    }

    pub fn distinct_values(&self, fldname: &str) -> i32 {
        self.p.lock().unwrap().distinct_values(fldname)
    }

    pub fn schema(&self) -> Arc<Mutex<Schema>> {
        self.sch.clone()
    }

    fn split_into_runs(&self, src: Arc<Mutex<dyn Scan>>) -> VecDeque<TemporaryTable> {
        let mut temps = VecDeque::new();
        src.lock().unwrap().before_first();
        if !src.lock().unwrap().next() {
            return temps;
        }

        let mut current_temp = TemporaryTable::new(self.tx.clone(), self.sch.clone());
        temps.push_back(current_temp.clone());
        let mut current_scan = current_temp.open();

        while self.copy(src.clone(), current_scan.clone()) {
            match self.comp.compare(src.clone(), current_scan.clone()) {
                Ordering::Less => {
                    current_scan.lock().unwrap().close();
                    current_temp = TemporaryTable::new(self.tx.clone(), self.sch.clone());
                    temps.push_back(current_temp.clone());
                    current_scan = current_temp.open();
                }
                _ => {}
            }
        }

        current_scan.lock().unwrap().close();
        temps
    }

    fn do_a_merge_iteration(
        &self,
        runs: &mut VecDeque<TemporaryTable>,
    ) -> VecDeque<TemporaryTable> {
        let mut result = VecDeque::new();

        while runs.len() > 1 {
            let p1 = runs.pop_front().unwrap();
            let p2 = runs.pop_front().unwrap();
            result.push_back(self.merge_two_runs(p1, p2));
        }

        if runs.len() == 1 {
            result.push_back(runs[0].clone());
        }

        result
    }

    fn merge_two_runs(&self, p1: TemporaryTable, p2: TemporaryTable) -> TemporaryTable {
        let src1 = p1.open();
        let src2 = p2.open();
        let result = TemporaryTable::new(self.tx.clone(), self.sch.clone());
        let dest = result.open();

        let mut has_more1 = src1.lock().unwrap().next();
        let mut has_more2 = src2.lock().unwrap().next();

        while has_more1 && has_more2 {
            match self.comp.compare(src1.clone(), src2.clone()) {
                Ordering::Less => {
                    has_more1 = self.copy(src1.clone(), dest.clone());
                }
                _ => {
                    has_more2 = self.copy(src2.clone(), dest.clone());
                }
            }
        }

        if has_more1 {
            while has_more1 {
                has_more1 = self.copy(src1.clone(), dest.clone());
            }
        } else {
            while has_more2 {
                has_more2 = self.copy(src2.clone(), dest.clone());
            }
        }

        src1.lock().unwrap().close();
        src2.lock().unwrap().close();
        dest.lock().unwrap().close();

        result
    }

    fn copy(&self, src: Arc<Mutex<dyn Scan>>, dest: Arc<Mutex<dyn Scan>>) -> bool {
        dest.lock().unwrap().insert();
        for fldname in self.sch.lock().unwrap().get_fields() {
            dest.lock()
                .unwrap()
                .set_value(&fldname, src.lock().unwrap().get_value(&fldname).unwrap());
        }
        src.lock().unwrap().next()
    }
}

impl Plan for SortPlan {
    fn open(&mut self) -> Arc<Mutex<dyn Scan>> {
        self.open()
    }
    fn blocks_accessed(&self) -> i32 {
        self.blocks_accessed()
    }
    fn records_output(&self) -> i32 {
        self.records_output()
    }
    fn distinct_values(&self, fldname: &str) -> i32 {
        self.distinct_values(fldname)
    }
    fn schema(&self) -> Arc<Mutex<Schema>> {
        self.schema()
    }
}
