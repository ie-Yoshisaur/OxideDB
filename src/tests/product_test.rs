use crate::query::product_scan::ProductScan;
use crate::record::layout::Layout;
use crate::record::schema::Schema;
use crate::record::table_scan::TableScan;
use crate::server::oxide_db::OxideDB;
use std::backtrace::Backtrace;
use std::fs::remove_dir_all;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

// no docs
// no comments
// no error handlings
// no variable name edit
#[test]
fn product_test() -> Result<(), Box<dyn std::error::Error>> {
    let test_directory = PathBuf::from("producttest");
    let db = OxideDB::new(test_directory.clone()).unwrap();
    let tx = Arc::new(Mutex::new(db.new_transaction()));

    let mut sch1 = Schema::new();
    sch1.add_int_field("A".to_string());
    sch1.add_string_field("B".to_string(), 9);
    let sch1 = Arc::new(Mutex::new(sch1));
    let layout1 = Arc::new(Layout::new(sch1).unwrap());

    let mut ts1 = TableScan::new(tx.clone(), "T1", layout1.clone()).unwrap();

    ts1.before_first();
    let n = 200;
    println!("Inserting {} records into T1.", n);
    for i in 0..n {
        ts1.insert()?;
        ts1.set_int("A", i)?;
        ts1.set_string("B", format!("aaa{}", i))?;
    }
    ts1.close();

    let mut sch2 = Schema::new();
    sch2.add_int_field("C".to_string());
    sch2.add_string_field("D".to_string(), 9);
    let sch2 = Arc::new(Mutex::new(sch2));
    let layout2 = Arc::new(Layout::new(sch2).unwrap());

    let mut ts2 = TableScan::new(tx.clone(), "T2", layout2.clone()).unwrap();

    ts2.before_first();
    println!("Inserting {} records into T2.", n);
    for i in 0..n {
        ts2.insert()?;
        ts2.set_int("C", n - i - 1)?;
        ts2.set_string("D", format!("bbb{}", n - i - 1))?;
    }
    ts2.close();

    let s1 = Arc::new(Mutex::new(
        TableScan::new(tx.clone(), "T1", layout1).unwrap(),
    ));
    let s2 = Arc::new(Mutex::new(
        TableScan::new(tx.clone(), "T2", layout2).unwrap(),
    ));
    let mut s3 = ProductScan::new(s1, s2);

    let mut expected_value = 0;
    let mut count = 0;

    while s3.next() {
        let b_value = s3.get_string("B");
        let expected_string = format!("aaa{}", expected_value);

        if b_value != expected_string {
            eprintln!("Test failed. \nBacktrace: {:#?}", Backtrace::capture());
            panic!("Expected {}, but got {}", expected_string, b_value);
        }

        count += 1;
        if count >= 200 {
            expected_value += 1;
            count = 0;
        }
    }
    s3.close();

    tx.lock().unwrap().commit().unwrap();

    remove_dir_all(test_directory).expect(&format!(
        "Failed to remove test directory.\nBacktrace: {:#?}",
        Backtrace::capture()
    ));
    Ok(())
}
