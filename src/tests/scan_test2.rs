// no docs
// no comments
// no error handlings
// no variable name edit
use crate::query::expression::Expression;
use crate::query::predicate::Predicate;
use crate::query::product_scan::ProductScan;
use crate::query::project_scan::ProjectScan;
use crate::query::select_scan::SelectScan;
use crate::query::term::Term;
use crate::record::layout::Layout;
use crate::record::schema::Schema;
use crate::record::table_scan::TableScan;
use crate::server::oxide_db::OxideDB;
use std::backtrace::Backtrace;
use std::collections::HashSet;
use std::fs::remove_dir_all;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[test]
fn scan_test2() -> Result<(), Box<dyn std::error::Error>> {
    let test_directory = PathBuf::from("scantest2");
    let db = OxideDB::new(test_directory.clone())?;
    let tx = Arc::new(Mutex::new(db.new_transaction()));

    // Schema and layout for table T1
    let mut sch1 = Schema::new();
    sch1.add_int_field("A".to_string());
    sch1.add_string_field("B".to_string(), 9);
    let layout1 = Arc::new(Layout::new(Arc::new(Mutex::new(sch1)))?);

    // Populate table T1
    let mut us1 = TableScan::new(tx.clone(), "T1", layout1.clone())?;
    let n = 200;
    for i in 0..n {
        us1.insert()?;
        us1.set_int("A", i)?;
        us1.set_string("B", format!("bbb{}", i))?;
    }
    us1.close();

    // Schema and layout for table T2
    let mut sch2 = Schema::new();
    sch2.add_int_field("C".to_string());
    sch2.add_string_field("D".to_string(), 9);
    let layout2 = Arc::new(Layout::new(Arc::new(Mutex::new(sch2)))?);

    // Populate table T2
    let mut us2 = TableScan::new(tx.clone(), "T2", layout2.clone())?;
    for i in 0..n {
        us2.insert()?;
        us2.set_int("C", n - i - 1)?;
        us2.set_string("D", format!("ddd{}", n - i - 1))?;
    }
    us2.close();

    // Create Scans
    let s1 = Arc::new(Mutex::new(TableScan::new(
        tx.clone(),
        "T1",
        layout1.clone(),
    )?));
    let s2 = Arc::new(Mutex::new(TableScan::new(
        tx.clone(),
        "T2",
        layout2.clone(),
    )?));
    let s3 = Arc::new(Mutex::new(ProductScan::new(s1, s2)));

    // Create Predicate
    let t = Term::new(
        Expression::FieldName("A".to_string()),
        Expression::FieldName("C".to_string()),
    );
    let pred = Predicate::new_from_term(t);

    // Filter using SelectScan
    let s4 = Arc::new(Mutex::new(SelectScan::new(s3, pred)));

    // Create Projection
    let mut fields: HashSet<String> = HashSet::new();
    fields.insert("B".to_string());
    fields.insert("D".to_string());
    let mut s5 = ProjectScan::new(s4, fields);

    // Create a vector to store the resulting pairs of B and D.
    let mut results: Vec<(String, String)> = Vec::new();

    // Print the result and collect into results
    while s5.next() {
        let b = s5.get_string("B").unwrap();
        let d = s5.get_string("D").unwrap();
        println!("{} {}", b, d);
        results.push((b, d));
    }

    s5.close();

    // Create expected results
    let expected_results: Vec<(String, String)> = (0..n)
        .map(|i| (format!("bbb{}", i), format!("ddd{}", i)))
        .collect();

    assert_eq!(
        results,
        expected_results,
        "Unexpected values. Backtrace: {:#?}",
        Backtrace::capture()
    );

    // Commit transaction
    tx.lock().unwrap().commit()?;

    remove_dir_all(test_directory).expect(&format!(
        "Failed to remove test directory.\nBacktrace: {:#?}",
        Backtrace::capture()
    ));
    Ok(())
}
