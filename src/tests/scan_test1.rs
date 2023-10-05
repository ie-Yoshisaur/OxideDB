// no docs
// no comments
// no error handlings
// no variable name edit
use crate::query::constant::Constant;
use crate::query::expression::Expression;
use crate::query::predicate::Predicate;
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
fn scan_test1() -> Result<(), Box<dyn std::error::Error>> {
    let test_directory = PathBuf::from("scantest1");
    let db = OxideDB::new(test_directory.clone())?;
    let tx = Arc::new(Mutex::new(db.new_transaction()));

    // Create schema and layout
    let mut sch1 = Schema::new();
    sch1.add_int_field("A".to_string());
    sch1.add_string_field("B".to_string(), 9);
    let layout = Arc::new(Layout::new(Arc::new(Mutex::new(sch1)))?);

    // Create and populate table scan s1
    let mut s1 = TableScan::new(tx.clone(), "T", layout.clone())?;
    s1.before_first();
    let n = 200;
    println!("Inserting {} records.", n);
    for k in 0..n {
        s1.insert()?;
        s1.set_int("A", k)?;
        s1.set_string("B", format!("rec{}", k))?;
    }
    s1.close();

    // Create table scan s2 and filter it with a select scan s3
    let s2 = Arc::new(Mutex::new(TableScan::new(tx.clone(), "T", layout.clone())?));
    let c = Constant::Int(10);
    let t = Term::new(
        Expression::FieldName("A".to_string()),
        Expression::Constant(c),
    );
    let pred = Predicate::new_from_term(t);
    println!("The predicate is {:?}", pred);
    let s3 = Arc::new(Mutex::new(SelectScan::new(s2, pred)));

    let mut fields: HashSet<String> = HashSet::new();
    fields.insert("B".to_string());
    let mut s4 = ProjectScan::new(s3, fields);

    let mut values: Vec<String> = Vec::new();

    while s4.next() {
        let value = s4.get_string("B").unwrap();
        println!("{}", value);
        values.push(value);
    }
    s4.close();

    assert_eq!(
        values,
        vec!["rec10"],
        "Unexpected values. Backtrace: {:#?}",
        Backtrace::capture()
    );
    s4.close();

    tx.lock().unwrap().commit()?;
    remove_dir_all(test_directory).expect(&format!(
        "Failed to remove test directory.\nBacktrace: {:#?}",
        Backtrace::capture()
    ));
    Ok(())
}
