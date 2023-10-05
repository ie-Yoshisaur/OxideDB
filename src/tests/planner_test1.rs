use crate::server::oxide_db::OxideDB;
use std::backtrace::Backtrace;
use std::fs::remove_dir_all;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[test]
fn planner_test1() -> Result<(), Box<dyn std::error::Error>> {
    let test_directory = PathBuf::from("plannertest1");
    let mut db = OxideDB::new(test_directory.clone())?;
    let tx = Arc::new(Mutex::new(db.new_transaction()));

    let planner = db.get_planner().as_ref().unwrap();
    let cmd = "create table T1(A int, B varchar(9))";
    planner.execute_update(cmd, tx.clone());

    let n = 200;
    println!("Inserting {} sequential records.", n);

    for _ in 0..2 {
        for i in 0..n {
            let a = i;
            let b = format!("rec{}", a);
            let cmd = format!("insert into T1(A,B) values({}, '{}')", a, b);
            planner.execute_update(&cmd, tx.clone());
        }
    }

    let qry = "select B from T1 where A=10";
    let plan = planner.create_query_plan(qry, tx.clone());

    let scan = plan.lock().unwrap().open();
    let mut scan = scan.lock().unwrap();

    let mut count = 0;
    let mut retrieved_values = vec![];

    while scan.next() {
        let b = scan.get_string("B").unwrap();
        retrieved_values.push(b);
        count += 1;
    }

    if count != 2 || retrieved_values.iter().any(|val| val != "rec10") {
        panic!(
            "Test failed. Expected 2 occurrences of 'rec10', but got {} occurrence(s) and values: {:?}. Backtrace: {:?}",
            count,
            retrieved_values,
            Backtrace::capture()
        );
    }

    scan.close();
    tx.lock().unwrap().commit().unwrap();

    remove_dir_all(test_directory).expect(&format!(
        "Failed to remove test directory.\nBacktrace: {:#?}",
        Backtrace::capture()
    ));

    Ok(())
}
