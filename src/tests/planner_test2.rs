use crate::server::oxide_db::OxideDB;
use std::backtrace::Backtrace;
use std::fs::remove_dir_all;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[test]
fn planner_test2() -> Result<(), Box<dyn std::error::Error>> {
    let test_directory = PathBuf::from("plannertest2");
    let mut db = OxideDB::new(test_directory.clone())?;
    let tx = Arc::new(Mutex::new(db.new_transaction()));

    let planner = db.get_planner().as_ref().unwrap();

    // Creating table T1
    let cmd1 = "create table T1(A int, B varchar(9))";
    planner.lock().unwrap().execute_update(cmd1, tx.clone());

    let n = 200;
    println!("Inserting {} records into T1.", n);
    for i in 0..n {
        let a = i;
        let b = format!("bbb{}", a);
        let cmd = format!("insert into T1(A,B) values({}, '{}')", a, b);
        planner.lock().unwrap().execute_update(&cmd, tx.clone());
    }

    // Creating table T2
    let cmd2 = "create table T2(C int, D varchar(9))";
    planner.lock().unwrap().execute_update(cmd2, tx.clone());

    println!("Inserting {} records into T2.", n);
    for i in 0..n {
        let c = n - i - 1;
        let d = format!("ddd{}", c);
        let cmd = format!("insert into T2(C,D) values({}, '{}')", c, d);
        planner.lock().unwrap().execute_update(&cmd, tx.clone());
    }

    // Querying
    let qry = "select B,D from T1,T2 where A=C";
    let plan = planner.lock().unwrap().create_query_plan(qry, tx.clone());

    let scan = plan.lock().unwrap().open();
    let mut scan = scan.lock().unwrap();

    let mut count = 0;

    while scan.next() {
        let expected_b = format!("bbb{}", count);
        let expected_d = format!("ddd{}", count);

        let actual_b = scan.get_string("B").unwrap();
        let actual_d = scan.get_string("D").unwrap();

        assert!(
            expected_b == actual_b && expected_d == actual_d,
            "Test failed at iteration {}. Expected b: {}, d: {} but got b: {}, d: {}. Backtrace: {:?}",
            count,
            expected_b,
            expected_d,
            actual_b,
            actual_d,
            Backtrace::capture()
        );

        count += 1;
    }

    scan.close();
    tx.lock().unwrap().commit().unwrap();

    remove_dir_all(test_directory).expect(&format!(
        "Failed to remove test directory.\nBacktrace: {:#?}",
        Backtrace::capture()
    ));
    Ok(())
}
