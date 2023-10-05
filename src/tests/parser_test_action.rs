use crate::parse::parser::Parser;
use std::backtrace::Backtrace;
use std::error::Error;
use std::panic::{catch_unwind, AssertUnwindSafe};

#[test]
fn parser_test_actions() -> Result<(), Box<dyn Error>> {
    // Define the test cases
    let test_cases = vec![
        ("select name from users where id = 1;", "yes"),
        ("update users set name = 'Alice' where id = 1;", "yes"),
        ("select from where;", "no"),
        ("update set where;", "no"),
    ];

    for (input, expected_result) in test_cases.iter() {
        let result = catch_unwind(AssertUnwindSafe(|| {
            let mut parser = Parser::new(input);
            let success: bool;
            if input.starts_with("select") {
                parser.query();
                success = true;
            } else {
                success = parser.update_cmd().is_some();
            }
            success
        }));

        let backtrace = Backtrace::capture();
        match (result, *expected_result) {
            (Ok(true), "yes") | (Err(_), "no") => {} // pass
            _ => {
                let msg = format!("Test failed for input '{}'.", input);
                assert!(false, "{} Backtrace: {:?}", msg, backtrace);
            }
        }
    }
    Ok(())
}
