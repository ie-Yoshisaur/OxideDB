// no docs
// no comments
// no error handlings
// no variable name edit
use crate::parse::parser::Parser;
use crate::parse::query_data::QueryData;
use crate::parse::update_data::UpdateData;
use std::backtrace::Backtrace;
use std::error::Error;
use std::panic::{catch_unwind, AssertUnwindSafe};

#[test]
fn parser_test() -> Result<(), Box<dyn Error>> {
    let test_cases = vec![
        ("select name from users where id = 1;", true),
        ("update users set name = 'Alice' where id = 1;", true),
        ("select from where;", false),
        ("update set where;", false),
    ];

    for (input, expected) in test_cases {
        let mut parser = Parser::new(input);

        let result = catch_unwind(AssertUnwindSafe(|| {
            if input.starts_with("select") {
                let _: QueryData = parser.query();
            } else {
                let _: Option<UpdateData> = parser.update_cmd();
            }
        }));

        if expected {
            assert!(
                result.is_ok(),
                "Test failed for input '{}'. Expected no panic but got one. Backtrace: {:?}",
                input,
                Backtrace::capture()
            );
            println!("yes");
        } else {
            assert!(
                result.is_err(),
                "Test failed for input '{}'. Expected a panic but didn't get one. Backtrace: {:?}",
                input,
                Backtrace::capture()
            );
            println!("no");
        }
    }
    Ok(())
}
