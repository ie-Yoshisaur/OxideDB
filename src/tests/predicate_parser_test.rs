// no docs
// no comments
// no error handlings
// no variable name edit
use crate::parse::pred_parser::PredParser;
use std::backtrace::Backtrace;
use std::error::Error;
use std::panic::{self, AssertUnwindSafe};

#[test]
fn predicate_parser_test() -> Result<(), Box<dyn Error>> {
    let test_cases = vec![
        ("id = 1", true),
        ("id = 1 and name = 'John'", true),
        ("id 1", false),
        ("name =", false),
    ];

    for (input, should_pass) in test_cases {
        let result = panic::catch_unwind(AssertUnwindSafe(|| {
            let mut parser = PredParser::new(input);
            parser.predicate();
        }));

        if should_pass {
            assert!(
                result.is_ok(),
                "Test failed for input '{}'. Expected to pass. Backtrace: {:?}",
                input,
                Backtrace::capture()
            );
        } else {
            assert!(
                result.is_err(),
                "Test failed for input '{}'. Expected to fail. Backtrace: {:?}",
                input,
                Backtrace::capture()
            );
        }
    }
    Ok(())
}
