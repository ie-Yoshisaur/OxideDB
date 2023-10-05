// no docs
// no comments
// no error handlings
// no variable name edit
use crate::parse::lexer::Lexer;
use std::backtrace::Backtrace;

#[test]
fn lexer_test() {
    let s = "x = 42".to_string();
    let mut lex = Lexer::new(&s);
    let (x, y): (String, i32);

    if lex.match_id() {
        x = lex.eat_id();
        lex.eat_delim('=');
        y = lex.eat_int_constant();
    } else {
        y = lex.eat_int_constant();
        lex.eat_delim('=');
        x = lex.eat_id();
    }

    assert_eq!(
        x,
        "x",
        "Variable x did not match. Backtrace: {:#?}",
        Backtrace::capture()
    );
    assert_eq!(
        y,
        42,
        "Variable y did not match. Backtrace: {:#?}",
        Backtrace::capture()
    );
}
