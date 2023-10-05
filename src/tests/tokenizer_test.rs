// no docs
// no comments
// no error handlings
// no variable name edit
use crate::parse::stream_tokenizer::StreamTokenizer;
use crate::parse::stream_tokenizer::Token;
use std::backtrace::Backtrace;

#[test]
fn test_tokenizer() {
    let query = "select * from table where id = 1 and name = 'John';";
    let mut tokenizer = StreamTokenizer::new(query);

    let mut tokens = Vec::new();
    while let Some(result) = tokenizer.next_token() {
        match result {
            Ok(token) => tokens.push(token),
            Err(err) => panic!("Error: {}", err),
        }
    }

    // Expected tokens
    let expected_tokens = vec![
        Token::Keyword("select".to_string()),
        Token::Delim('*'),
        Token::Keyword("from".to_string()),
        Token::Keyword("table".to_string()),
        Token::Keyword("where".to_string()),
        Token::Id("id".to_string()),
        Token::Delim('='),
        Token::IntConstant(1),
        Token::Keyword("and".to_string()),
        Token::Id("name".to_string()),
        Token::Delim('='),
        Token::StringConstant("John".to_string()),
        Token::Delim(';'),
    ];

    assert_eq!(
        tokens,
        expected_tokens,
        "Tokens did not match. Backtrace: {:#?}",
        Backtrace::capture()
    );
}
