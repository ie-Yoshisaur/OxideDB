use crate::parse::stream_tokenizer::StreamTokenizer;
use crate::parse::stream_tokenizer::Token;
use std::collections::HashSet;

pub struct Lexer<'a> {
    keywords: HashSet<&'static str>,
    tokenizer: StreamTokenizer<'a>,
    current_token: Option<Result<Token, String>>,
}

impl<'a> Lexer<'a> {
    pub fn new(input: &'a str) -> Self {
        let keywords: HashSet<&'static str> = [
            "select", "from", "where", "and", "insert", "into", "values", "delete", "update",
            "set", "create", "table", "int", "varchar", "view", "as", "index", "on",
        ]
        .iter()
        .cloned()
        .collect();

        let mut lexer = Self {
            keywords,
            tokenizer: StreamTokenizer::new(input),
            current_token: None,
        };
        lexer.next_token();
        lexer
    }

    fn next_token(&mut self) {
        self.current_token = self.tokenizer.next_token();
    }

    pub fn match_delim(&self, d: char) -> bool {
        matches!(self.current_token, Some(Ok(Token::Delim(ch))) if ch == d)
    }

    pub fn match_int_constant(&self) -> bool {
        matches!(self.current_token, Some(Ok(Token::IntConstant(_))))
    }

    pub fn match_string_constant(&self) -> bool {
        matches!(self.current_token, Some(Ok(Token::StringConstant(_))))
    }

    pub fn match_keyword(&self, w: &str) -> bool {
        matches!(self.current_token, Some(Ok(Token::Keyword(ref keyword))) if keyword == w)
    }

    pub fn match_id(&self) -> bool {
        matches!(self.current_token, Some(Ok(Token::Id(ref id))) if !self.keywords.contains(id.as_str()))
    }

    pub fn eat_delim(&mut self, d: char) {
        if self.match_delim(d) {
            self.next_token();
        } else {
            panic!("Bad syntax");
        }
    }

    pub fn eat_int_constant(&mut self) -> i32 {
        if let Some(Ok(Token::IntConstant(i))) = &self.current_token {
            let result = *i;
            self.next_token();
            result
        } else {
            panic!("Bad syntax");
        }
    }

    pub fn eat_string_constant(&mut self) -> String {
        if let Some(Ok(Token::StringConstant(ref s))) = &self.current_token {
            let result = s.clone();
            self.next_token();
            result
        } else {
            panic!("Bad syntax");
        }
    }

    pub fn eat_keyword(&mut self, w: &str) {
        if self.match_keyword(w) {
            self.next_token();
        } else {
            panic!("Bad syntax");
        }
    }

    pub fn eat_id(&mut self) -> String {
        if let Some(Ok(Token::Id(ref id))) = &self.current_token {
            if !self.keywords.contains(id.as_str()) {
                let result = id.clone();
                self.next_token();
                return result;
            }
        }
        panic!("Bad syntax");
    }
}
