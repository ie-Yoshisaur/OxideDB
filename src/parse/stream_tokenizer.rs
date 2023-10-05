// no docs
// no comments
// no error handlings
// no variable name edit
use std::iter::Peekable;
use std::str::Chars;

#[derive(PartialEq, Debug)]
pub enum Token {
    Delim(char),
    IntConstant(i32),
    StringConstant(String),
    Keyword(String),
    Id(String),
}

pub struct StreamTokenizer<'a> {
    chars: Peekable<Chars<'a>>,
    keywords: Vec<&'static str>,
}

impl<'a> StreamTokenizer<'a> {
    pub fn new(input: &'a str) -> Self {
        Self {
            chars: input.chars().peekable(),
            keywords: vec![
                "select", "from", "where", "and", "insert", "into", "values", "delete", "update",
                "set", "create", "table", "int", "varchar", "view", "as", "index", "on",
            ],
        }
    }

    pub fn next_token(&mut self) -> Option<Result<Token, String>> {
        while let Some(&ch) = self.chars.peek() {
            if ch.is_whitespace() {
                self.chars.next();
            } else {
                break;
            }
        }

        let ch = self.chars.next()?;

        if ch.is_digit(10) {
            let mut num = (ch as u8 - b'0') as i32;
            while let Some(&ch) = self.chars.peek() {
                if ch.is_digit(10) {
                    num = num * 10 + (ch as u8 - b'0') as i32;
                    self.chars.next();
                } else {
                    break;
                }
            }
            return Some(Ok(Token::IntConstant(num)));
        }

        if ch.is_alphabetic() || ch == '_' {
            let mut s = ch.to_string();
            while let Some(&ch) = self.chars.peek() {
                if ch.is_alphanumeric() || ch == '_' {
                    s.push(ch);
                    self.chars.next();
                } else {
                    break;
                }
            }
            if self.keywords.contains(&s.as_str()) {
                return Some(Ok(Token::Keyword(s)));
            } else {
                return Some(Ok(Token::Id(s)));
            }
        }

        if ch == '\'' {
            let mut s = String::new();
            while let Some(&ch) = self.chars.peek() {
                if ch != '\'' {
                    s.push(ch);
                    self.chars.next();
                } else {
                    self.chars.next();
                    break;
                }
            }
            return Some(Ok(Token::StringConstant(s)));
        }

        Some(Ok(Token::Delim(ch)))
    }
}
