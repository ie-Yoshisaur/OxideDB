// no docs
// no comments
// no error handlings
// no variable name edit
use crate::parse::lexer::Lexer;

pub struct PredParser<'a> {
    lex: Lexer<'a>,
}

impl<'a> PredParser<'a> {
    pub fn new(s: &'a str) -> Self {
        Self { lex: Lexer::new(s) }
    }

    pub fn field(&mut self) -> String {
        self.lex.eat_id()
    }

    pub fn constant(&mut self) {
        if self.lex.match_string_constant() {
            self.lex.eat_string_constant();
        } else {
            self.lex.eat_int_constant();
        }
    }

    pub fn expression(&mut self) {
        if self.lex.match_id() {
            self.field();
        } else {
            self.constant();
        }
    }

    pub fn term(&mut self) {
        self.expression();
        self.lex.eat_delim('=');
        self.expression();
    }

    pub fn predicate(&mut self) {
        self.term();
        if self.lex.match_keyword("and") {
            self.lex.eat_keyword("and");
            self.predicate();
        }
    }
}
