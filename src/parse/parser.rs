// no docs
// no comments
// no error handlings
// no variable name edit
use crate::parse::create_index_data::CreateIndexData;
use crate::parse::create_table_data::CreateTableData;
use crate::parse::create_view_data::CreateViewData;
use crate::parse::delete_data::DeleteData;
use crate::parse::insert_data::InsertData;
use crate::parse::lexer::Lexer;
use crate::parse::modify_data::ModifyData;
use crate::parse::query_data::QueryData;
use crate::parse::update_data::UpdateData;
use crate::query::constant::Constant;
use crate::query::expression::Expression;
use crate::query::predicate::Predicate;
use crate::query::term::Term;
use crate::record::schema::Schema;
use std::sync::{Arc, Mutex};

pub struct Parser<'a> {
    lex: Lexer<'a>,
}

impl<'a> Parser<'a> {
    pub fn new(s: &'a str) -> Self {
        Self { lex: Lexer::new(s) }
    }

    pub fn field(&mut self) -> String {
        self.lex.eat_id()
    }

    pub fn constant(&mut self) -> Constant {
        if self.lex.match_string_constant() {
            Constant::Str(self.lex.eat_string_constant())
        } else {
            Constant::Int(self.lex.eat_int_constant())
        }
    }

    pub fn expression(&mut self) -> Expression {
        if self.lex.match_id() {
            Expression::FieldName(self.field())
        } else {
            Expression::Constant(self.constant())
        }
    }

    pub fn term(&mut self) -> Term {
        let lhs = self.expression();
        self.lex.eat_delim('=');
        let rhs = self.expression();
        Term::new(lhs, rhs)
    }

    pub fn predicate(&mut self) -> Predicate {
        let mut pred = Predicate::new_from_term(self.term());
        if self.lex.match_keyword("and") {
            self.lex.eat_keyword("and");
            pred.conjoin_with(self.predicate());
        }
        pred
    }

    pub fn query(&mut self) -> QueryData {
        self.lex.eat_keyword("select");
        let fields = self.select_list();
        self.lex.eat_keyword("from");
        let tables = self.table_list();
        let mut pred = Predicate::new();
        if self.lex.match_keyword("where") {
            self.lex.eat_keyword("where");
            pred = self.predicate();
        }
        QueryData::new(fields, tables, pred)
    }

    pub fn select_list(&mut self) -> Vec<String> {
        let mut fields = Vec::new();
        fields.push(self.field());
        while self.lex.match_delim(',') {
            self.lex.eat_delim(',');
            fields.push(self.field());
        }
        fields
    }

    pub fn table_list(&mut self) -> Vec<String> {
        let mut tables = Vec::new();
        tables.push(self.lex.eat_id());
        while self.lex.match_delim(',') {
            self.lex.eat_delim(',');
            tables.push(self.lex.eat_id());
        }
        tables
    }

    // Methods for parsing the various update commands
    pub fn update_cmd(&mut self) -> Option<UpdateData> {
        if self.lex.match_keyword("insert") {
            return Some(UpdateData::Insert(self.insert()));
        } else if self.lex.match_keyword("delete") {
            return Some(UpdateData::Delete(self.delete()));
        } else if self.lex.match_keyword("update") {
            return Some(UpdateData::Modify(self.modify()));
        } else if self.lex.match_keyword("create") {
            return self.create();
        } else {
            None
        }
    }

    fn create(&mut self) -> Option<UpdateData> {
        self.lex.eat_keyword("create");
        if self.lex.match_keyword("table") {
            return Some(UpdateData::CreateTable(self.create_table()));
        } else if self.lex.match_keyword("view") {
            return Some(UpdateData::CreateView(self.create_view()));
        } else if self.lex.match_keyword("index") {
            return Some(UpdateData::CreateIndex(self.create_index()));
        } else {
            None
        }
    }

    // Method for parsing delete commands
    pub fn delete(&mut self) -> DeleteData {
        self.lex.eat_keyword("delete");
        self.lex.eat_keyword("from");
        let tblname = self.lex.eat_id();
        let mut pred = Predicate::new();
        if self.lex.match_keyword("where") {
            self.lex.eat_keyword("where");
            pred = self.predicate();
        }
        DeleteData::new(tblname, pred)
    }

    // Methods for parsing insert commands
    pub fn insert(&mut self) -> InsertData {
        self.lex.eat_keyword("insert");
        self.lex.eat_keyword("into");
        let tblname = self.lex.eat_id();
        self.lex.eat_delim('(');
        let flds = self.field_list();
        self.lex.eat_delim(')');
        self.lex.eat_keyword("values");
        self.lex.eat_delim('(');
        let vals = self.const_list();
        self.lex.eat_delim(')');
        InsertData::new(tblname, flds, vals)
    }

    pub fn field_list(&mut self) -> Vec<String> {
        let mut fields = Vec::new();
        fields.push(self.field());
        while self.lex.match_delim(',') {
            self.lex.eat_delim(',');
            fields.push(self.field());
        }
        fields
    }

    pub fn const_list(&mut self) -> Vec<Constant> {
        let mut consts = Vec::new();
        consts.push(self.constant());
        while self.lex.match_delim(',') {
            self.lex.eat_delim(',');
            consts.push(self.constant());
        }
        consts
    }

    // Method for parsing modify commands
    pub fn modify(&mut self) -> ModifyData {
        self.lex.eat_keyword("update");
        let tblname = self.lex.eat_id();
        self.lex.eat_keyword("set");
        let fldname = self.field();
        self.lex.eat_delim('=');
        let newval = self.expression();
        let mut pred = Predicate::new();
        if self.lex.match_keyword("where") {
            self.lex.eat_keyword("where");
            pred = self.predicate();
        }
        ModifyData::new(tblname, fldname, newval, pred)
    }

    // Method for parsing create table commands
    pub fn create_table(&mut self) -> CreateTableData {
        self.lex.eat_keyword("table");
        let tblname = self.lex.eat_id();
        self.lex.eat_delim('(');
        println!("create_table!");
        let schema = Arc::new(Mutex::new(self.field_defs()));
        println!("create_table");
        self.lex.eat_delim(')');
        println!("create_table");
        CreateTableData::new(tblname, schema)
    }

    pub fn field_defs(&mut self) -> Schema {
        let mut schema = self.field_def();
        while self.lex.match_delim(',') {
            self.lex.eat_delim(',');
            let schema_to_add = Arc::new(Mutex::new(self.field_defs()));
            schema.add_all(schema_to_add);
        }
        schema
    }

    pub fn field_def(&mut self) -> Schema {
        let fldname = self.field();
        self.field_type(fldname)
    }

    pub fn field_type(&mut self, fldname: String) -> Schema {
        let mut schema = Schema::new();
        if self.lex.match_keyword("int") {
            self.lex.eat_keyword("int");
            schema.add_int_field(fldname);
        } else {
            self.lex.eat_keyword("varchar");
            self.lex.eat_delim('(');
            let str_len = self.lex.eat_int_constant() as usize;
            self.lex.eat_delim(')');
            schema.add_string_field(fldname, str_len);
        }
        schema
    }

    // Method for parsing create view commands
    pub fn create_view(&mut self) -> CreateViewData {
        self.lex.eat_keyword("view");
        let viewname = self.lex.eat_id();
        self.lex.eat_keyword("as");
        let qd = self.query();
        CreateViewData::new(viewname, qd)
    }

    // Method for parsing create index commands
    pub fn create_index(&mut self) -> CreateIndexData {
        self.lex.eat_keyword("index");
        let idxname = self.lex.eat_id();
        self.lex.eat_keyword("on");
        let tblname = self.lex.eat_id();
        self.lex.eat_delim('(');
        let fldname = self.field();
        self.lex.eat_delim(')');
        CreateIndexData::new(idxname, tblname, fldname)
    }
}
