# OxideDB

![test](https://github.com/ie-Yoshisaur/OxideDB/actions/workflows/rust.yml/badge.svg)

OxideDB is a relational database system, developed in Rust.

## Features

- Supported Data Types
  - [x] int
  - [x] varchar  
  - [ ] float
  - [ ] bool
  - [ ] null
  - [ ] date
  - [ ] time
- Transaction Management
  - [x] Concurrency Control
    - Lock granularity is at the block level
  - [ ] Recovery
    - Undo logs are used for recovery.
- SQL Parsing
  - [x] select
  - [x] update
  - [x] delete
  - [x] insert
  - [x] create table
  - [ ] drop
  - [ ] alter
  - [ ] join
  - [x] where
  - [ ] group by
  - [ ] order by
- Indexing
  - [x] hash
  - [ ] B-Tree
- Interface
  - [x] Interactive console
  - [ ] Network
  
## How to Use

To use OxideDB, follow these steps:

1. Install Rust on your system.
2. Clone this repository.
3. Navigate to the repository's root directory.
4. Run `cargo build` to compile the project.
5. Run `cargo run` to start the interactive console.

Please note that OxideDB is still under development and not all features are fully implemented yet.

## Example Queries

Here are some example queries you can run on OxideDB, along with explanations of what each query does:

```sql
create table table1(id int, name varchar(9));
create index id_index on table1 (id);
insert into table1(id, name) values(1, 'User1');
insert into table1(id, name) values(2, 'User2');
select id, name from table1;
select id, name from table1 where id = 1;
create table table2(data int, name2 varchar(9));
insert into table2(data, name2) values(42, 'User1');
insert into table2(data, name2) values(43, 'User2');
select data, name2 from table2;
select id, data from table1, table2 where name=name2;
delete from table1 where id = 1;
select id, name from table1;
```

**Note:**

Please note that if you run a query with incorrect syntax, OxideDB may panic and crash. Always ensure your queries are correctly formatted.

## License

This project is licensed under the MIT License.
