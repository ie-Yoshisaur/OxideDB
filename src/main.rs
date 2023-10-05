use crate::interface::connection_adapter::ConnectionAdapter;
use crate::interface::embedded::embedded_driver::EmbeddedDriver;
use crate::interface::embedded::embedded_statement::EmbeddedStatement;
use crate::record::field_type::FieldType;
use std::error::Error;
use std::io;
use std::io::Write;
use std::sync::Arc;
mod buffer;
mod file;
mod index;
mod interface;
mod log;
mod materialize;
mod metadata;
mod multibuffer;
mod opt;
mod parse;
mod plan;
mod query;
mod record;
mod server;
mod transaction;

fn main() -> Result<(), Box<dyn Error>> {
    let mut input = String::new();
    let driver: EmbeddedDriver = EmbeddedDriver::new();
    let conn = driver.connect(&input)?;
    let stmt = Arc::new(conn.create_statement()?);

    loop {
        print!("\nSQL> ");
        io::stdout().flush()?;
        input.clear();
        io::stdin().read_line(&mut input)?;
        let cmd = input.trim();
        if cmd.starts_with("exit") {
            break;
        } else if cmd.starts_with("select") {
            do_query(stmt.clone(), cmd)?;
        } else {
            do_update(stmt.clone(), cmd)?;
        }
    }

    Ok(())
}

fn do_query(stmt: Arc<EmbeddedStatement>, cmd: &str) -> Result<(), Box<dyn Error>> {
    let mut rs = stmt.execute_query(cmd)?;
    let md = rs.get_meta_data();
    let num_cols = md.get_column_count();
    let mut total_width = 0;

    // print header
    for i in 1..=num_cols {
        let field_name = md.get_column_name(i)?;
        let width = md.get_column_display_size(i)?;
        total_width += width;
        print!("{:<width$}", field_name, width = width as usize);
    }
    println!();
    println!("{}", "-".repeat(total_width as usize));

    // print records
    while rs.next()? {
        for i in 1..=num_cols {
            let field_name = md.get_column_name(i)?;
            let field_type = md.get_column_type(i)?;
            let width = md.get_column_display_size(i)?;
            if field_type == FieldType::Integer {
                let ival = rs.get_int(&field_name)?;
                let formatted = format!("{:<width$}", ival, width = width as usize);
                print!("{}", formatted);
            } else {
                let sval = rs.get_string(&field_name)?;
                let formatted = format!("{:<width$}", sval, width = width as usize);
                print!("{}", formatted);
            }
        }
        println!();
    }

    Ok(())
}

fn do_update(stmt: Arc<EmbeddedStatement>, cmd: &str) -> Result<(), Box<dyn Error>> {
    let how_many = stmt.execute_update(cmd)?;
    println!("{} records processed", how_many);

    Ok(())
}

#[cfg(test)]
mod tests;
