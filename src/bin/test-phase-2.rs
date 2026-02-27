use anyhow::Result;
use rust_port::*;
use std::io::Read;

fn read_file_to_string(filename: &str) -> Result<String> {
    let file = std::fs::File::open(filename)?;
    let mut buf_reader = std::io::BufReader::new(file);

    let mut contents = String::new();
    buf_reader.read_to_string(&mut contents)?;

    Ok(contents)
}

fn main() -> Result<()> {
    let catalog_sql = read_file_to_string("01-catalog.sql")?;

    let mut catalog = Catalog::catalog_from_sql(&catalog_sql)?;
    let compiler = QueryCompiler::new(&catalog);

    let queries = read_file_to_string("all.sql")?;

    for query in queries.split(";") {
        let query = query.trim();
        println!("Query: {}", query);
        let query_execution_tree = match compiler.compile(&query) {
            Ok(query_execution_tree) => query_execution_tree,
            Err(error) => {
                println!("Failed to compile query: {}", error);
                continue;
            }
        };

        println!("{}", query_execution_tree.as_string());
    }

    Ok(())
}
