use anyhow::Result;
use rust_port::*;
use std::io::Read;

fn main() -> Result<()> {
    let mut catalog = Catalog::open("catalog.sqlite".to_string())?;
    catalog.create_table(
        &"Example1".to_string(),
        ["att1".to_string(), "att2".to_string()].as_slice(),
        ["INTEGER".to_string(), "STRING".to_string()].as_slice(),
    );
    catalog.create_table(
        &"Example2".to_string(),
        ["att2".to_string(), "att3".to_string()].as_slice(),
        ["STRING".to_string(), "STRING".to_string()].as_slice(),
    );
    let compiler = QueryCompiler::new(&catalog);

    let file = std::fs::File::open("test-phase-2.sql")?;

    let mut buf_reader = std::io::BufReader::new(file);

    let mut contents = String::new();
    buf_reader.read_to_string(&mut contents)?;

    let query_execution_tree = compiler.compile(&contents)?;

    println!("{}", query_execution_tree.as_string());

    Ok(())
}
