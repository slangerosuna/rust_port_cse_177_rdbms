use rust_port::*;
use anyhow::Result;

fn main() -> Result<()> {
    let catalog = Catalog::open("catalog.sqlite")?;
    let compiler = QueryCompiler::new(&catalog);



    Ok(())
}
