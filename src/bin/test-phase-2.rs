use anyhow::Result;
use rust_port::*;

fn main() -> Result<()> {
    let catalog = Catalog::open("catalog.sqlite".to_string())?;
    let compiler = QueryCompiler::new(&catalog);

    Ok(())
}
