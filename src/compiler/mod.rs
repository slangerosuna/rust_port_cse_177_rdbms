use crate::catalog::Catalog;
use lalrpop_util::*;

mod ast;
mod lexer;

lalrpop_mod!(
    #[allow(unused)]
    grammar,
    "/compiler/grammar.rs"
);

use lexer::*;

pub struct QueryCompiler<'a> {
    catalog: &'a Catalog,
}

impl<'a> QueryCompiler<'a> {
    pub fn new(catalog: &'a Catalog) -> Self {
        Self { catalog }
    }
}
