use crate::catalog::Catalog;
use crate::comparison::Comparison;
use crate::relop::*;
use crate::schema::Schema;
use anyhow::{Result, anyhow};
use lalrpop_util::*;
use logos::Logos;
use std::collections::HashMap;

mod ast;
mod lexer;

lalrpop_mod!(
    #[allow(unused)]
    grammar,
    "/compiler/grammar.rs"
);

pub use ast::*;
use grammar::*;
use lexer::*;

pub struct QueryCompiler<'a> {
    catalog: &'a Catalog,
}

impl<'a> QueryCompiler<'a> {
    pub fn new(catalog: &'a Catalog) -> Self {
        Self { catalog }
    }
}
