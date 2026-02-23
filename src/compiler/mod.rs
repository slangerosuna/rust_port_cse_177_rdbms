use crate::*;
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

fn parse(query: &str) -> anyhow::Result<ast::Query> {
    let mut lexer = logos::Lexer::new(query);
    let parser = grammar::QueryParser::new();

    let tokens: Vec<_> = std::iter::from_fn(move || {
        let next = lexer.next()?;
        let span = lexer.span();

        Some((span.start, next, span.end))
    })
    .collect();

    if tokens.iter().any(|(_, res, _)| res.is_err()) {
        let errors: Vec<_> = tokens
            .iter()
            .filter_map(|(start, res, end)| res.as_ref().err().map(|err| (err, start, end)))
            .collect();

        anyhow::bail!("Lexing errors at positions: {:?}", errors);
    }

    let tokens: Vec<_> = tokens
        .into_iter()
        .map(|(start, tok, end)| (start, tok.unwrap(), end))
        .collect();

    Ok(parser.parse(tokens)?)
}

impl<'a> QueryCompiler<'a> {
    pub fn new(catalog: &'a Catalog) -> Self {
        Self { catalog }
    }

    fn optimal_scan_relop(&self, table_names: &[String], r#where: Option<&ast::Condition>) -> anyhow::Result<(Schema, RelOp)> {
        todo!()
    }

    fn compile_condition(&self, condition: &ast::Condition, schema: &Schema) -> anyhow::Result<(Cnf, Record)> {
        todo!()
    }

    fn compile_ast(&self, query: ast::Query) -> anyhow::Result<(Schema, RelOp)> {
        match query {
            ast::Query::Select { atts, from, r#where, distinct } => {
                let (unprojected_schema, unprojected_producer) = match *from {
                    ast::Query::Scan { table_names } => self.optimal_scan_relop(&table_names, r#where.as_ref())?,
                    _ => {
                        let (unprojected_schema, producer) = self.compile_ast(*from)?;
                        if let Some(r#where) = r#where {
                            let (predicate, constants) = self.compile_condition(&r#where, &unprojected_schema)?;
                            let producer = Box::new(producer);
                            let select = RelOp::Select(Select {
                                predicate,
                                constants,
                                producer,
                            });

                            (unprojected_schema, select)
                        } else {
                            (unprojected_schema, producer)
                        }
                    },
                };

                todo!()
            },
            ast::Query::GroupBy { atts, from } => {
                todo!()
            },
            ast::Query::OrderBy { asc, atts, from } => {
                todo!()
            },
            ast::Query::Join { join_type, left, right, on } => {
                todo!()
            },
            ast::Query::Scan { table_names } => self.optimal_scan_relop(&table_names, None),
        }
    }

    pub fn compile(&self, query: &str) -> anyhow::Result<QueryExecutionTree> {
        let ast = parse(query)?;

        let (_schema, relop) = self.compile_ast(ast)?;
        let relop = RelOp::WriteOut(WriteOut {
            file: "output.table".to_string(),
            producer: Box::new(relop),
        });

        Ok(QueryExecutionTree { root: relop })
    }
}
