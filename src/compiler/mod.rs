use crate::catalog::Catalog;
use crate::comparison::Comparison;
use crate::relop::*;
use crate::schema::Schema;
use crate::types::Result;
use lalrpop_util::*;
use logos::Logos;
use std::collections::HashMap;

mod ast;
mod lexer;

lalrpop_mod!(grammar, "/compiler/grammar.rs");

pub use ast::*;
use grammar::*;
use lexer::*;

pub struct QueryCompiler<'a> {
    catalog: &'a Catalog,
}

impl<'a> QueryCompiler<'a> {
    pub fn new(catalog: &'a Catalog) -> Self {
        QueryCompiler { catalog }
    }

    pub fn compile(
        &self,
        tables: &[Table],
        atts_to_select: Option<&[String]>,
        final_function: Option<&FuncOperator>,
        predicate: Option<&AndList>,
        grouping_atts: Option<&[String]>,
        distinct_atts: bool,
        query_tree: &mut QueryExecutionTree,
    ) -> Result<()> {
        let mut table_operators: HashMap<String, Box<dyn RelationalOp>> = HashMap::new();
        let mut table_schemas: HashMap<String, Schema> = HashMap::new();

        for table in tables {
            // TODO: Get schema from catalog
            let schema = Schema::default(); // Placeholder
            let scan = Box::new(Scan::new(schema.clone(), table.name.clone()));
            table_schemas.insert(table.name.clone(), schema);
            table_operators.insert(table.name.clone(), scan);
        }

        if let Some(pred) = predicate {
            self.apply_selections(pred, &mut table_operators, &table_schemas)?;
        }

        let mut root_op = if tables.len() == 1 {
            // Single table query - just take the scan operator
            table_operators.into_iter().next().unwrap().1
        } else {
            self.create_join_tree(tables, &mut table_operators, &table_schemas, predicate)?
        };

        if let Some(atts) = atts_to_select {
            root_op = self.create_project_operator(root_op, atts, &table_schemas)?;
        }

        // Handle DISTINCT
        if distinct_atts && final_function.is_none() {
            let schema = Schema::default(); // TODO: Get proper schema
            root_op = Box::new(DuplicateRemoval::new(schema, root_op));
        }

        if let Some(_grouping) = grouping_atts {
            let schema_in = Schema::default(); // TODO: Get proper schema
            let schema_out = Schema::default(); // TODO: Get proper schema
            root_op = Box::new(GroupBy::new(schema_in, schema_out, root_op));
        }

        if let Some(_func) = final_function {
            let schema_in = Schema::default(); // TODO: Get proper schema
            let schema_out = Schema::default(); // TODO: Get proper schema
            root_op = Box::new(Sum::new(schema_in, schema_out, root_op));
        }

        query_tree.set_root(root_op);

        Ok(())
    }

    fn apply_selections(
        &self,
        predicate: &AndList,
        table_operators: &mut HashMap<String, Box<dyn RelationalOp>>,
        table_schemas: &HashMap<String, Schema>,
    ) -> Result<()> {
        // TODO: Analyze predicate and push down selections
        // For now, we'll create a simple select operator on the first table
        if let Some((table_name, operator)) = table_operators.iter_mut().next() {
            // Create a placeholder comparison
            let comparison = Comparison::new(); // TODO: Build from predicate
            let schema = table_schemas.get(table_name).unwrap().clone();

            // This is a simplified approach - in reality we'd need to:
            // 1. Parse the predicate to extract conditions
            // 2. Determine which conditions can be pushed to which tables
            // 3. Create appropriate Comparison objects
            // 4. Wrap operators with Select as needed
        }
        Ok(())
    }

    fn create_join_tree(
        &self,
        tables: &[Table],
        table_operators: &mut HashMap<String, Box<dyn RelationalOp>>,
        table_schemas: &HashMap<String, Schema>,
        predicate: Option<&AndList>,
    ) -> Result<Box<dyn RelationalOp>> {
        let mut operators: Vec<Box<dyn RelationalOp>> =
            table_operators.drain().map(|(_, op)| op).collect();

        let mut result = operators.pop().unwrap();

        while let Some(right_op) = operators.pop() {
            let left_schema = Schema::default(); // TODO: Get proper schemas
            let right_schema = Schema::default();
            let out_schema = Schema::default();

            let comparison = Comparison::new(); // TODO: Extract from predicate

            result = Box::new(Join::new(
                left_schema,
                right_schema,
                out_schema,
                comparison,
                result,
                right_op,
            ));
        }

        Ok(result)
    }

    fn create_project_operator(
        &self,
        input: Box<dyn RelationalOp>,
        atts_to_select: &[String],
        table_schemas: &HashMap<String, Schema>,
    ) -> Result<Box<dyn RelationalOp>> {
        // TODO: Determine which attributes to keep based on atts_to_select
        let schema_in = Schema::default(); // TODO: Get input schema
        let schema_out = Schema::default(); // TODO: Create output schema
        let keep_me = vec![0, 1, 2]; // TODO: Map attribute names to indices

        Ok(Box::new(Project::new(
            schema_in, schema_out, keep_me, input,
        )))
    }
}

pub fn parse_input(input: &str) -> std::result::Result<Query, String> {
    let mut lex = Token::lexer(input).spanned();
    let mut tokens = Vec::new();
    while let Some((tok, span)) = lex.next() {
        let (start, end) = (span.start, span.end);
        match tok {
            Ok(Token::Error) => {
                return Err(format!("Lex error at {}..{}", start, end));
            }
            Ok(token) => {
                tokens.push((start, token, end));
            }
            Err(_) => {
                return Err(format!("Lex error at {}..{}", start, end));
            }
        }
    }

    let parser = QueryParser::new();
    match parser.parse(tokens.into_iter()) {
        Ok(q) => Ok(q),
        Err(e) => Err(format!("Parse error: {:?}", e)),
    }
}

pub fn compile_query(
    input: &str,
    catalog: &Catalog,
) -> std::result::Result<QueryExecutionTree, String> {
    let query = parse_input(input)?;
    let compiler = QueryCompiler::new(catalog);
    let mut query_tree = QueryExecutionTree::new();

    compiler
        .compile(
            &query.tables,
            query.atts_to_select.as_deref(),
            query.final_function.as_deref(),
            query.predicate.as_ref(),
            query.grouping_atts.as_deref(),
            query.distinct_atts,
            &mut query_tree,
        )
        .map_err(|e| format!("Compilation error: {}", e))?;

    Ok(query_tree)
}
