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

    // Assumes left-deep join trees
    fn compute_join_cost(&self, combo: &[usize], scans: &[(Schema, RelOp)]) -> usize {
        let mut schema = scans[combo[0]].0.clone();
        let mut cost = 0;

        for i in 1..combo.len() {
            let next_schema = &scans[combo[i]].0;

            let cur_atts = schema.get_num_atts();
            let next_atts = next_schema.get_num_atts();

            let estimated_cost = cur_atts * next_atts;
            let mut estimated_cost = estimated_cost as f64;

            let cnf = Cnf::extract_cnf(&schema, next_schema);

            for comparison in cnf.and_list.iter() {
                let left_distincts = schema.get_distincts(&schema.get_atts()[comparison.which_att1 as usize].name).unwrap_or(1) as f64;
                let right_distincts = next_schema.get_distincts(&schema.get_atts()[comparison.which_att2 as usize].name).unwrap_or(1) as f64;

                let max_distincts = f64::max(left_distincts, right_distincts);
                if max_distincts != 0.0 { estimated_cost /= max_distincts; }
            }

            cost += estimated_cost as usize;
            schema.join_right(&scans[combo[i]].0);
        }

        cost
    }

    fn choose_join(&self, left: RelOp, right: RelOp, left_schema: &Schema, right_schema: &Schema) -> RelOp {
        // TODO: Implement the actual join choice logic based on the schemas and estimated costs

        RelOp::NestedLoopJoin(NestedLoopJoin {
            predicate: Cnf::new(),

            records: Vec::new(),

            left_producer: Box::new(left),
            right_producer: Box::new(right),
        })
    }

    fn dynamic_scan_order(
        &self,
        scans: Vec<(Schema, RelOp)>,
    ) -> anyhow::Result<(Schema, RelOp)> {
        fn combinations<T: Clone>(items: Vec<T>) -> Vec<Vec<T>> {
            if items.is_empty() {
                return vec![vec![]];
            }

            let mut result = Vec::new();

            for i in 0..items.len() {
                let mut remaining = items.clone();
                let item = remaining.remove(i);

                for mut combo in combinations(remaining) {
                    combo.push(item.clone());
                    result.push(combo);
                }
            }

            result
        }

        let (combo, _) = combinations((0..scans.len()).collect())
            .into_iter()
            .map(|combo| {
                let join_cost = self.compute_join_cost(&combo, &scans);
                (combo, join_cost)
            })
            .max_by_key(|(_, cost)| *cost)
            .ok_or_else(|| anyhow::anyhow!("No scan combinations found"))?;

        // TODO: estimate effect on no_tuples and update schema accordingly
        let mut scans: Vec<_> = scans.into_iter().map(Some).collect();

        let (mut schema, mut relop) = scans[combo[0]].take().unwrap();

        for i in 1..combo.len() {
            let next_scan = scans[combo[i]].take().unwrap();
            let next_schema = next_scan.0;
            let next_relop = next_scan.1;

            relop = self.choose_join(relop, next_relop, &schema, &next_schema);
            schema.join_right(&next_schema);
        }

        Ok((schema, relop))
    }

    fn greedy_scan_order(
        &self,
        scans: Vec<(Schema, RelOp)>,
    ) -> anyhow::Result<(Schema, RelOp)> {
        // TODO: Actually implement the greedy algorithm described in 16.6.6
        self.dynamic_scan_order(scans)
    }

    fn optimal_scan_relop(
        &self,
        table_names: &[String],
    ) -> anyhow::Result<(Schema, RelOp)> {
        let scans = table_names
            .iter()
            .map(|table_name| {
                let schema = self
                    .catalog
                    .get_schema(&table_name)
                    .ok_or_else(|| anyhow::anyhow!("Table '{}' not found in catalog", table_name))?
                    .clone();

                let path = self
                    .catalog
                    .get_data_file(&table_name)
                    .ok_or_else(|| anyhow::anyhow!("Data file for table '{}' not found in catalog", table_name))?;

                if path == "" {
                    Ok((schema, RelOp::EmptyTableScan))
                } else {
                    let mut file = DBFile::new();
                    file.open(&path)?;
                    let scan = RelOp::Scan(Scan { file });

                    Ok((schema, scan))
                }
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        if scans.len() <= 4 {
            self.dynamic_scan_order(scans)
        } else {
            self.greedy_scan_order(scans)
        }
    }

    fn compile_condition(
        &self,
        condition: &ast::Condition,
        schema: &Schema,
    ) -> anyhow::Result<(Cnf, Record)> {
        todo!()
    }

    fn compile_ast(&self, query: ast::Query) -> anyhow::Result<(Schema, RelOp)> {
        match query {
            ast::Query::Select {
                atts,
                from,
                r#where,
                distinct,
            } => {
                let (mut schema, mut producer) = self.compile_ast(*from)?;

                if let Some(r#where) = r#where {
                    // TODO: estimate effect on no_tuples and update schema accordingly
                    let (predicate, constants) = self.compile_condition(&r#where, &schema)?;

                    producer = RelOp::Select(Select {
                        producer: Box::new(producer),
                        predicate,
                        constants,
                    });
                }

                if let ast::SelectAtts::Atts(atts) = atts {
                    let atts_to_keep = atts
                        .iter()
                        .map(|att| {
                            schema
                                .index_of(att)
                                .map(|i| i as i32)
                                .ok_or_else(|| anyhow::anyhow!("Attribute '{}' not found in schema", att))
                        })
                        .collect::<anyhow::Result<Vec<_>>>()?;

                    schema.project(&atts_to_keep);

                    producer = RelOp::Project(Project {
                        producer: Box::new(producer),
                        atts_to_keep,
                    });
                }

                if distinct {
                    // TODO: estimate effect on no_tuples and update schema accordingly
                    producer = RelOp::DupElim(DupElim {
                        seen: std::collections::HashSet::new(),
                        producer: Box::new(producer),
                    });
                }

                Ok((schema, producer))
            }
            ast::Query::GroupBy { atts, from } => {
                let (schema, producer) = self.compile_ast(*from)?;
                let grouping = todo!();
                let groupby = RelOp::GroupBy(GroupBy {
                    grouping,
                    current_group: Vec::new(),
                    next_record: None,
                    producer: Box::new(producer),
                });

                Ok((schema, groupby))
            }
            ast::Query::OrderBy { asc, atts, from } => {
                let (schema, producer) = self.compile_ast(*from)?;
                let ordering = todo!();
                let orderby = RelOp::OrderBy(OrderBy {
                    producer: Box::new(producer),
                    records: Vec::new(),
                    ordering,
                    ascending: asc,
                });

                Ok((schema, orderby))
            }
            ast::Query::Join {
                join_type,
                left,
                right,
                on,
            } => {
                todo!()
            }
            ast::Query::Scan { table_names } => self.optimal_scan_relop(&table_names),
        }
    }

    pub fn compile(&self, query: &str) -> anyhow::Result<QueryExecutionTree> {
        let ast = parse(query)?;

        let (_schema, relop) = self.compile_ast(ast)?;
        let relop = RelOp::WriteOut(WriteOut {
            file: "output.tbl".to_string(),
            producer: Box::new(relop),
        });

        Ok(QueryExecutionTree { root: relop })
    }
}
