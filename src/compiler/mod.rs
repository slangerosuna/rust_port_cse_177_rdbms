use crate::*;
use lalrpop_util::*;

mod ast;
use ast::*;

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
    fn compute_join_cost(&self, combo: &[usize], cnf: &Option<(Cnf, Record, Schema)>, scans: &[(Schema, RelOp)]) -> usize {
        let mut schema = scans[combo[0]].0.clone();
        let mut cost = 0.0;

        for i in 1..combo.len() {
            let next_schema = &scans[combo[i]].0;

            let cur_atts = schema.get_num_atts();
            let next_atts = next_schema.get_num_atts();

            let estimated_cost = cur_atts * next_atts;
            let mut estimated_cost = estimated_cost as f64;

            let old_schema = schema.clone();
            schema.append(&scans[combo[i]].0);
 
            let cnf = match cnf {
                Some(cnf) => cnf.0.project_to_schema(&cnf.2, &schema),
                None => Cnf::new(),
            };// Cnf::extract_cnf(&schema, next_schema);

            for comparison in cnf.comparisons() {
                let left_distincts = match comparison.operand1 {
                    Target::Left => old_schema
                        .get_distincts(&schema.get_atts()[comparison.which_att1 as usize].name)
                        .unwrap_or(1) as f64,
                    Target::Right => next_schema
                        .get_distincts(&schema.get_atts()[comparison.which_att1 as usize].name)
                        .unwrap_or(1) as f64,
                    Target::Literal => 1.0,
                };
                let right_distincts = match comparison.operand2 {
                    Target::Left => old_schema
                        .get_distincts(&schema.get_atts()[comparison.which_att2 as usize].name)
                        .unwrap_or(1) as f64,
                    Target::Right => next_schema
                        .get_distincts(&schema.get_atts()[comparison.which_att2 as usize].name)
                        .unwrap_or(1) as f64,
                    Target::Literal => 1.0,
                };


                let max_distincts = f64::max(left_distincts, right_distincts);
                if max_distincts != 0.0 {
                    estimated_cost /= max_distincts;
                }
            }

            cost += estimated_cost;
        }

        // using usize instead of f64 here because it impls Ord
        cost as usize
    }

    fn choose_join(
        &self,
        predicate: Cnf,
        left: RelOp,
        right: RelOp,
        left_schema: &Schema,
        right_schema: &Schema,
    ) -> RelOp {
        // TODO: Implement the actual join choice logic based on the schemas and estimated costs

        RelOp::NestedLoopJoin(NestedLoopJoin {
            predicate,

            records: Vec::new(),

            left_producer: Box::new(left),
            right_producer: Box::new(right),
        })
    }

    fn dynamic_scan_order(&self, cnf: Option<(Cnf, Record, Schema)>, scans: Vec<(Schema, RelOp)>) -> anyhow::Result<(Schema, RelOp)> {
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
                let join_cost = self.compute_join_cost(&combo, &cnf, &scans);
                (combo, join_cost)
            })
            .max_by_key(|(_, cost)| *cost)
            .ok_or_else(|| anyhow::anyhow!("No scan combinations found"))?;

        let mut scans: Vec<_> = scans.into_iter().map(Some).collect();

        let (mut schema, mut relop) = scans[combo[0]].take().unwrap();

        for i in 1..combo.len() {
            let next_scan = scans[combo[i]].take().unwrap();
            let next_schema = next_scan.0;
            let next_relop = next_scan.1;

            let old_schema = schema.clone();

            schema.append(&next_schema);

            let cnf = match cnf {
                Some(ref cnf) => cnf.0.project_to_schema(&cnf.2, &schema),
                None => Cnf::new(),
            };

            relop = self.choose_join(cnf, relop, next_relop, &schema, &next_schema);
        }

        Ok((schema, relop))
    }

    fn greedy_scan_order(&self, cnf: Option<(Cnf, Record, Schema)>, scans: Vec<(Schema, RelOp)>) -> anyhow::Result<(Schema, RelOp)> {
        // TODO: Actually implement the greedy algorithm described in 16.6.6
        self.dynamic_scan_order(cnf, scans)
    }

    fn optimal_scan_relop(&self, cnf: Option<(Cnf, Record, Schema)>, table_names: &[String]) -> anyhow::Result<(Schema, RelOp)> {
        let scans = table_names
            .iter()
            .map(|table_name| {
                let schema = self
                    .catalog
                    .get_schema(&table_name)
                    .ok_or_else(|| anyhow::anyhow!("Table '{}' not found in catalog", table_name))?
                    .clone();

                let path = self.catalog.get_data_file(&table_name).ok_or_else(|| {
                    anyhow::anyhow!("Data file for table '{}' not found in catalog", table_name)
                })?;

                if path == "" {
                    Ok((schema, RelOp::EmptyTableScan))
                } else {
                    let mut file = DBFile::new();
                    if let Err(e) = file.open(&path) {
                        // TODO: Make it actually fail, for now we just print the error and
                        // continue with an empty scan as we just want the query plan to be
                        // generated
                        println!("{e}");
                    }
                    let scan = RelOp::Scan(Scan { file });

                    Ok((schema, scan))
                }
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        if scans.len() <= 4 {
            self.dynamic_scan_order(cnf, scans)
        } else {
            self.greedy_scan_order(cnf, scans)
        }
    }

    fn compile_condition(
        &self,
        condition: &ast::Condition,
        schema: &Schema,
    ) -> anyhow::Result<(Cnf, Record)> {
        match condition {
            Condition::And(left, right) => {
                let (mut left_cnf, mut left_constants) = self.compile_condition(&left, schema)?;
                let (mut right_cnf, right_constants) = self.compile_condition(&right, schema)?;

                right_cnf.increase_constants_offset(left_constants.len());
                left_cnf *= right_cnf;
                left_constants.merge_right(&right_constants);

                Ok((left_cnf, left_constants))
            }
            Condition::Or(left, right) => {
                let (mut left_cnf, mut left_constants) = self.compile_condition(&left, schema)?;
                let (mut right_cnf, right_constants) = self.compile_condition(&right, schema)?;

                right_cnf.increase_constants_offset(left_constants.len());
                left_cnf += right_cnf;
                left_constants.merge_right(&right_constants);

                Ok((left_cnf, left_constants))
            }
            Condition::Not(internal) => {
                let (cnf, constants) = self.compile_condition(&internal, schema)?;
                let cnf = cnf.negation();

                Ok((cnf, constants))
            }

            Condition::BoolLiteral(value) => {
                let cnf = if *value {
                    Cnf::new()
                } else {
                    Cnf::new().negation()
                };

                Ok((cnf, Record::new()))
            }
            Condition::Comparison(left, right, op) => {
                let mut record = Record::new();

                let mut att_type = None;

                macro_rules! get_expr_target {
                    ($value: ident) => (match $value.as_ref() {
                        ConditionExpr::StrLit(lit) => {
                            if let Some(att_type) = att_type {
                                if att_type != Type::String {
                                    anyhow::bail!("Type mismatch in condition: expected {:?}, found STRING literal", att_type);
                                }
                            } else {
                                att_type = Some(Type::String);
                            }
                            record.push_str(lit);
                            (Target::Literal, (record.len() - 1) as i32)
                        },
                        ConditionExpr::Arith(arith) => {
                            match arith {
                                ArithExpr::IntLit(value) => {
                                    if let Some(att_type) = att_type {
                                        if att_type != Type::Integer {
                                            anyhow::bail!("Type mismatch in condition: expected {:?}, found INT literal", att_type);
                                        }
                                    } else {
                                        att_type = Some(Type::Integer);
                                    }

                                    record.push_int(*value);
                                    (Target::Literal, (record.len() - 1) as i32)
                                },
                                ArithExpr::FltLit(value) => {
                                    if let Some(att_type) = att_type {
                                        if att_type != Type::Float {
                                            anyhow::bail!("Type mismatch in condition: expected {:?}, found FLT literal", att_type);
                                        }
                                    } else {
                                        att_type = Some(Type::Float);
                                    }

                                    record.push_flt(*value);
                                    (Target::Literal, (record.len() - 1) as i32)
                                },
                                ArithExpr::Load(att) => {
                                    let att_index = schema.index_of(att).ok_or_else(|| {
                                        anyhow::anyhow!("Attribute '{}' not found in schema", att)
                                    })?;

                                    let att_type_in_schema = schema.get_atts()[att_index].type_;

                                    if let Some(att_type) = att_type {
                                        if att_type != att_type_in_schema {
                                            anyhow::bail!("Type mismatch in condition: expected {:?}, found attribute '{}' of type {:?}", att_type, att, att_type_in_schema);
                                        }
                                    } else {
                                        att_type = Some(att_type_in_schema);
                                    }

                                    (Target::Left, att_index as i32)
                                },

                                _ => anyhow::bail!("Unsupported arithmetic expression in condition"),
                            }
                        },
                    })
                }

                let (operand1, which_att1) = get_expr_target!(left);
                let (operand2, which_att2) = get_expr_target!(right);

                let comparison = Comparison {
                    operand1,
                    which_att1,

                    operand2,
                    which_att2,

                    att_type: att_type.unwrap(),
                    op: *op,
                };

                Ok((comparison.into(), record))
            }
        }
    }

    fn compile_ast(&self, query: ast::Query) -> anyhow::Result<(Schema, RelOp)> {
        match query {
            ast::Query::Select {
                atts,
                from,
                r#where,
                distinct,
            } => {
                let (mut schema, mut producer) = if let Some(r#where) = r#where {
                    // TODO: estimate effect on no_tuples and update schema accordingly
                    if matches!(*from, ast::Query::Scan { .. }) {
                        let ast::Query::Scan { table_names } = *from else { unreachable!() };
                        let mut schema = table_names
                            .iter()
                            .filter_map(|table_name| self.catalog.get_schema(table_name))
                            .cloned()
                            .reduce(|mut acc, schema| {
                                acc.append(&schema);
                                acc
                            })
                            .ok_or_else(|| anyhow::anyhow!("No tables found in scan"))?;

                        let (predicate, constants) = self.compile_condition(&r#where, &schema)?;
                        // TODO: refactor to handle constants properly instead of just ignoring
                        // them like this

                        let (schema, producer) = self.optimal_scan_relop(Some((predicate, constants, schema)), &table_names)?;
                        let (predicate, constants) = self.compile_condition(&r#where, &schema)?;

                        let producer = RelOp::Select(Select {
                            producer: Box::new(producer),
                            predicate,
                            constants,
                        });

                        (schema, producer)
                    } else {
                        let (schema, producer) = self.compile_ast(*from)?;
                        let (predicate, constants) = self.compile_condition(&r#where, &schema)?;

                        let producer = RelOp::Select(Select {
                            producer: Box::new(producer),
                            predicate,
                            constants,
                        });

                        (schema, producer)
                    }
                } else {
                    self.compile_ast(*from)?
                };

                if let ast::SelectAtts::Atts(atts) = atts {
                    if atts
                        .iter()
                        .any(|att| matches!(att, ast::SelectArg::Aggregate(_)))
                    {
                        match producer {
                            RelOp::GroupBy(ref mut groupby) => {
                                // TODO: Add the aggregate functions into the GroupBy relop so that
                                // they can be computed during grouping
                            }
                            _ => {
                                // TODO: Add a GroupBy relop with an empty grouping to compute the
                                // aggregates
                            }
                        }
                    }
                    let mut atts_to_keep = atts
                        .iter()
                        .filter_map(|att| match att {
                            ast::SelectArg::Name(att) => Some(att),
                            _ => None,
                        })
                        .map(|att| {
                            schema.index_of(att).map(|i| i as i32).ok_or_else(|| {
                                anyhow::anyhow!("Attribute '{:?}' not found in schema", att)
                            })
                        })
                        .collect::<anyhow::Result<Vec<_>>>()?;

                    // TODO: Add the part where you also keep the aggregates

                    // checks whether or not the projection is the identity operation, in which
                    // case we can ignore it
                    if !(atts_to_keep == (0..schema.get_num_atts() as i32).collect::<Vec<_>>()) {
                        schema.project(&atts_to_keep);

                        producer = RelOp::Project(Project {
                            producer: Box::new(producer),
                            atts_to_keep,
                        });
                    }
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
                let grouping = OrderMaker::from_atts(
                    &schema,
                    &atts
                        .atts
                        .iter()
                        .filter_map(|s| schema.index_of(&s))
                        .map(|i| i as i32)
                        .collect::<Vec<_>>(),
                );
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
                let ordering = OrderMaker::from_atts(
                    &schema,
                    &atts
                        .atts
                        .iter()
                        .filter_map(|s| schema.index_of(&s))
                        .map(|i| i as i32)
                        .collect::<Vec<_>>(),
                );
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
            ast::Query::Scan { table_names } => self.optimal_scan_relop(None, &table_names),
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
