use crate::*;

pub struct QueryExecutionTree {
    pub root: RelOp,
}

impl QueryExecutionTree {
    pub fn as_string(&self) -> String {
        self.root.as_string()
    }
}

impl Iterator for QueryExecutionTree {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        self.root.next()
    }
}

pub enum RelOp {
    Scan(Scan),
    EmptyTableScan,
    Select(Select),
    Project(Project),
    NestedLoopJoin(NestedLoopJoin),
    MergeJoin(MergeJoin),
    HashJoin(HashJoin),
    DupElim(DupElim),
    ApplyFunction(ApplyFunction),
    GroupBy(GroupBy),
    OrderBy(OrderBy),
    WriteOut(WriteOut),
}

impl RelOp {
    fn as_string(&self) -> String {
        fn append_to_line_start(s: &str, line_start: &str) -> String {
            s.lines()
                .map(|line| format!("{line_start}{line}"))
                .reduce(|a, b| format!("{a}\n{b}"))
                .unwrap_or_else(String::new)
        }

        macro_rules! format_producers {
            ($producer:expr) => {
                format!("\u{2514}\u{2500}{}", &append_to_line_start(&$producer.as_string(), "  ")[2..])
            };
            ($producer:expr, $($producers:expr),+) => {
                format!(
                    "\u{251C}\u{2500}{}\n{}",
                    &append_to_line_start(
                        &$producer.as_string(),
                        "\u{2502} "
                    )[4..],
                    format_producers!($($producers),+)
                )
            };
        }

        macro_rules! format_with_producers {
            ($name:expr,$($producer:expr),+) => {
                format!("{}\n{}", $name, format_producers!($($producer),+))
            }
        }

        match self {
            RelOp::Scan(scan) => format!("Scan({})", scan.file.get_file_name()),
            RelOp::EmptyTableScan => "EmptyTableScan".to_string(),

            RelOp::Select(select) => format_with_producers!("Select", select.producer),
            RelOp::Project(project) => format_with_producers!("Project", project.producer),
            RelOp::NestedLoopJoin(join) => format_with_producers!("NestedLoopJoin", join.left_producer, join.right_producer),
            RelOp::MergeJoin(join) => format_with_producers!("MergeJoin", join.left_producer, join.right_producer),
            RelOp::HashJoin(join) => format_with_producers!("HashJoin", join.left_producer, join.right_producer),
            RelOp::DupElim(dup_elim) => format_with_producers!("DupElim", dup_elim.producer),
            RelOp::ApplyFunction(apply_function) => format_with_producers!("ApplyFunction", apply_function.producer),
            RelOp::GroupBy(group_by) => format_with_producers!("GroupBy", group_by.producer),
            RelOp::OrderBy(order_by) => format_with_producers!("OrderBy", order_by.producer),
            RelOp::WriteOut(write_out) => format_with_producers!("WriteOut", write_out.producer),
        }
    }
}


impl Iterator for RelOp {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        macro_rules! impl_next {
            ($($variant:ident),*) => {
                match self {
                    RelOp::EmptyTableScan => None,
                    $(RelOp::$variant(op) => op.next(),)*
                }
             };
        }

        impl_next!(
            Scan,
            Select,
            Project,
            NestedLoopJoin,
            MergeJoin,
            HashJoin,
            DupElim,
            ApplyFunction,
            GroupBy,
            OrderBy,
            WriteOut
        )
    }
}

pub struct Scan {
    pub file: DBFile,
}

impl Scan {
    fn next(&mut self) -> Option<Record> {
        let mut record = Record::new();
        if self.file.get_next(&mut record).ok()? {
            Some(record)
        } else {
            None
        }
    }
}

pub struct Select {
    pub predicate: Cnf,
    pub constants: Record,

    pub producer: Box<RelOp>,
}

impl Select {
    fn next(&mut self) -> Option<Record> {
        self.producer
            .as_mut()
            // Important, constnants must be rhs, so if constants are lhs, the comparison must be
            // flipped before being added to the cnf
            .find(|record| self.predicate.run(&record, &self.constants))
    }
}

pub struct Project {
    pub atts_to_keep: Vec<i32>,
    pub producer: Box<RelOp>,
}

impl Project {
    fn next(&mut self) -> Option<Record> {
        self.producer
            .as_mut()
            .next()
            .map(|mut record| {
                record.project(&self.atts_to_keep)?;
                Some(record)
            })
            .flatten()
    }
}

pub struct NestedLoopJoin {
    pub predicate: Cnf,

    pub records: Vec<Record>,

    pub left_producer: Box<RelOp>,
    pub right_producer: Box<RelOp>,
}

impl NestedLoopJoin {
    fn next(&mut self) -> Option<Record> {
        if self.records.is_empty() {
            let right_collection: Vec<Record> = self.right_producer.by_ref().collect();
            self.records = self
                .left_producer
                .by_ref()
                .flat_map(|left_record| {
                    right_collection
                        .iter()
                        .filter_map(|right_record| {
                            if self.predicate.run(&left_record, right_record) {
                                let mut joined = left_record.clone();
                                joined.merge_right(right_record);

                                Some(joined)
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>()
                })
                .collect();
        }

        self.records.pop()
    }
}

// Assumes input is already sorted, make sure to combine with a `GroupBy` if not
pub struct MergeJoin {
    pub buf: Vec<Record>,

    pub predicate: Cnf,

    pub left_ordering: OrderMaker,
    pub right_ordering: OrderMaker,

    pub left_record: Option<Record>,
    pub right_record: Option<Record>,

    pub left_producer: Box<RelOp>,
    pub right_producer: Box<RelOp>,
}

impl MergeJoin {
    fn next(&mut self) -> Option<Record> {
        if !self.buf.is_empty() {
            return self.buf.pop();
        }

        if self.left_record.is_none() {
            self.left_record = Some(self.left_producer.next()?);
        }

        if self.right_record.is_none() {
            self.right_record = Some(self.right_producer.next()?);
        }

        match self.ordering(
            self.left_record.as_ref().unwrap(),
            self.right_record.as_ref().unwrap(),
        ) {
            std::cmp::Ordering::Less => {
                self.left_record = Some(self.left_producer.next()?);
                return self.next();
            }
            std::cmp::Ordering::Greater => {
                self.right_record = Some(self.right_producer.next()?);
                return self.next();
            }
            _ => (),
        }

        fn cartesian_product(
            predicate: &Cnf,
            left_records: &[Record],
            right_records: &[Record],
        ) -> Vec<Record> {
            left_records
                .iter()
                .flat_map(|left_record| {
                    right_records
                        .iter()
                        .filter(|right_record| predicate.run(left_record, right_record))
                        .map(|right_record| {
                            let mut joined = left_record.clone();
                            joined.merge_right(right_record);
                            joined
                        })
                        .collect::<Vec<_>>()
                })
                .collect()
        }

        let mut right_records = Vec::new();
        let mut next_right_record = self.right_record.take();
        while self.ordering(
            self.left_record.as_ref().unwrap(),
            next_right_record.as_ref().unwrap(),
        ) == std::cmp::Ordering::Equal
        {
            right_records.push(next_right_record.take().unwrap());
            next_right_record = self.right_producer.next();

            if next_right_record.is_none() {
                break;
            }
        }

        let mut left_records = Vec::new();
        let mut next_left_record = self.left_record.take();
        while self.ordering(
            next_left_record.as_ref().unwrap(),
            self.right_record.as_ref().unwrap(),
        ) == std::cmp::Ordering::Equal
        {
            left_records.push(next_left_record.take().unwrap());
            next_left_record = self.left_producer.next();

            if next_left_record.is_none() {
                break;
            }
        }

        self.left_record = next_left_record;
        self.right_record = next_right_record;

        let cartesian_product = cartesian_product(&self.predicate, &left_records, &right_records);
        self.buf.extend(cartesian_product);

        self.next()
    }

    fn ordering(&self, left: &Record, right: &Record) -> std::cmp::Ordering {
        self.left_ordering
            .run_with_different_order(left, right, &self.right_ordering)
    }
}

use std::collections::HashMap;

pub struct HashJoin {
    pub predicate: Cnf,
    pub fill_left: bool,

    pub hash_table: HashMap<Vec<ProjectedData>, Vec<Record>>,

    pub buf: Vec<Record>,

    pub left_projection: Vec<i32>,
    pub right_projection: Vec<i32>,

    pub left_producer: Box<RelOp>,
    pub right_producer: Box<RelOp>,
}

impl HashJoin {
    fn fill_hash_table(&mut self) {
        if self.fill_left {
            for left_record in self.left_producer.by_ref() {
                let projected_data = left_record.get_projected_data(&self.left_projection);
                if let Some(records) = self.hash_table.get_mut(&projected_data) {
                    records.push(left_record);
                } else {
                    self.hash_table.insert(projected_data, vec![left_record]);
                }
            }
        } else {
            for right_record in self.right_producer.by_ref() {
                let projected_data = right_record.get_projected_data(&self.right_projection);
                if let Some(records) = self.hash_table.get_mut(&projected_data) {
                    records.push(right_record);
                } else {
                    self.hash_table.insert(projected_data, vec![right_record]);
                }
            }
        }
    }

    pub fn next(&mut self) -> Option<Record> {
        if !self.buf.is_empty() {
            return self.buf.pop();
        }
        if self.hash_table.len() == 0 {
            self.fill_hash_table();
        }

        if self.fill_left {
            loop {
                let record = self.right_producer.next()?;
                let projected_data = record.get_projected_data(&self.right_projection);

                let Some(records) = self.hash_table.get(&projected_data) else {
                    continue;
                };
                let records: Vec<Record> = records
                    .iter()
                    .filter(|left_record: &&Record| self.predicate.run(*left_record, &record))
                    .map(|left_record: &Record| {
                        let mut joined = left_record.clone();
                        joined.merge_right(&record);
                        joined
                    })
                    .collect();

                self.buf.extend(records);
                return self.next();
            }
        } else {
            loop {
                let record = self.left_producer.next()?;
                let projected_data = record.get_projected_data(&self.left_projection);

                let Some(records) = self.hash_table.get(&projected_data) else {
                    continue;
                };
                let records: Vec<Record> = records
                    .iter()
                    .filter(|right_record: &&Record| self.predicate.run(&record, *right_record))
                    .map(|right_record: &Record| {
                        let mut joined = record.clone();
                        joined.merge_right(&right_record);
                        joined
                    })
                    .collect();

                self.buf.extend(records);
                return self.next();
            }
        }
    }
}

use std::collections::HashSet;

pub struct DupElim {
    pub seen: HashSet<Record>,
    pub producer: Box<RelOp>,
}

impl DupElim {
    fn next(&mut self) -> Option<Record> {
        let next = self.producer.next()?;
        if self.seen.contains(&next) {
            self.next()
        } else {
            self.seen.insert(next.clone());
            Some(next)
        }
    }
}

pub struct ApplyFunction {
    pub function: Function,
    pub producer: Box<RelOp>,
}

impl ApplyFunction {
    fn next(&mut self) -> Option<Record> {
        let record = self.producer.next()?;
        let data = self.function.eval(&record);

        Some(vec![data].into())
    }
}

pub struct GroupBy {
    pub grouping: OrderMaker,

    pub current_group: Vec<Record>,
    pub next_record: Option<Record>,

    pub producer: Box<RelOp>,
}

impl GroupBy {
    fn next(&mut self) -> Option<Record> {
        if self.current_group.is_empty() {
            if let Some(next_record) = self.next_record.take() {
                self.current_group.push(next_record);
            } else {
                self.current_group.push(self.producer.next()?);
            }

            while let Some(record) = self.producer.next() {
                if self.grouping.run(&self.current_group[0], &record) == std::cmp::Ordering::Equal {
                    self.current_group.push(record);
                } else {
                    self.next_record = Some(record);
                    break;
                }
            }
        }

        todo!()
    }
}

pub struct OrderBy {
    pub ordering: OrderMaker,
    pub records: Vec<Record>,
    pub producer: Box<RelOp>,
    pub ascending: bool,
}

impl OrderBy {
    fn next(&mut self) -> Option<Record> {
        if self.records.is_empty() {
            self.records = self.producer.by_ref().collect::<Vec<_>>();

            if self.ascending {
                self.records.sort_by(|a, b| self.ordering.run(b, a));
            } else {
                self.records.sort_by(|a, b| self.ordering.run(a, b));
            }
        }

        self.records.pop()
    }
}

pub struct WriteOut {
    pub file: String,
    pub producer: Box<RelOp>,
}

impl WriteOut {
    fn next(&mut self) -> Option<Record> {
        use std::fs::File;
        use std::io::Write;

        let mut file = File::create(&self.file).ok()?;

        for record in self.producer.by_ref() {
            let bytes = record.to_bytes();
            file.write_all(&bytes).ok()?;
        }

        None
    }
}
