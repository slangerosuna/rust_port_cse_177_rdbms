use crate::*;

pub struct QueryExecutionTree {
    root: RelOp,
}

impl Iterator for QueryExecutionTree {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        self.root.next()
    }
}

pub enum RelOp {
    Scan(Scan),
    Select(Select),
    Project(Project),
    NestedLoopJoin(NestedLoopJoin),
    MergeJoin(MergeJoin),
    DupElim(DupElim),
    ApplyFunction(ApplyFunction),
    GroupBy(GroupBy),
    WriteOut(WriteOut),
}

impl Iterator for RelOp {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            RelOp::Scan(scan) => scan.next(),
            RelOp::Select(select) => select.next(),
            RelOp::Project(project) => project.next(),
            RelOp::NestedLoopJoin(join) => join.next(),
            RelOp::MergeJoin(join) => join.next(),
            RelOp::DupElim(dup_elim) => dup_elim.next(),
            RelOp::ApplyFunction(apply_function) => apply_function.next(),
            RelOp::GroupBy(group_by) => group_by.next(),
            RelOp::WriteOut(write_out) => write_out.next(),
        }
    }
}

pub struct Scan {
    file: DBFile,
    table_name: String,
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
    predicate: Cnf,
    constants: Record,

    producer: Box<RelOp>,
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
    atts_to_keep: Vec<i32>,
    producer: Box<RelOp>,
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
    predicate: Cnf,

    records: Vec<Record>,

    left_producer: Box<RelOp>,
    right_producer: Box<RelOp>,
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
    buf: Vec<Record>,

    predicate: Cnf,

    left_ordering: OrderMaker,
    right_ordering: OrderMaker,

    left_record: Option<Record>,
    right_record: Option<Record>,

    left_producer: Box<RelOp>,
    right_producer: Box<RelOp>,
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

        fn cartesian_product(predicate: &Cnf, left_records: &[Record], right_records: &[Record]) -> Vec<Record> {
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
    predicate: Cnf,
    fill_left: bool,

    hash_table: HashMap<Vec<ProjectedData>, Vec<Record>>,

    buf: Vec<Record>,

    left_projection: Vec<i32>,
    right_projection: Vec<i32>,

    left_producer: Box<RelOp>,
    right_producer: Box<RelOp>,
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

                let Some(records) = self.hash_table.get(&projected_data) else { continue; };
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

                let Some(records) = self.hash_table.get(&projected_data) else { continue; };
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
    schema: Schema,

    seen: HashSet<Record>,
    producer: Box<RelOp>,
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
    function: Function,

    producer: Box<RelOp>,
}

impl ApplyFunction {
    fn next(&mut self) -> Option<Record> {
        let record = self.producer.next()?;
        let data = self.function.eval(&record);

        Some(vec![data].into())
    }
}

pub struct GroupBy {
    grouping: OrderMaker,
    records: Vec<Record>,
    producer: Box<RelOp>,
}

impl GroupBy {
    fn next(&mut self) -> Option<Record> {
        if self.records.is_empty() {
            self.records = self.producer.by_ref().collect::<Vec<_>>();
            self.records.sort_by(|a, b| self.grouping.run(b, a));
        }

        self.records.pop()
    }
}

pub struct WriteOut {
    schema: Schema,
    file: String,

    producer: Box<RelOp>,
}

impl WriteOut {
    fn next(&mut self) -> Option<Record> {
        use std::fs::File;
        use std::io::Write;

        let buf = self
            .producer
            .by_ref()
            .flat_map(|record| record.to_bytes())
            .collect::<Vec<_>>();

        let mut file = File::create(&self.file).ok()?;
        file.write_all(&buf).ok()?;

        None
    }
}
