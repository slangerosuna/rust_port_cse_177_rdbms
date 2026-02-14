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
    Join(Join),
    NestedLoopJoin(NestedLoopJoin),
    DupElim(DupElim),
    Sum(Sum),
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
            RelOp::Join(join) => join.next(),
            RelOp::NestedLoopJoin(nested_loop_join) => nested_loop_join.next(),
            RelOp::DupElim(dup_elim) => dup_elim.next(),
            RelOp::Sum(sum) => sum.next(),
            RelOp::GroupBy(group_by) => group_by.next(),
            RelOp::WriteOut(write_out) => write_out.next(),
        }
    }
}

pub struct Scan {
    schema: Schema,
    file: DBFile,
    table_name: String,
}

impl Scan {
    fn next(&mut self) -> Option<Record> {
        unimplemented!()
    }
}

pub struct Select {
    schema: Schema,
    predicate: Cnf,
    constants: Record,

    producer: Box<RelOp>,
}

impl Select {
    fn next(&mut self) -> Option<Record> {
        unimplemented!()
    }
}

pub struct Project {
    schema_in: Schema,
    schema_out: Schema,

    num_att_in: i32,
    num_att_out: i32,
    keep_me: Vec<i32>,

    producer: Box<RelOp>,
}

impl Project {
    fn next(&mut self) -> Option<Record> {
        unimplemented!()
    }
}

pub struct Join {
    schema_left: Schema,
    schema_right: Schema,
    schema_out: Schema,

    predicate: Cnf,

    left_producer: Box<RelOp>,
    right_producer: Box<RelOp>,
}

impl Join {
    fn next(&mut self) -> Option<Record> {
        unimplemented!()
    }
}

pub struct NestedLoopJoin {
    schema_left: Schema,
    schema_right: Schema,
    schema_out: Schema,

    predicate: Cnf,

    left_producer: Box<RelOp>,
    right_producer: Box<RelOp>,

    records: Vec<Record>, // uses Vec instead of LinkedList because the implementations I found
    // seem to only append to the end of the list, so a Vec should be more
    // efficient
    lRec: Record,
}

impl NestedLoopJoin {
    fn next(&mut self) -> Option<Record> {
        unimplemented!()
    }
}

pub struct DupElim {
    schema: Schema,

    producer: Box<RelOp>,
}

impl DupElim {
    fn next(&mut self) -> Option<Record> {
        unimplemented!()
    }
}

pub struct Sum {
    schema_in: Schema,
    schema_out: Schema,

    compute: Function,

    producer: Box<RelOp>,
}

impl Sum {
    fn next(&mut self) -> Option<Record> {
        unimplemented!()
    }
}

pub struct GroupBy {
    schema_in: Schema,
    schema_out: Schema,

    grouping: OrderMaker,
    compute: Function,

    producer: Box<RelOp>,
}

impl GroupBy {
    fn next(&mut self) -> Option<Record> {
        unimplemented!()
    }
}

pub struct WriteOut {
    schema: Schema,
    file: String,

    producer: Box<RelOp>,
}

impl WriteOut {
    fn next(&mut self) -> Option<Record> {
        unimplemented!()
    }
}
