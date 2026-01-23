#![allow(unused)]

use crate::comparison::Comparison;
use crate::db_file::DBFile;
use crate::record::Record;
use crate::schema::Schema;
use crate::types::Result;
use std::fmt;
use std::rc::Rc;

pub trait RelationalOp {
    fn set_no_pages(&mut self, no_pages: i32);
    fn get_next(&mut self, record: &mut Record) -> Result<bool>;
    fn print(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result;
}

pub struct Scan {
    schema: Schema,
    table_name: String,
    no_pages: i32,
    db_file: Option<DBFile>,
}

impl Scan {
    pub fn new(schema: Schema, table_name: String) -> Self {
        Scan {
            schema,
            table_name,
            no_pages: -1,
            db_file: None,
        }
    }

    pub fn set_db_file(&mut self, mut db_file: DBFile) {
        db_file.set_schema(self.schema.clone());
        self.db_file = Some(db_file);
    }

    pub fn load_from_text(&mut self, file_path: &str) -> Result<()> {
        if let Some(ref mut db_file) = self.db_file {
            db_file.load(&self.schema, file_path)?;
            db_file.move_first();
        }
        Ok(())
    }
}

impl RelationalOp for Scan {
    fn set_no_pages(&mut self, no_pages: i32) {
        self.no_pages = no_pages;
    }

    fn get_next(&mut self, record: &mut Record) -> Result<bool> {
        if let Some(ref mut db_file) = self.db_file {
            db_file.get_next(record)
        } else {
            Ok(false)
        }
    }

    fn print(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SCAN {}", self.table_name)
    }
}

pub struct Select {
    schema: Schema,
    comparison: Comparison,
    producer: Box<dyn RelationalOp>,
    no_pages: i32,
}

impl Select {
    pub fn new(schema: Schema, comparison: Comparison, producer: Box<dyn RelationalOp>) -> Self {
        Select {
            schema,
            comparison,
            producer,
            no_pages: -1,
        }
    }
}

impl RelationalOp for Select {
    fn set_no_pages(&mut self, no_pages: i32) {
        self.no_pages = no_pages;
    }

    fn get_next(&mut self, record: &mut Record) -> Result<bool> {
        let mut temp_record = Record::new();

        if !self.producer.get_next(&mut temp_record)? {
            return Ok(false);
        }

        *record = temp_record;
        Ok(true)
    }

    fn print(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SELECT\n└── ")?;
        self.producer.print(f)
    }
}

pub struct Project {
    schema_in: Schema,
    schema_out: Schema,
    keep_me: Vec<usize>,
    producer: Box<dyn RelationalOp>,
    no_pages: i32,
}

impl Project {
    pub fn new(
        schema_in: Schema,
        schema_out: Schema,
        keep_me: Vec<usize>,
        producer: Box<dyn RelationalOp>,
    ) -> Self {
        Project {
            schema_in,
            schema_out,
            keep_me,
            producer,
            no_pages: -1,
        }
    }
}

impl RelationalOp for Project {
    fn set_no_pages(&mut self, no_pages: i32) {
        self.no_pages = no_pages;
    }

    fn get_next(&mut self, record: &mut Record) -> Result<bool> {
        let mut input_record = Record::new();
        if !self.producer.get_next(&mut input_record)? {
            return Ok(false);
        }
        let mut projected_record = Record::new();

        for &idx in &self.keep_me {
            if let Some(val) = input_record.get_column(idx) {
                *record = input_record;
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn print(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PROJECT\n└── ")?;
        self.producer.print(f)
    }
}

pub struct Join {
    schema_left: Schema,
    schema_right: Schema,
    schema_out: Schema,
    comparison: Comparison,
    left_producer: Box<dyn RelationalOp>,
    right_producer: Box<dyn RelationalOp>,
    no_pages: i32,
}

impl Join {
    pub fn new(
        schema_left: Schema,
        schema_right: Schema,
        schema_out: Schema,
        comparison: Comparison,
        left_producer: Box<dyn RelationalOp>,
        right_producer: Box<dyn RelationalOp>,
    ) -> Self {
        Join {
            schema_left,
            schema_right,
            schema_out,
            comparison,
            left_producer,
            right_producer,
            no_pages: -1,
        }
    }
}

impl RelationalOp for Join {
    fn set_no_pages(&mut self, no_pages: i32) {
        self.no_pages = no_pages;
    }

    fn get_next(&mut self, record: &mut Record) -> Result<bool> {
        // TODO: Implement join logic
        todo!("Implement Join::get_next")
    }

    fn print(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "JOIN\n├── ")?;
        self.left_producer.print(f)?;
        write!(f, "\n└── ")?;
        self.right_producer.print(f)
    }
}

pub struct DuplicateRemoval {
    schema: Schema,
    producer: Box<dyn RelationalOp>,
    no_pages: i32,
}

impl DuplicateRemoval {
    pub fn new(schema: Schema, producer: Box<dyn RelationalOp>) -> Self {
        DuplicateRemoval {
            schema,
            producer,
            no_pages: -1,
        }
    }
}

impl RelationalOp for DuplicateRemoval {
    fn set_no_pages(&mut self, no_pages: i32) {
        self.no_pages = no_pages;
    }

    fn get_next(&mut self, record: &mut Record) -> Result<bool> {
        // TODO: Implement duplicate removal logic
        todo!("Implement DuplicateRemoval::get_next")
    }

    fn print(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DUPLICATE_REMOVAL\n└── ")?;
        self.producer.print(f)
    }
}

pub struct Sum {
    schema_in: Schema,
    schema_out: Schema,
    // TODO: Add Function type when available
    producer: Box<dyn RelationalOp>,
    no_pages: i32,
}

impl Sum {
    pub fn new(schema_in: Schema, schema_out: Schema, producer: Box<dyn RelationalOp>) -> Self {
        Sum {
            schema_in,
            schema_out,
            producer,
            no_pages: -1,
        }
    }
}

impl RelationalOp for Sum {
    fn set_no_pages(&mut self, no_pages: i32) {
        self.no_pages = no_pages;
    }

    fn get_next(&mut self, record: &mut Record) -> Result<bool> {
        // TODO: Implement sum computation logic
        todo!("Implement Sum::get_next")
    }

    fn print(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SUM\n└── ")?;
        self.producer.print(f)
    }
}

pub struct GroupBy {
    schema_in: Schema,
    schema_out: Schema,
    // TODO: Add OrderMaker and Function types when available
    producer: Box<dyn RelationalOp>,
    no_pages: i32,
}

impl GroupBy {
    pub fn new(schema_in: Schema, schema_out: Schema, producer: Box<dyn RelationalOp>) -> Self {
        GroupBy {
            schema_in,
            schema_out,
            producer,
            no_pages: -1,
        }
    }
}

impl RelationalOp for GroupBy {
    fn set_no_pages(&mut self, no_pages: i32) {
        self.no_pages = no_pages;
    }

    fn get_next(&mut self, record: &mut Record) -> Result<bool> {
        // TODO: Implement group by logic
        todo!("Implement GroupBy::get_next")
    }

    fn print(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GROUP_BY\n└── ")?;
        self.producer.print(f)
    }
}

pub struct WriteOut {
    schema: Schema,
    out_file: String,
    producer: Box<dyn RelationalOp>,
    no_pages: i32,
}

impl WriteOut {
    pub fn new(schema: Schema, out_file: String, producer: Box<dyn RelationalOp>) -> Self {
        WriteOut {
            schema,
            out_file,
            producer,
            no_pages: -1,
        }
    }
}

impl RelationalOp for WriteOut {
    fn set_no_pages(&mut self, no_pages: i32) {
        self.no_pages = no_pages;
    }

    fn get_next(&mut self, record: &mut Record) -> Result<bool> {
        // TODO: Implement write out logic
        todo!("Implement WriteOut::get_next")
    }

    fn print(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WRITE_OUT {}\n└── ", self.out_file)?;
        self.producer.print(f)
    }
}

pub struct QueryExecutionTree {
    root: Option<Box<dyn RelationalOp>>,
}

impl QueryExecutionTree {
    pub fn new() -> Self {
        QueryExecutionTree { root: None }
    }

    pub fn set_root(&mut self, root: Box<dyn RelationalOp>) {
        self.root = Some(root);
    }

    pub fn get_root(&mut self) -> Option<&mut Box<dyn RelationalOp>> {
        self.root.as_mut()
    }

    pub fn execute_query(&mut self) -> Result<()> {
        if let Some(ref mut root) = self.root {
            let mut record = Record::new();
            while root.get_next(&mut record)? {
                // Process each record
            }
        }
        Ok(())
    }
}

impl fmt::Display for QueryExecutionTree {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.root {
            Some(root) => root.print(f),
            None => write!(f, "Empty query tree"),
        }
    }
}
