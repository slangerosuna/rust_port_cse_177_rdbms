#![allow(dead_code)]
#![allow(unused)]

use std::collections::HashMap;

use crate::schema::*;
use crate::types::*;

use rusqlite::{Connection, Result};

pub struct Catalog {
    filename: String,
    conn: Connection,

    table_schema: HashMap<String, Schema>,
}

impl Catalog {
    pub fn new(filename: String) -> Result<Self> {
        let mut conn = Connection::open(&filename)?;
        let mut table_schema = HashMap::new();

        conn.execute_batch("
            CREATE TABLE IF NOT EXISTS Tables (name VARCHAR, num_tuples INT, file VARCHAR);
            CREATE TABLE IF NOT EXISTS Attributes (table_name VARCHAR, position INT, name VARCHAR, type VARCHAR, num_distinct INT);
        ");

        // TODO: fill table_schema from the database

        Ok(Catalog {
            filename,
            table_schema,
            conn,
        })
    }

    pub fn save(&mut self) -> Result<()> {
        todo!()
    }

    pub fn get_no_tuples(&self, table: &str) -> Result<i32> {
        todo!()
    }

    pub fn set_no_tuples(&mut self, table: &str, no_tuples: &i32) {
        todo!()
    }

    pub fn get_data_file(&self, table: &str) -> Result<String> {
        todo!()
    }

    pub fn set_data_file(&mut self, table: &str, data_file: &str) {
        todo!()
    }

    pub fn get_no_distinct(&self, table: &str, attribute: &str) -> Result<i32> {
        todo!()
    }

    pub fn set_no_distinct(&mut self, table: &str, attribute: &str, no_distinct: &i32) {
        todo!()
    }

    pub fn get_tables(&self) -> Vec<String> {
        todo!()
    }

    pub fn get_attributes(&self, table: &str) -> Result<Vec<String>> {
        todo!()
    }

    pub fn get_schema(&self, table: &str) -> Result<Schema> {
        todo!()
    }

    pub fn create_table(
        &mut self,
        table: &str,
        attributes: &[String],
        attribute_types: &[String],
    ) -> Result<()> {
        todo!()
    }

    pub fn drop_table(table: &str) -> Result<()> {
        todo!()
    }
}

impl std::fmt::Display for Catalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Catalog")
    }
}
