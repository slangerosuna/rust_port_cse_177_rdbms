#![allow(dead_code)]
#![allow(unused)]

use crate::schema::*;
use crate::types::*;

pub struct Catalog {}

impl Catalog {
    pub fn new(filename: String) -> Self {
        Catalog {}
    }

    // I'm using `Result` instead of `bool` because `false` represents failure in the C++ version,
    // so it seemed more idiomatic to use `Result` in Rust.
    pub fn save(&mut self) -> Result<()> {
        todo!()
    }

    // Same goes with using `Option` instead of `bool` here. `Option` being used because failure
    // here is to be expected and only has one cause
    pub fn get_no_tuples(&self, table: &str, no_tuples: &mut i32) -> Option<()> {
        todo!()
    }

    pub fn set_no_tuples(&mut self, table: &str, no_tuples: &i32) {
        todo!()
    }

    pub fn get_data_file(&self, table: &str, data_file: &mut String) -> Option<()> {
        todo!()
    }

    pub fn set_data_file(&mut self, table: &str, data_file: &str) {
        todo!()
    }

    pub fn get_no_distinct(
        &self,
        table: &str,
        attribute: &str,
        no_distinct: &mut i32,
    ) -> Option<()> {
        todo!()
    }

    pub fn set_no_distinct(&mut self, table: &str, attribute: &str, no_distinct: &i32) {
        todo!()
    }

    pub fn get_tables(&self, tables: &mut Vec<String>) {
        todo!()
    }

    pub fn get_attributes(&self, table: &str, attributes: &mut Vec<String>) -> Option<()> {
        todo!()
    }

    pub fn get_schema(&self, table: &str, schema: &mut Schema) -> Option<()> {
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

    pub fn drop_table(table: &str) -> Option<()> {
        todo!()
    }
}

impl std::fmt::Display for Catalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Catalog")
    }
}
