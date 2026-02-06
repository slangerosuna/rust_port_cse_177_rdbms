#![allow(dead_code)]
#![allow(unused)]

use std::collections::HashMap;

use crate::schema::*;
use crate::types::*;

use anyhow::{Result, anyhow};
use rusqlite::{Connection, params};

pub struct Catalog {
    filename: String,
    conn: Connection,

    table_schema: HashMap<String, Schema>,
}

impl Catalog {
    pub fn open(filename: String) -> Result<Self> {
        let mut conn = Connection::open(&filename)?;
        let mut table_schema = HashMap::new();

        conn.execute_batch("
            CREATE TABLE IF NOT EXISTS Tables (name VARCHAR, num_tuples INT, file VARCHAR);
            CREATE TABLE IF NOT EXISTS Attributes (table_name VARCHAR, position INT, name VARCHAR, type VARCHAR, num_distinct INT);
        ");

        let mut stmt = conn.prepare("SELECT name, num_tuples, file FROM Tables;")?;
        let mut rows = stmt.query([])?;

        while let Some(row) = rows.next()? {
            let table_name: String = row.get("name")?;
            let num_tuples: i32 = row.get("num_tuples")?;
            let filepath: String = row.get("file")?;

            table_schema.insert(table_name, Schema::new_no_attributes(num_tuples, filepath));
        }

        drop(rows);
        stmt.finalize()?;

        let mut stmt = conn.prepare("SELECT name, position, type, num_distinct, table_name FROM Attributes ORDER BY position;")?;
        let mut rows = stmt.query([])?;

        while let Some(row) = rows.next()? {
            let name: String = row.get("name")?;
            let position: i32 = row.get("position")?;
            let type_: String = row.get("type")?;
            let num_distinct: i32 = row.get("num_distinct")?;
            let table_name: String = row.get("table_name")?;

            if position as usize != table_schema.get(&table_name).unwrap().get_num_atts() {
                return Err(anyhow!("Attribute position {position} skipped"));
            }

            let att_names = [name];
            let att_types = [type_];
            let att_distincts = [num_distinct];

            let schema = Schema::from_attributes(&att_names, &att_types, &att_distincts);

            table_schema.get_mut(&table_name).unwrap().append(&schema);
        }

        drop(rows);
        stmt.finalize()?;

        Ok(Catalog {
            filename,
            table_schema,
            conn,
        })
    }

    pub fn save(&mut self) -> Result<()> {
        self.conn.execute_batch(
            "
            BEGIN TRANSACTION;
            DELETE FROM Tables;
            DELETE FROM Attributes;
        ",
        )?;

        let mut stmt = self.conn.prepare("INSERT INTO Tables VALUES(?, ?, ?);")?;

        for (table_name, schema) in self.table_schema.iter() {
            let num_tuples = schema.get_no_tuples() as i32;
            let f_path = schema.get_f_path();

            stmt.execute(params![table_name, num_tuples, f_path]);
        }

        stmt.finalize()?;

        let mut stmt = self
            .conn
            .prepare("INSERT INTO Attributes VALUES(?, ?, ?, ?, ?);")?;

        for (table_name, schema) in self.table_schema.iter() {
            let atts = schema.get_atts();

            for (pos, att) in atts.iter().enumerate() {
                let name = &att.name;
                let num_distinct = att.no_distinct;

                let type_ = match att.type_ {
                    Type::Integer => "INTEGER",
                    Type::Float => "FLOAT",
                    Type::String => "STRING",
                    _ => {
                        return Err(anyhow!(
                            "Invalid type ({:?}) for attribute {name}",
                            att.type_
                        ));
                    }
                };

                stmt.execute(params![table_name, pos, name, type_, num_distinct]);
            }
        }

        stmt.finalize()?;

        self.conn.execute("COMMIT;", []);

        Ok(())
    }

    pub fn get_no_tuples(&self, table: &str) -> Option<i32> {
        Some(self.table_schema.get(table)?.get_no_tuples())
    }

    pub fn set_no_tuples(&mut self, table: &str, no_tuples: i32) -> bool {
        let Some(schema) = self.table_schema.get_mut(table) else {
            return false;
        };
        schema.set_no_tuples(no_tuples);

        true
    }

    pub fn get_data_file(&self, table: &str) -> Option<String> {
        Some(self.table_schema.get(table)?.get_f_path().to_string())
    }

    pub fn set_data_file(&mut self, table: &str, data_file: &str) -> bool {
        let Some(schema) = self.table_schema.get_mut(table) else {
            return false;
        };
        schema.set_f_path(data_file);

        true
    }

    pub fn get_no_distinct(&self, table: &str, attribute: &str) -> Option<i32> {
        self.table_schema.get(table)?.get_distincts(attribute)
    }

    pub fn set_no_distinct(&mut self, table: &str, attribute: &str, no_distinct: i32) -> bool {
        let Some(schema) = self.table_schema.get_mut(table) else {
            return false;
        };
        schema.set_distincts(attribute, no_distinct)
    }

    pub fn get_tables(&self) -> Vec<String> {
        self.table_schema
            .iter()
            .map(|table_schema| table_schema.0.clone())
            .collect()
    }

    pub fn get_attributes(&self, table: &str) -> Option<Vec<String>> {
        Some(
            self.table_schema
                .get(table)?
                .get_atts()
                .iter()
                .map(|att| att.name.clone())
                .collect::<Vec<_>>(),
        )
    }

    pub fn get_schema(&self, table: &str) -> Option<&Schema> {
        self.table_schema.get(table)
    }

    pub fn create_table(
        &mut self,
        table: &String,
        attributes: &[String],
        attribute_types: &[String],
    ) -> bool {
        if self.table_schema.get(table).is_some() {
            return false;
        }

        let distincts = vec![0; attributes.len()];

        if !attribute_types
            .iter()
            .all(|type_| type_ == "INTEGER" || type_ == "FLOAT" || type_ == "STRING")
        {
            return false;
        }

        self.table_schema.insert(
            table.clone(),
            Schema::from_attributes(attributes, attribute_types, &distincts),
        );

        true
    }

    pub fn drop_table(&mut self, table: &str) -> bool {
        self.table_schema.remove(table).is_some()
    }
}

impl std::fmt::Display for Catalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (name, schema) in self.table_schema.iter() {
            writeln!(f, "{name} {schema}")?;
        }

        Ok(())
    }
}
