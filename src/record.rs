use std::io::BufRead;
use std::mem::size_of;

use crate::comparison::*;
use crate::schema::*;
use crate::types::*;

#[derive(Debug, Clone)]
pub enum AttrData {
    // I bumped this to i64 because the `AttrData` enum was already 24 bytes, so using i32
    // wouldn't save any space
    Integer(i64),
    Float(f64),

    // String here is 24 bytes (pointer, length, capacity), which does increate the size of
    // the overall enum to 32 bytes, making the C++ `Record` implementation more space efficient
    // (not counting the temporary 262kb allocation in Record::ExtractNextRecord) but the Rust
    // implementation is safer and also reduces the amount of indirection because you don't have
    // to store the offsets to each element, meaning that the only indirection is when there's
    // strings (which storing null-terminated does tie in terms of indirection for strings, but
    // the rust implementation is better for ints and floats)
    String(String),
}

#[derive(Default, Clone)]
pub struct Record {
    // I got rid of the pointer to `OrderMaker` in the C++ version because doing that isn't
    // particularly idiomatic in Rust
    data: Vec<AttrData>,
}

impl Record {
    pub fn consume(&mut self, to_consume: Vec<AttrData>) {
        self.data = to_consume;
    }

    pub fn extract_next_record(
        &mut self,
        schema: &Schema,
        buf_reader: &mut impl BufRead,
    ) -> Option<()> {
        let atts = schema.get_atts();

        // using the `Vec` here instead of `new char[PAGE_SIZE]` to 1: be more rust-y and 2: not
        // allocate 262 kb unnecessarily
        let mut data = Vec::new();
        let mut attr_buf = Vec::new();

        for (i, att) in atts.iter().enumerate() {
            attr_buf.clear();
            buf_reader.read_until(b'\0', &mut attr_buf).ok()?;

            match att.type_ {
                Type::Integer => {
                    let s = String::from_utf8_lossy(&attr_buf);
                    let val = s.parse().ok()?;

                    data.push(AttrData::Integer(val));
                }
                Type::Float => {
                    let s = String::from_utf8_lossy(&attr_buf);
                    let val = s.parse().ok()?;

                    data.push(AttrData::Float(val));
                }
                Type::String => {
                    let s = String::from_utf8_lossy(&attr_buf);
                    data.push(AttrData::String(s.to_string()));
                }
                Type::Name => {
                    panic!("Name not expected in record bin");
                }
            }
        }

        self.data = data;

        Some(())
    }

    pub fn get_data(&self) -> &Vec<AttrData> {
        &self.data
    }

    pub fn get_column(&self, column: usize) -> Option<&AttrData> {
        self.data.get(column)
    }

    pub fn get_size(&self) -> usize {
        size_of::<AttrData>() * self.data.capacity()
            + self
                .data
                .iter()
                .filter_map(|attr| match attr {
                    AttrData::String(s) => Some(s.capacity()),
                    _ => None,
                })
                .sum::<usize>()
    }

    pub fn clear(&mut self) {
        self.data.clear();
    }

    pub fn project(&mut self, atts_to_keep: &[i32]) -> Option<()> {
        let new_data = atts_to_keep
            .iter()
            .map(|&i| self.data.get(i as usize))
            .collect::<Vec<_>>();

        if new_data.iter().any(|attr| attr.is_none()) {
            return None;
        }

        self.data = new_data
            .into_iter()
            .map(|attr| attr.unwrap().clone())
            .collect();

        Some(())
    }

    // I got rid of the simultaneous merge right, merge left and project in the C++ code in favor
    // of just splitting them up into separate methods. I will replace calls of
    // Record::MergeRecords with a sequence of calls to merge_right/left and project because it is
    // a lot clearer this way
    pub fn merge_right(&mut self, other: &Record) {
        self.data.extend_from_slice(&other.data);
    }

    pub fn merge_left(&mut self, other: &Record) {
        let mut new_data = other.data.clone();
        new_data.extend_from_slice(&self.data);
        self.data = new_data;
    }

    pub fn display(&self, schema: &Schema) {
        let atts = schema.get_atts();

        print!("Record: {{ ");
        for (i, att) in atts.iter().enumerate() {
            match &self.data[i] {
                AttrData::Integer(val) => print!("{}: {} ", att.name, val),
                AttrData::Float(val) => print!("{}: {} ", att.name, val),
                AttrData::String(val) => print!("{}: {} ", att.name, val),
            }

            if i < atts.len() - 1 {
                print!(", ");
            }
        }

        // C++ code doesn't include the << endl, but I added it anyway because it feels weird to
        // bot have
        println!("}}");
    }
}
