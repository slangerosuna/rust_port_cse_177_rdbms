use std::io::BufRead;
use std::mem::size_of;

use crate::schema::*;
use crate::types::*;

#[derive(Debug, Clone)]
pub enum AttrData {
    // I bumped this to i64 because the `AttrData` enum was already 8 bytes, so using i32
    // wouldn't save any space
    Integer(i64),
    Float(f64),

    // points to the start index of the null-terminated string in the strbuf
    String(usize),
}

pub enum MappedAttrData<'a> {
    Integer(&'a i64),
    Float(&'a f64),
    String(&'a str),
}

#[derive(Default, Clone)]
pub struct Record {
    // I got rid of the pointer to `OrderMaker` in the C++ version because doing that isn't
    // particularly idiomatic in Rust
    data: Vec<AttrData>,
    strbuf: String,
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
        let mut strbuf = String::new();
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
                    data.push(AttrData::String(strbuf.len()));
                    if attr_buf.last() != Some(&b'\0') {
                        attr_buf.push(b'\0');
                    }
                    strbuf.push_str(&String::from_utf8_lossy(&attr_buf));
                }
                Type::Name => {
                    panic!("Name not expected in record bin");
                }
            }
        }

        self.data = data;
        self.strbuf = strbuf;

        Some(())
    }

    fn map_attr_data<'a>(&'a self, attr: &'a AttrData) -> MappedAttrData<'a> {
        match attr {
            AttrData::Integer(val) => MappedAttrData::Integer(val),
            AttrData::Float(val) => MappedAttrData::Float(val),
            AttrData::String(start) => {
                let s = &self.strbuf[*start..];
                let end = s.find('\0').unwrap_or(s.len());
                MappedAttrData::String(&s[..end])
            }
        }
    }

    pub fn get_data(&self) -> Vec<MappedAttrData> {
        self.data
            .iter()
            .map(|attr| self.map_attr_data(attr))
            .collect()
    }

    pub fn get_column(&self, column: usize) -> Option<MappedAttrData> {
        self.data.get(column).map(|attr| self.map_attr_data(attr))
    }

    pub fn get_size(&self) -> usize {
        size_of::<AttrData>() * self.data.capacity() + self.strbuf.capacity() * size_of::<u8>()
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
