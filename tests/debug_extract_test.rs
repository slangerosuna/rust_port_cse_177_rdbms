use rust_port::*;
use std::io::Cursor;

#[test]
fn debug_extract_test() {
    let data = "1|Alice|Engineer|75000|";
    println!("Input data: {:?}", data);

    let mut cursor = Cursor::new(data.as_bytes());

    let attributes = vec![
        "id".to_string(),
        "name".to_string(),
        "title".to_string(),
        "salary".to_string(),
    ];
    let types = vec![
        "Integer".to_string(),
        "String".to_string(),
        "String".to_string(),
        "Integer".to_string(),
    ];
    let distincts = vec![0, 0, 0, 0];
    let schema = Schema::new(&attributes, &types, &distincts, 0, "employees.tbl");

    let mut record = Record::new();
    let result = record.extract_next_record(&schema, &mut cursor);
    println!("Extract result: {:?}", result);

    if result.is_some() {
        println!("Record data length: {}", record.get_data().len());
        for (i, data) in record.get_data().iter().enumerate() {
            match data {
                MappedAttrData::Integer(val) => println!("Field {}: Integer({})", i, val),
                MappedAttrData::Float(val) => println!("Field {}: Float({})", i, val),
                MappedAttrData::String(val) => println!("Field {}: String('{}')", i, val),
            }
        }
    }
}
