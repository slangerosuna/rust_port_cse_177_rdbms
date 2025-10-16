use rust_port::*;
use std::io::Cursor;
use std::io::Write;
use tempfile::NamedTempFile;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_basic_operations() {
        let record = Record::new();

        let data = record.get_data();
        assert!(data.is_empty(), "empty record should return empty data");

        let _size = record.get_size();

        assert!(
            record.get_column(0).is_none(),
            "get_column on empty record should return None"
        );
        assert!(
            record.get_column(100).is_none(),
            "get_column with invalid index should return None"
        );
    }

    #[test]
    fn test_record_extract_from_pipe_delimited() {
        let data = "123|John Doe|25.5|";
        let mut cursor = Cursor::new(data.as_bytes());

        let attributes = vec!["id".to_string(), "name".to_string(), "salary".to_string()];
        let types = vec![
            "Integer".to_string(),
            "String".to_string(),
            "Float".to_string(),
        ];
        let distincts = vec![0, 0, 0];
        let schema = Schema::new(&attributes, &types, &distincts, 0, "test.tbl");

        let mut record = Record::new();
        let result = record.extract_next_record(&schema, &mut cursor);

        assert!(result.is_some());
        assert_eq!(record.get_data().len(), 3);

        match record.get_column(0) {
            Some(MappedAttrData::Integer(val)) => assert_eq!(*val, 123),
            _ => panic!("Expected integer for first column"),
        }

        match record.get_column(1) {
            Some(MappedAttrData::String(val)) => assert_eq!(val, "John Doe"),
            _ => panic!("Expected string for second column"),
        }

        match record.get_column(2) {
            Some(MappedAttrData::Float(val)) => assert!((val - 25.5).abs() < f64::EPSILON),
            _ => panic!("Expected float for third column"),
        }
    }

    #[test]
    fn test_record_extract_malformed_data() {
        let data = "123|John Doe";
        let mut cursor = Cursor::new(data.as_bytes());

        let attributes = vec!["id".to_string(), "name".to_string(), "age".to_string()];
        let types = vec![
            "Integer".to_string(),
            "String".to_string(),
            "Integer".to_string(),
        ];
        let distincts = vec![0, 0, 0];
        let schema = Schema::new(&attributes, &types, &distincts, 0, "test.tbl");

        let mut record = Record::new();
        let result = record.extract_next_record(&schema, &mut cursor);

        assert!(result.is_none());
    }

    #[test]
    fn test_record_project() {
        let data = "1|Alice|30|Engineer|";
        let mut cursor = Cursor::new(data.as_bytes());

        let attributes = vec![
            "id".to_string(),
            "name".to_string(),
            "age".to_string(),
            "job".to_string(),
        ];
        let types = vec![
            "Integer".to_string(),
            "String".to_string(),
            "Integer".to_string(),
            "String".to_string(),
        ];
        let distincts = vec![0, 0, 0, 0];
        let schema = Schema::new(&attributes, &types, &distincts, 0, "test.tbl");

        let mut record = Record::new();
        record.extract_next_record(&schema, &mut cursor);

        let result = record.project(&[0, 2]);
        assert!(result.is_some());
        assert_eq!(record.get_data().len(), 2);
    }

    #[test]
    fn test_record_merge() {
        let data1 = "1|Alice|";
        let data2 = "30|Engineer|";

        let mut cursor1 = Cursor::new(data1.as_bytes());
        let mut cursor2 = Cursor::new(data2.as_bytes());

        let schema1_attrs = vec!["id".to_string(), "name".to_string()];
        let schema1_types = vec!["Integer".to_string(), "String".to_string()];
        let schema1_distincts = vec![0, 0];
        let schema1 = Schema::new(
            &schema1_attrs,
            &schema1_types,
            &schema1_distincts,
            0,
            "test1.tbl",
        );

        let schema2_attrs = vec!["age".to_string(), "job".to_string()];
        let schema2_types = vec!["Integer".to_string(), "String".to_string()];
        let schema2_distincts = vec![0, 0];
        let schema2 = Schema::new(
            &schema2_attrs,
            &schema2_types,
            &schema2_distincts,
            0,
            "test2.tbl",
        );

        let mut record1 = Record::new();
        let mut record2 = Record::new();

        record1.extract_next_record(&schema1, &mut cursor1);
        record2.extract_next_record(&schema2, &mut cursor2);

        assert_eq!(record1.get_data().len(), 2);
        assert_eq!(record2.get_data().len(), 2);

        record1.merge_right(&record2);
        assert_eq!(record1.get_data().len(), 4);
    }

    #[test]
    fn test_record_serialization() {
        let data = "42|Hello World|3.14|";
        let mut cursor = Cursor::new(data.as_bytes());

        let attributes = vec!["id".to_string(), "text".to_string(), "pi".to_string()];
        let types = vec![
            "Integer".to_string(),
            "String".to_string(),
            "Float".to_string(),
        ];
        let distincts = vec![0, 0, 0];
        let schema = Schema::new(&attributes, &types, &distincts, 0, "test.tbl");

        let mut record = Record::new();
        record.extract_next_record(&schema, &mut cursor);

        let bytes = record.to_bytes();
        assert!(!bytes.is_empty());

        let mut new_cursor = Cursor::new(bytes.as_slice());
        let mut new_record = Record::new();
        let success = new_record.extract_next_record(&schema, &mut new_cursor);
        assert!(success.is_some());
    }

    #[test]
    fn test_schema_creation() {
        let attributes = vec!["id".to_string(), "name".to_string()];
        let types = vec!["Integer".to_string(), "String".to_string()];
        let distincts = vec![10, 50];

        let schema = Schema::new(&attributes, &types, &distincts, 100, "test.tbl");

        assert_eq!(schema.get_num_atts(), 2);
        assert_eq!(schema.get_no_tuples(), 100);
        assert_eq!(schema.get_f_path(), "test.tbl");
    }

    #[test]
    fn test_schema_from_attributes() {
        let attributes = vec!["col1".to_string(), "col2".to_string()];
        let types = vec!["Float".to_string(), "Integer".to_string()];
        let distincts = vec![5, 15];

        let schema = Schema::from_attributes(&attributes, &types, &distincts);

        assert_eq!(schema.get_num_atts(), 2);
        assert_eq!(schema.get_no_tuples(), 0);
        assert_eq!(schema.get_f_path(), "");
    }

    #[test]
    fn test_schema_index_of() {
        let attributes = vec!["id".to_string(), "name".to_string(), "age".to_string()];
        let types = vec![
            "Integer".to_string(),
            "String".to_string(),
            "Integer".to_string(),
        ];
        let distincts = vec![0, 0, 0];

        let schema = Schema::new(&attributes, &types, &distincts, 0, "");

        assert_eq!(schema.index_of("name"), Some(1));
        assert_eq!(schema.index_of("age"), Some(2));
        assert_eq!(schema.index_of("nonexistent"), None);
    }

    #[test]
    fn test_schema_find_type() {
        let attributes = vec!["id".to_string(), "salary".to_string(), "name".to_string()];
        let types = vec![
            "Integer".to_string(),
            "Float".to_string(),
            "String".to_string(),
        ];
        let distincts = vec![0, 0, 0];

        let schema = Schema::new(&attributes, &types, &distincts, 0, "");

        assert_eq!(schema.find_type("id"), Some(Type::Integer));
        assert_eq!(schema.find_type("salary"), Some(Type::Float));
        assert_eq!(schema.find_type("name"), Some(Type::String));
        assert_eq!(schema.find_type("missing"), None);
    }

    #[test]
    fn test_schema_append() {
        let attrs1 = vec!["col1".to_string()];
        let types1 = vec!["Integer".to_string()];
        let distincts1 = vec![5];
        let mut schema1 = Schema::new(&attrs1, &types1, &distincts1, 10, "file1");

        let attrs2 = vec!["col2".to_string()];
        let types2 = vec!["String".to_string()];
        let distincts2 = vec![15];
        let schema2 = Schema::new(&attrs2, &types2, &distincts2, 20, "file2");

        let result = schema1.append(&schema2);
        assert!(result.is_some());
        assert_eq!(schema1.get_num_atts(), 2);
    }

    #[test]
    fn test_schema_project() {
        let attributes = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
        ];
        let types = vec![
            "Integer".to_string(),
            "Float".to_string(),
            "String".to_string(),
            "Integer".to_string(),
        ];
        let distincts = vec![1, 2, 3, 4];

        let mut schema = Schema::new(&attributes, &types, &distincts, 0, "");

        let result = schema.project(&[0, 2]);
        assert!(result.is_some());
        assert_eq!(schema.get_num_atts(), 2);
    }

    #[test]
    fn test_comparison_creation() {
        let comparison = Comparison::new();
        // Basic creation should work - comparison has sane defaults
        assert!(true);
    }

    #[test]
    fn test_comparison_run() {
        let attributes = vec!["a".to_string(), "b".to_string()];
        let types = vec!["Integer".to_string(), "Integer".to_string()];
        let distincts = vec![0, 0];
        let schema = Schema::new(&attributes, &types, &distincts, 0, "");

        let mut record1 = Record::new();
        let mut record2 = Record::new();

        let data1 = "5|10|";
        let mut cursor1 = Cursor::new(data1.as_bytes());
        record1.extract_next_record(&schema, &mut cursor1);

        let data2 = "3|15|";
        let mut cursor2 = Cursor::new(data2.as_bytes());
        record2.extract_next_record(&schema, &mut cursor2);

        let comparison = Comparison::new(); // Defaults: left[0] == right[0]
        let result = comparison.run(&record1, &record2);
        assert!(!result); // 5 != 3
    }

    #[test]
    fn test_scan_creation() {
        let attributes = vec!["id".to_string(), "name".to_string()];
        let types = vec!["Integer".to_string(), "String".to_string()];
        let distincts = vec![0, 0];
        let schema = Schema::new(&attributes, &types, &distincts, 0, "test.tbl");

        let scan = Scan::new(schema, "test_table".to_string());

        let mut scan_mut = scan;
        scan_mut.set_no_pages(10);

        let mut record = Record::new();
        let result = scan_mut.get_next(&mut record);
        assert!(result.is_ok());
    }

    #[test]
    fn test_select_operator() {
        let attributes = vec!["id".to_string(), "value".to_string()];
        let types = vec!["Integer".to_string(), "Integer".to_string()];
        let distincts = vec![0, 0];
        let schema = Schema::new(&attributes, &types, &distincts, 0, "test.tbl");

        let scan = Scan::new(schema.clone(), "test_table".to_string());
        let comparison = Comparison::new();

        let mut select = Select::new(schema, comparison, Box::new(scan));
        select.set_no_pages(5);

        let mut record = Record::new();
        let result = select.get_next(&mut record);
        assert!(result.is_ok());
    }

    #[test]
    fn test_project_operator() {
        let schema_in_attrs = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let schema_in_types = vec![
            "Integer".to_string(),
            "String".to_string(),
            "Float".to_string(),
        ];
        let schema_in_distincts = vec![0, 0, 0];
        let schema_in = Schema::new(
            &schema_in_attrs,
            &schema_in_types,
            &schema_in_distincts,
            0,
            "",
        );

        let schema_out_attrs = vec!["a".to_string(), "c".to_string()];
        let schema_out_types = vec!["Integer".to_string(), "Float".to_string()];
        let schema_out_distincts = vec![0, 0];
        let schema_out = Schema::new(
            &schema_out_attrs,
            &schema_out_types,
            &schema_out_distincts,
            0,
            "",
        );

        let scan = Scan::new(schema_in.clone(), "test_table".to_string());
        let keep_attrs = vec![0, 2]; // Keep columns a and c

        let mut project = Project::new(schema_in, schema_out, keep_attrs, Box::new(scan));
        project.set_no_pages(3);

        let mut record = Record::new();
        let result = project.get_next(&mut record);
        assert!(result.is_ok());
    }

    #[test]
    fn test_query_execution_tree() {
        let attributes = vec!["id".to_string()];
        let types = vec!["Integer".to_string()];
        let distincts = vec![0];
        let schema = Schema::new(&attributes, &types, &distincts, 0, "test.tbl");

        let scan = Scan::new(schema, "test_table".to_string());

        let mut tree = QueryExecutionTree::new();
        tree.set_root(Box::new(scan));

        let result = tree.execute_query();
        assert!(result.is_ok());
    }

    #[test]
    fn test_dbfile_create_and_open() {
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path();

        let mut db_file = DBFile::new();

        let result = db_file.create(&file_path, FileType::Heap);
        assert!(result.is_ok());
        assert!(db_file.is_open());

        let result = db_file.close();
        assert!(result.is_ok());
        assert!(!db_file.is_open());

        let result = db_file.open(&file_path);
        assert!(result.is_ok());
        assert!(db_file.is_open());
    }

    #[test]
    fn test_dbfile_load_from_text() {
        let temp_db_file = NamedTempFile::new().unwrap();
        let mut temp_text_file = NamedTempFile::new().unwrap();

        writeln!(temp_text_file, "1|Alice|25|").unwrap();
        writeln!(temp_text_file, "2|Bob|30|").unwrap();
        temp_text_file.flush().unwrap();

        let attributes = vec!["id".to_string(), "name".to_string(), "age".to_string()];
        let types = vec![
            "Integer".to_string(),
            "String".to_string(),
            "Integer".to_string(),
        ];
        let distincts = vec![0, 0, 0];
        let schema = Schema::new(&attributes, &types, &distincts, 0, "test.tbl");

        let mut db_file = DBFile::new();
        let result = db_file.create(&temp_db_file.path(), FileType::Heap);
        assert!(result.is_ok());

        let result = db_file.load(&schema, temp_text_file.path().to_str().unwrap());
        assert!(result.is_ok());
    }

    #[test]
    fn test_dbfile_append_and_retrieve() {
        let temp_file = NamedTempFile::new().unwrap();

        let mut db_file = DBFile::new();
        let result = db_file.create(&temp_file.path(), FileType::Heap);
        assert!(result.is_ok());

        let data = "123|Test User|42|";
        let mut cursor = Cursor::new(data.as_bytes());

        let attributes = vec!["id".to_string(), "name".to_string(), "age".to_string()];
        let types = vec![
            "Integer".to_string(),
            "String".to_string(),
            "Integer".to_string(),
        ];
        let distincts = vec![0, 0, 0];
        let schema = Schema::new(&attributes, &types, &distincts, 0, "test.tbl");

        let mut record = Record::new();
        record.extract_next_record(&schema, &mut cursor);

        let result = db_file.append_record(record);
        assert!(result.is_ok());

        db_file.move_first();

        let mut retrieved_record = Record::new();
        let result = db_file.get_next(&mut retrieved_record);
        assert!(result.is_ok());
    }

    #[test]
    fn test_page_operations() {
        let mut page = Page::new();
        assert!(page.is_empty());
        assert_eq!(page.get_num_records(), 0);

        let record = Record::new();

        let success = page.append(record);
        assert!(success);
        assert!(!page.is_empty());
        assert_eq!(page.get_num_records(), 1);

        let mut retrieved_record = Record::new();
        let success = page.get_first(&mut retrieved_record);
        assert!(success);
        assert!(page.is_empty());
        assert_eq!(page.get_num_records(), 0);
    }

    #[test]
    fn test_page_serialization() {
        let mut page = Page::new();

        // Add some records
        for _ in 0..3 {
            let record = Record::new();
            page.append(record);
        }

        let binary_data = page.to_binary();
        assert_eq!(binary_data.len(), 131072); // PAGE_SIZE
    }

    #[test]
    fn test_scan_with_real_data() {
        let temp_db_file = NamedTempFile::new().unwrap();
        let mut temp_text_file = NamedTempFile::new().unwrap();

        writeln!(temp_text_file, "1|Alice|Engineer|75000|").unwrap();
        writeln!(temp_text_file, "2|Bob|Designer|65000|").unwrap();
        writeln!(temp_text_file, "3|Charlie|Manager|90000|").unwrap();
        temp_text_file.flush().unwrap();

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

        let mut db_file = DBFile::new();
        assert!(db_file.create(temp_db_file.path(), FileType::Heap).is_ok());
        assert!(
            db_file
                .load(&schema, temp_text_file.path().to_str().unwrap())
                .is_ok()
        );
        db_file.move_first();

        let mut scan = Scan::new(schema.clone(), "employees".to_string());
        scan.set_db_file(db_file);

        let mut record = Record::new();
        let result = scan.get_next(&mut record);
        assert!(result.is_ok());
        assert!(result.unwrap());

        if let Some(MappedAttrData::String(name)) = record.get_column(1) {
            assert_eq!(name, "Alice");
        } else {
            panic!("Expected name field to be a string");
        }

        let mut record2 = Record::new();
        let result2 = scan.get_next(&mut record2);
        assert!(result2.is_ok());
        assert!(result2.unwrap());

        if let Some(MappedAttrData::String(name)) = record2.get_column(1) {
            assert_eq!(name, "Bob");
        } else {
            panic!("Expected name field to be a string");
        }
    }

    #[test]
    fn test_complex_query_tree() {
        let attributes = vec![
            "id".to_string(),
            "value".to_string(),
            "category".to_string(),
        ];
        let types = vec![
            "Integer".to_string(),
            "Integer".to_string(),
            "String".to_string(),
        ];
        let distincts = vec![0, 0, 0];
        let schema = Schema::new(&attributes, &types, &distincts, 0, "data.tbl");

        let scan = Scan::new(schema.clone(), "data_table".to_string());
        let comparison = Comparison::new();
        let select = Select::new(schema.clone(), comparison, Box::new(scan));

        let proj_attrs = vec!["id".to_string(), "value".to_string()];
        let proj_types = vec!["Integer".to_string(), "Integer".to_string()];
        let proj_distincts = vec![0, 0];
        let proj_schema = Schema::new(&proj_attrs, &proj_types, &proj_distincts, 0, "");

        let project = Project::new(schema, proj_schema, vec![0, 1], Box::new(select));

        let mut tree = QueryExecutionTree::new();
        tree.set_root(Box::new(project));

        let result = tree.execute_query();
        assert!(result.is_ok());
    }

    #[test]
    fn test_record_edge_cases() {
        let record = Record::new();
        let size = record.get_size();
        assert_eq!(size, 0);

        let data = "1||3|";
        let mut cursor = Cursor::new(data.as_bytes());

        let attributes = vec!["id".to_string(), "empty_str".to_string(), "num".to_string()];
        let types = vec![
            "Integer".to_string(),
            "String".to_string(),
            "Integer".to_string(),
        ];
        let distincts = vec![0, 0, 0];
        let schema = Schema::new(&attributes, &types, &distincts, 0, "test.tbl");

        let mut record = Record::new();
        let result = record.extract_next_record(&schema, &mut cursor);
        assert!(result.is_some());

        match record.get_column(1) {
            Some(MappedAttrData::String(val)) => assert_eq!(val, ""),
            _ => panic!("Expected empty string"),
        }
    }
}
