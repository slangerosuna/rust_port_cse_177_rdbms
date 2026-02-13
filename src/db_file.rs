use crate::record::*;
use crate::schema::*;
use crate::types::*;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;

use anyhow::{Result, anyhow};

/// Page size constant - 128KB as defined in C++ Config.h
const PAGE_SIZE: usize = 131072;

/// Maximum number of records that can fit in a page (rough estimate)
const MAX_RECORDS_PER_PAGE: usize = 1000;

/// A database page that holds multiple records
#[derive(Debug, Clone)]
pub struct Page {
    records: Vec<Record>,
    num_records: usize,
    current_size_bytes: usize,
}

impl Page {
    pub fn new() -> Self {
        Page {
            records: Vec::new(),
            num_records: 0,
            current_size_bytes: 0,
        }
    }

    pub fn to_binary(&self) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(PAGE_SIZE);

        for record in &self.records {
            let record_string = record.to_bytes();
            let record_string = String::from_utf8_lossy(&record_string);
            buffer.extend_from_slice(record_string.as_bytes());
        }

        // Pad to PAGE_SIZE
        buffer.resize(PAGE_SIZE, 0);
        buffer
    }

    pub fn from_binary(&mut self, bits: &[u8], schema: &Schema) -> Result<()> {
        use std::io::Cursor;

        self.records.clear();
        self.num_records = 0;
        self.current_size_bytes = 0;

        // Convert bytes to string and create a cursor for BufRead
        let data_str = String::from_utf8_lossy(bits);
        let mut cursor = Cursor::new(data_str.as_bytes());

        loop {
            let mut record = Record::new();
            if record.extract_next_record(schema, &mut cursor).is_some() {
                self.current_size_bytes += record.get_size();
                self.records.push(record);
                self.num_records += 1;
            } else {
                break;
            }
        }

        Ok(())
    }

    pub fn get_first(&mut self, record: &mut Record) -> bool {
        if self.records.is_empty() {
            return false;
        }

        *record = self.records.remove(0);
        self.num_records -= 1;
        // Note: current_size_bytes calculation is approximate in this implementation
        true
    }

    pub fn append(&mut self, record: Record) -> bool {
        let record_size = record.get_size();

        if self.current_size_bytes + record_size + 8 > PAGE_SIZE
            || self.records.len() >= MAX_RECORDS_PER_PAGE
        {
            return false;
        }

        self.records.push(record);
        self.num_records += 1;
        self.current_size_bytes += record_size + 8; // +8 for overhead
        true
    }

    pub fn empty_it_out(&mut self) {
        self.records.clear();
        self.num_records = 0;
        self.current_size_bytes = 0;
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn get_num_records(&self) -> usize {
        self.num_records
    }
}

#[derive(Debug)]
pub struct DBFile {
    file: Option<File>,
    file_name: String,
    current_page_pos: u64,
    current_page: Page,
    is_open: bool,
    schema: Option<Schema>,
}

impl DBFile {
    pub fn new() -> Self {
        DBFile {
            file: None,
            file_name: String::new(),
            current_page_pos: 0,
            current_page: Page::new(),
            is_open: false,
            schema: None,
        }
    }

    pub fn create<P: AsRef<Path>>(&mut self, file_path: P, _file_type: FileType) -> Result<()> {
        let path = file_path.as_ref();
        self.file_name = path.to_string_lossy().to_string();

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(true)
            .open(path)
            .map_err(|_| anyhow!(""))?;

        self.file = Some(file);
        self.current_page_pos = 0;
        self.current_page = Page::new();
        self.is_open = true;

        Ok(())
    }

    pub fn open<P: AsRef<Path>>(&mut self, file_path: P) -> Result<()> {
        let path = file_path.as_ref();
        self.file_name = path.to_string_lossy().to_string();

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|_| anyhow!(""))?;

        self.file = Some(file);
        self.current_page_pos = 0;
        self.current_page = Page::new();
        self.is_open = true;

        self.move_first();

        Ok(())
    }

    pub fn close(&mut self) -> Result<()> {
        if self.file.is_some() {
            if !self.current_page.is_empty() {
                self.write_current_page()?;
            }
            self.file.take();
        }
        self.is_open = false;
        Ok(())
    }

    pub fn move_first(&mut self) {
        self.current_page_pos = 0;
        if self.schema.is_some() {
            if let Err(_) = self.load_page(0) {
                self.current_page = Page::new();
            }
        } else {
            self.current_page = Page::new();
        }
    }

    pub fn get_next(&mut self, record: &mut Record) -> Result<bool> {
        if self.current_page.get_first(record) {
            return Ok(true);
        }

        self.current_page_pos += 1;
        if let Ok(()) = self.load_page(self.current_page_pos) {
            if self.current_page.get_first(record) {
                return Ok(true);
            }
        }

        Ok(false)
    }

    pub fn append_record(&mut self, record: Record) -> Result<()> {
        if !self.current_page.append(record.clone()) {
            self.write_current_page()?;
            self.current_page_pos += 1;
            self.current_page = Page::new();

            if !self.current_page.append(record) {
                return Err(anyhow!(""));
            }
        }
        Ok(())
    }

    pub fn load(&mut self, schema: &Schema, text_file_path: &str) -> Result<()> {
        self.schema = Some(schema.clone());

        let file = std::fs::File::open(text_file_path).map_err(|_| anyhow!(""))?;
        let mut reader = BufReader::new(file);

        self.current_page_pos = 0;
        self.current_page = Page::new();

        let mut record = Record::new();
        while record.extract_next_record(schema, &mut reader).is_some() {
            self.append_record(record.clone())?;
            record = Record::new();
        }

        if !self.current_page.is_empty() {
            self.write_current_page()?;
        }

        Ok(())
    }

    fn load_page(&mut self, page_num: u64) -> Result<()> {
        let file = self.file.as_mut().ok_or(anyhow!(""))?;
        let schema = self.schema.as_ref().ok_or(anyhow!(""))?;

        file.seek(SeekFrom::Start(page_num * PAGE_SIZE as u64))
            .map_err(|_| anyhow!(""))?;

        let mut buffer = vec![0u8; PAGE_SIZE];
        let bytes_read = file.read(&mut buffer).map_err(|_| anyhow!(""))?;

        if bytes_read == 0 {
            self.current_page = Page::new();
            return Err(anyhow!(""));
        }

        self.current_page.from_binary(&buffer, schema)?;
        Ok(())
    }

    fn write_current_page(&mut self) -> Result<()> {
        let file = self.file.as_mut().ok_or(anyhow!(""))?;

        file.seek(SeekFrom::Start(self.current_page_pos * PAGE_SIZE as u64))
            .map_err(|_| anyhow!(""))?;

        let page_data = self.current_page.to_binary();
        file.write_all(&page_data).map_err(|_| anyhow!(""))?;
        file.flush().map_err(|_| anyhow!(""))?;

        Ok(())
    }

    pub fn get_file_name(&self) -> &str {
        &self.file_name
    }

    pub fn is_open(&self) -> bool {
        self.is_open
    }

    pub fn get_current_page_pos(&self) -> u64 {
        self.current_page_pos
    }

    pub fn get_current_page_record_count(&self) -> usize {
        self.current_page.get_num_records()
    }

    pub fn set_schema(&mut self, schema: Schema) {
        self.schema = Some(schema);
        if self.is_open {
            self.move_first();
        }
    }
}

impl Default for DBFile {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for DBFile {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn create_test_schema() -> Schema {
        let attributes = vec!["id".to_string(), "name".to_string(), "age".to_string()];
        let types = vec![
            "Integer".to_string(),
            "String".to_string(),
            "Integer".to_string(),
        ];
        let distincts = vec![0, 0, 0];
        Schema::new(&attributes, &types, &distincts, 0, "test.tbl")
    }

    fn create_test_record() -> Record {
        use std::io::Cursor;
        let mut record = Record::new();
        let schema = create_test_schema();
        let data = "42|John Doe|25|";
        let mut cursor = Cursor::new(data.as_bytes());
        record.extract_next_record(&schema, &mut cursor);
        record
    }

    fn create_test_data_file() -> NamedTempFile {
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "1|Alice|30|").unwrap();
        writeln!(temp_file, "2|Bob|25|").unwrap();
        writeln!(temp_file, "3|Charlie|35|").unwrap();
        temp_file.flush().unwrap();
        temp_file
    }

    #[test]
    fn test_page_new() {
        let page = Page::new();
        assert!(page.is_empty());
        assert_eq!(page.get_num_records(), 0);
        assert_eq!(page.current_size_bytes, 0);
        assert_eq!(page.records.len(), 0);
    }

    #[test]
    fn test_page_append_single_record() {
        let mut page = Page::new();
        let record = create_test_record();

        assert!(page.append(record));
        assert!(!page.is_empty());
        assert_eq!(page.get_num_records(), 1);
        assert_eq!(page.records.len(), 1);
    }

    #[test]
    fn test_page_append_multiple_records() {
        let mut page = Page::new();

        for i in 0..10 {
            use std::io::Cursor;
            let mut record = Record::new();
            let schema = create_test_schema();
            let data = format!("{}|Test User {}|{}|", i, i, 20 + i);
            let mut cursor = Cursor::new(data.as_bytes());
            record.extract_next_record(&schema, &mut cursor);
            assert!(page.append(record));
        }

        assert_eq!(page.get_num_records(), 10);
        assert!(!page.is_empty());
    }

    #[test]
    fn test_page_get_first() {
        let mut page = Page::new();
        let original_record = create_test_record();
        page.append(original_record.clone());

        let mut retrieved_record = Record::new();
        assert!(page.get_first(&mut retrieved_record));
        assert!(page.is_empty());
        assert_eq!(page.get_num_records(), 0);
    }

    #[test]
    fn test_page_get_first_empty_page() {
        let mut page = Page::new();
        let mut record = Record::new();

        assert!(!page.get_first(&mut record));
    }

    #[test]
    fn test_page_empty_it_out() {
        let mut page = Page::new();

        for i in 0..5 {
            use std::io::Cursor;
            let mut record = Record::new();
            let schema = create_test_schema();
            let data = format!("{}|User{}|{}|", i, i, 20 + i);
            let mut cursor = Cursor::new(data.as_bytes());
            record.extract_next_record(&schema, &mut cursor);
            page.append(record);
        }

        assert!(!page.is_empty());

        page.empty_it_out();

        assert!(page.is_empty());
        assert_eq!(page.get_num_records(), 0);
        assert_eq!(page.current_size_bytes, 0);
    }

    #[test]
    fn test_page_to_binary() {
        let mut page = Page::new();
        let record = create_test_record();
        page.append(record);

        let binary = page.to_binary();
        assert_eq!(binary.len(), PAGE_SIZE);

        let data_str = String::from_utf8_lossy(&binary);
        assert!(data_str.contains("42|John Doe|25|"));
    }

    #[test]
    fn test_page_from_binary() {
        let schema = create_test_schema();

        let test_data = "1|Alice|30|\n2|Bob|25|\n";
        let mut binary = test_data.as_bytes().to_vec();
        binary.resize(PAGE_SIZE, 0); // Pad to page size

        let mut page = Page::new();
        assert!(page.from_binary(&binary, &schema).is_ok());

        assert!(!page.is_empty());
        assert_eq!(page.get_num_records(), 2);
    }

    #[test]
    fn test_page_serialization_roundtrip() {
        let schema = create_test_schema();
        let mut original_page = Page::new();

        use std::io::Cursor;
        let mut record1 = Record::new();
        let mut cursor1 = Cursor::new("1|Alice|30|".as_bytes());
        record1.extract_next_record(&schema, &mut cursor1);
        original_page.append(record1);

        let mut record2 = Record::new();
        let mut cursor2 = Cursor::new("2|Bob|25|".as_bytes());
        record2.extract_next_record(&schema, &mut cursor2);
        original_page.append(record2);
        assert_eq!(original_page.get_num_records(), 2);

        let binary = original_page.to_binary();

        let string_data = String::from_utf8_lossy(&binary);
        println!("Serialized Page Data:\n{}", string_data);

        let mut restored_page = Page::new();
        assert!(restored_page.from_binary(&binary, &schema).is_ok());

        assert_eq!(restored_page.get_num_records(), 2);
        assert!(!restored_page.is_empty());
    }

    #[test]
    fn test_dbfile_new() {
        let db_file = DBFile::new();
        assert!(!db_file.is_open());
        assert_eq!(db_file.get_file_name(), "");
        assert_eq!(db_file.get_current_page_pos(), 0);
        assert_eq!(db_file.get_current_page_record_count(), 0);
        assert!(db_file.schema.is_none());
    }

    #[test]
    fn test_dbfile_create() {
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path();

        let mut db_file = DBFile::new();
        assert!(db_file.create(&file_path, FileType::Heap).is_ok());

        assert!(db_file.is_open());
        assert_eq!(db_file.get_file_name(), file_path.to_string_lossy());
        assert_eq!(db_file.get_current_page_pos(), 0);
    }

    #[test]
    fn test_dbfile_open_existing_file() {
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path();

        let mut db_file = DBFile::new();
        assert!(db_file.create(&file_path, FileType::Heap).is_ok());
        assert!(db_file.close().is_ok());

        let mut db_file2 = DBFile::new();
        assert!(db_file2.open(&file_path).is_ok());
        assert!(db_file2.is_open());
        assert_eq!(db_file2.get_file_name(), file_path.to_string_lossy());
    }

    #[test]
    fn test_dbfile_close() {
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path();

        let mut db_file = DBFile::new();
        assert!(db_file.create(&file_path, FileType::Heap).is_ok());
        assert!(db_file.is_open());

        assert!(db_file.close().is_ok());
        assert!(!db_file.is_open());
    }

    #[test]
    fn test_dbfile_set_schema() {
        let mut db_file = DBFile::new();
        let schema = create_test_schema();

        assert!(db_file.schema.is_none());

        db_file.set_schema(schema.clone());
        assert!(db_file.schema.is_some());

        if let Some(ref stored_schema) = db_file.schema {
            assert_eq!(stored_schema.get_num_atts(), schema.get_num_atts());
        }
    }

    #[test]
    fn test_dbfile_append_record() {
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path();
        let schema = create_test_schema();

        let mut db_file = DBFile::new();
        assert!(db_file.create(&file_path, FileType::Heap).is_ok());
        db_file.set_schema(schema.clone());

        let mut record = Record::new();
        use std::io::Cursor;
        let mut cursor = Cursor::new("100|Test User|30|".as_bytes());
        record.extract_next_record(&schema, &mut cursor);

        assert!(db_file.append_record(record).is_ok());
        assert_eq!(db_file.get_current_page_record_count(), 1);
    }

    #[test]
    fn test_dbfile_get_next_empty_file() {
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path();
        let schema = create_test_schema();

        let mut db_file = DBFile::new();
        assert!(db_file.create(&file_path, FileType::Heap).is_ok());
        db_file.set_schema(schema);

        let mut record = Record::new();
        let result = db_file.get_next(&mut record);
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[test]
    fn test_dbfile_append_and_get_next() {
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path();
        let schema = create_test_schema();

        let mut db_file = DBFile::new();
        assert!(db_file.create(&file_path, FileType::Heap).is_ok());
        db_file.set_schema(schema.clone());

        use std::io::Cursor;
        let mut original_record = Record::new();
        let mut cursor = Cursor::new("200|Jane Doe|28|".as_bytes());
        original_record.extract_next_record(&schema, &mut cursor);
        assert!(db_file.append_record(original_record).is_ok());

        assert_eq!(db_file.get_current_page_record_count(), 1);

        let mut retrieved_record = Record::new();
        let result = db_file.get_next(&mut retrieved_record);
        assert!(result.is_ok());
        assert!(result.unwrap());

        assert_eq!(db_file.get_current_page_record_count(), 0);
    }

    #[test]
    fn test_dbfile_load_from_text_file() {
        let temp_db_file = NamedTempFile::new().unwrap();
        let db_path = temp_db_file.path();

        let temp_data_file = create_test_data_file();
        let data_path = temp_data_file.path().to_string_lossy();

        let schema = create_test_schema();

        let mut db_file = DBFile::new();

        assert!(db_file.create(&db_path, FileType::Heap).is_ok());
        assert!(db_file.load(&schema, &data_path).is_ok());
        assert!(db_file.get_current_page_record_count() > 0);
    }

    #[test]
    fn test_dbfile_multiple_pages() {
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path();
        let schema = create_test_schema();

        let mut db_file = DBFile::new();
        assert!(db_file.create(&file_path, FileType::Heap).is_ok());
        db_file.set_schema(schema.clone());

        for i in 0..MAX_RECORDS_PER_PAGE + 10 {
            use std::io::Cursor;
            let mut record = Record::new();
            let data = format!("{}|User{}|{}|", i, i, 20 + (i % 50));
            let mut cursor = Cursor::new(data.as_bytes());
            record.extract_next_record(&schema, &mut cursor);
            assert!(db_file.append_record(record).is_ok());
        }

        assert!(db_file.get_current_page_pos() > 0);
    }

    #[test]
    fn test_dbfile_move_first() {
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path();
        let schema = create_test_schema();

        let mut db_file = DBFile::new();
        assert!(db_file.create(&file_path, FileType::Heap).is_ok());
        db_file.set_schema(schema.clone());

        for i in 0..10 {
            use std::io::Cursor;
            let mut record = Record::new();
            let data = format!("{}|User{}|25|", i, i);
            let mut cursor = Cursor::new(data.as_bytes());
            record.extract_next_record(&schema, &mut cursor);
            assert!(db_file.append_record(record).is_ok());
        }

        db_file.move_first();
        assert_eq!(db_file.get_current_page_pos(), 0);
    }

    #[test]
    fn test_page_append_full_page() {
        let mut page = Page::new();
        let mut append_count = 0;

        for i in 0..MAX_RECORDS_PER_PAGE + 100 {
            use std::io::Cursor;
            let mut record = Record::new();
            let schema = create_test_schema();
            let data = format!(
                "{}|Very Long User Name That Takes More Space{}|{}|",
                i, i, 25
            );
            let mut cursor = Cursor::new(data.as_bytes());
            record.extract_next_record(&schema, &mut cursor);

            if page.append(record) {
                append_count += 1;
            } else {
                break;
            }
        }

        assert!(append_count < MAX_RECORDS_PER_PAGE + 100);
        assert!(append_count <= MAX_RECORDS_PER_PAGE);
    }

    #[test]
    fn test_dbfile_drop_cleanup() {
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path();

        {
            let mut db_file = DBFile::new();
            assert!(db_file.create(&file_path, FileType::Heap).is_ok());
            assert!(db_file.is_open());
        }

        assert!(file_path.exists());
    }
}
