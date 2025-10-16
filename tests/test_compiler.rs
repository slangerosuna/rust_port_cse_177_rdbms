use rust_port::*;

#[test]
fn test_query_compiler() {
    println!("Testing Query Compiler");

    let catalog = Catalog::new("test.db".to_string());

    let sql = "SELECT name FROM customer WHERE age > 25";

    match compile_query(sql, &catalog) {
        Ok(query_tree) => {
            println!("Successfully compiled query: {}", sql);
            println!("Query tree: {}", query_tree);
        }
        Err(e) => {
            panic!("Failed to compile query: {}", e);
        }
    }

    let sql2 = "SELECT SUM(amount) FROM orders WHERE total > 100 GROUP BY customer_id";

    match compile_query(sql2, &catalog) {
        Ok(query_tree) => {
            println!("Successfully compiled query: {}", sql2);
            println!("Query tree: {}", query_tree);
        }
        Err(e) => {
            panic!("Failed to compile query: {}", e);
        }
    }
}
