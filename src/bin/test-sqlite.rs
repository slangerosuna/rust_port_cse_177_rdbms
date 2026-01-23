use rusqlite::{Connection, Result};

fn main() -> Result<()> {
    let mut args = std::env::args();
    let mut conn = match (args.nth(1), args.next()) {
        (Some(db_file), None) => {
            println!("++++++++++++++++++++++++++++++++++");
            println!("Open database {db_file}");

            let conn = Connection::open(&db_file)?;

            println!("success");
            println!("++++++++++++++++++++++++++++++++++");

            conn
        }
        (None, None) => {
            // I added the option to open an in-memory database because I kept on forgetting to
            // write `cargo r --bin test-sqlite test.sqlite` and decided that this was easier than
            // just remembering to do that.

            println!("++++++++++++++++++++++++++++++++++");
            println!("Open in-memory database");

            let conn = Connection::open_in_memory()?;

            println!("success");
            println!("++++++++++++++++++++++++++++++++++");

            conn
        }
        _ => {
            eprintln!("Usage: main [sqlite_file]");
            std::process::exit(-1);
        }
    };

    drop_tables(&mut conn)?;
    create_tables(&mut conn)?;
    populate_tables(&mut conn)?;

    let (maker, product) = ("A", "Laptop");

    pcs_by_maker(&mut conn, maker)?;
    product_by_maker(&mut conn, product, maker)?;
    all_products_by_maker(&mut conn, maker)?;

    conn.close().map_err(|(_, err)| err)?;

    Ok(())
}

fn drop_tables(conn: &mut Connection) -> Result<()> {
    println!("++++++++++++++++++++++++++++++++++");
    println!("Drop Tables");

    conn.execute_batch(
        "
        DROP TABLE IF EXISTS Product;
        DROP TABLE IF EXISTS PC;
        DROP TABLE IF EXISTS Laptop;
        DROP TABLE IF EXISTS Printer;
        ",
    )?;

    println!("success");
    println!("++++++++++++++++++++++++++++++++++");

    Ok(())
}

fn create_tables(conn: &mut Connection) -> Result<()> {
    println!("++++++++++++++++++++++++++++++++++");
    println!("Create Tables");

    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS Product (
            maker Char(32),
            model INTEGER PRIMARY KEY,
            type VARCHAR(20) NOT NULL
        );

        CREATE TABLE IF NOT EXISTS PC (
            model INTEGER PRIMARY KEY,
            speed FLOAT,
            ram INTEGER,
            hd INTEGER,
            price DECIMAL(7, 2) NOT NULL
        );

        CREATE TABLE IF NOT EXISTS Laptop (
            model INTEGER PRIMARY KEY,
            speed FLOAT,
            ram INTEGER,
            hd INTEGER,
            screen DECIMAL(4, 1),
            price DECIMAL(7, 2) NOT NULL
        );

        CREATE TABLE Printer (
            model INTEGER PRIMARY KEY,
            color BOOL,
            type VARCHAR(20),
            price demimal(7, 2) NOT NULL
        );
        ",
    )?;

    println!("success");
    println!("++++++++++++++++++++++++++++++++++");

    Ok(())
}

fn populate_tables(conn: &mut Connection) -> Result<()> {
    populate_product(conn)?;
    populate_pc(conn)?;
    populate_laptop(conn)?;
    populate_printer(conn)?;

    return Ok(());

    fn populate_product(conn: &mut Connection) -> Result<()> {
        println!("++++++++++++++++++++++++++++++++++");
        println!("Populate Product");

        conn.execute(
            "
            INSERT INTO Product (maker, model, type) VALUES
                ('A', 1001, 'pc'),
                ('A', 1002, 'pc'),
                ('A', 1003, 'pc'),
                ('A', 2004, 'laptop'),
                ('A', 2005, 'laptop'),
                ('A', 2006, 'laptop'),
                ('B', 1004, 'pc'),
                ('B', 1005, 'pc'),
                ('B', 1006, 'pc'),
                ('B', 2007, 'laptop'),
                ('C', 1007, 'pc'),
                ('D', 1008, 'pc'),
                ('D', 1009, 'pc'),
                ('D', 1010, 'pc'),
                ('D', 3004, 'printer'),
                ('D', 3005, 'printer'),
                ('E', 1011, 'pc'),
                ('E', 1012, 'pc'),
                ('E', 1013, 'pc'),
                ('E', 2001, 'laptop'),
                ('E', 2002, 'laptop'),
                ('E', 2003, 'laptop'),
                ('E', 3001, 'printer'),
                ('E', 3002, 'printer'),
                ('E', 3003, 'printer'),
                ('F', 2008, 'laptop'),
                ('F', 2009, 'laptop'),
                ('G', 2010, 'laptop'),
                ('H', 3006, 'printer'),
                ('H', 3007, 'printer');
            ",
            (),
        )?;

        println!("success");
        println!("++++++++++++++++++++++++++++++++++");

        Ok(())
    }

    fn populate_pc(conn: &mut Connection) -> Result<()> {
        println!("++++++++++++++++++++++++++++++++++");
        println!("Populate PC");

        conn.execute(
            "
            INSERT INTO PC(model, speed, ram, hd, price) VALUES
                (1001, 2.66, 1024, 250, 2114),
                (1002, 2.10, 512, 250, 995),
                (1003, 1.42, 512, 80, 478),
                (1004, 2.80, 1024, 250, 649),
                (1005, 3.20, 512, 250, 630),
                (1006, 3.20, 1024, 320, 1049),
                (1007, 2.20, 1024, 200, 510),
                (1008, 2.20, 2048, 250, 770),
                (1009, 2.00, 1024, 250, 650),
                (1010, 2.80, 2048, 300, 770),
                (1011, 1.86, 2048, 160, 959),
                (1012, 2.80, 1024, 160, 649),
                (1013, 3.06, 512, 80, 529);
            ",
            (),
        )?;

        println!("success");
        println!("++++++++++++++++++++++++++++++++++");

        Ok(())
    }

    fn populate_laptop(conn: &mut Connection) -> Result<()> {
        println!("++++++++++++++++++++++++++++++++++");
        println!("Populate Laptop");

        conn.execute(
            "
            INSERT INTO Laptop(model, speed, ram, hd, screen, price) VALUES
                (2001, 2.00, 2048, 240, 20.1, 3673),
                (2002, 1.73, 1024, 80, 17.0, 949),
                (2003, 1.80, 512, 60, 15.4, 549),
                (2004, 2.00, 512, 60, 13.3, 1150),
                (2005, 2.16, 1024, 120, 17.0, 2500),
                (2006, 2.00, 2048, 80, 15.4, 1700),
                (2007, 1.83, 1024, 120, 13.3, 1429),
                (2008, 1.60, 1024, 100, 15.4, 900),
                (2009, 1.60, 512, 80, 14.1, 680),
                (2010, 2.00, 2048, 160, 15.4, 2300);
            ",
            (),
        )?;

        println!("success");
        println!("++++++++++++++++++++++++++++++++++");

        Ok(())
    }

    fn populate_printer(conn: &mut Connection) -> Result<()> {
        println!("++++++++++++++++++++++++++++++++++");
        println!("Populate Printer");

        conn.execute(
            "
            INSERT INTO Printer(model, color, type, price) VALUES
                (3001, 1, 'laser', 99),
                (3002, 0, 'laser', 239),
                (3003, 1, 'ink-jet', 899),
                (3004, 1, 'laser', 120),
                (3005, 0, 'laser', 120),
                (3006, 1, 'ink-jet', 100),
                (3007, 1, 'laser', 200);
            ",
            (),
        )?;

        println!("success");
        println!("++++++++++++++++++++++++++++++++++");

        Ok(())
    }
}

fn pcs_by_maker(conn: &mut Connection, maker: &str) -> Result<()> {
    println!("++++++++++++++++++++++++++++++++++");
    println!("PCs by Maker");

    let mut stmt = conn.prepare(
        "
        SELECT P.model as model, PC.price as price
        FROM Product P, PC
        where P.model = PC.model AND maker = ?
        ",
    )?;

    let mut rows = stmt.query((maker,))?;

    println!("{:>10}{:>10}", "model", "price");
    println!("-------------------------------");

    while let Some(row) = rows.next()? {
        let model: i32 = row.get("model")?;
        let price: f64 = row.get("price")?;

        println!("{:>10}{:>10}", model, price);
    }

    println!("-------------------------------");

    println!("++++++++++++++++++++++++++++++++++");

    Ok(())
}

fn product_by_maker(conn: &mut Connection, product: &str, maker: &str) -> Result<()> {
    println!("++++++++++++++++++++++++++++++++++");
    println!("Product by Maker");

    let mut stmt = conn.prepare(&format!(
        "
        SELECT P.model as model, {product}.price as price
        FROM Product P, {product}
        where P.model = {product}.model AND maker = ?
        "
    ))?;

    let mut rows = stmt.query((maker,))?;

    println!("{:>10}{:>10}", "model", "price");
    println!("-------------------------------");

    while let Some(row) = rows.next()? {
        let model: i32 = row.get("model")?;
        let price: f64 = row.get("price")?;

        println!("{:>10}{:>10}", model, price);
    }

    println!("-------------------------------");

    println!("success");
    println!("++++++++++++++++++++++++++++++++++");

    Ok(())
}

fn all_products_by_maker(conn: &mut Connection, maker: &str) -> Result<()> {
    println!("++++++++++++++++++++++++++++++++++");
    println!("All Products by Maker");

    let mut stmt = conn.prepare(
        "
        SELECT P.model as model, P.type as type, PC.price as price
        FROM Product P, PC
        where P.model = PC.model AND maker = ?1

        UNION

        SELECT P.model as model, P.type as type, L.price as price
        FROM Product P, Laptop L
        where P.model = L.model AND maker = ?1

        UNION

        SELECT P.model as model, P.type as type, Pr.price as price
        FROM Product P, Printer Pr
        where P.model = Pr.model AND maker = ?1
        ",
    )?;

    let mut rows = stmt.query((maker,))?;

    println!("{:>10}{:>20}{:>10}", "model", "type", "price");
    println!("-------------------------------");

    while let Some(row) = rows.next()? {
        let model: i32 = row.get("model")?;
        let type_: String = row.get("type")?;
        let price: f64 = row.get("price")?;

        println!("{:>10}{:>20}{:>10}", model, type_, price);
    }

    println!("-------------------------------");

    println!("++++++++++++++++++++++++++++++++++");

    Ok(())
}
