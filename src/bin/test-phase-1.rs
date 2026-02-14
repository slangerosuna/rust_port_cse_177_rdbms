use rand::prelude::*;
use rust_port::*;

use anyhow::Result;

fn main() -> Result<()> {
    let mut args = std::env::args();
    let executable_path = args.next().unwrap(); // skip the path to the executable

    macro_rules! incorrect_args {
        () => {{
            println!("Usage: {executable_path} [sqlite_file] [no_tables] [no_atts]");
            std::process::exit(-1);
        }};
    }

    let (sqlite_file, no_tables, no_atts) =
        match (args.next(), args.next(), args.next(), args.next()) {
            (Some(sqlite_file), Some(no_tables), Some(no_atts), None) => {
                let Ok(no_tables) = no_tables.parse::<usize>() else {
                    incorrect_args!()
                };
                let Ok(no_atts) = no_atts.parse::<usize>() else {
                    incorrect_args!()
                };

                (sqlite_file, no_tables, no_atts)
            }
            _ => incorrect_args!(),
        };

    let mut catalog = Catalog::open(sqlite_file)?;
    println!("{catalog}");

    for i in 0..no_tables {
        let table_name = format!("T_{i}");

        let table_att_no = (i + 1) * no_atts;
        let mut atts = (0..table_att_no)
            .map(|j| format!("A_{i}_{j}"))
            .collect::<Vec<String>>();

        let mut types = (0..table_att_no)
            .map(|j| match j % 3 {
                0 => "INTEGER",
                1 => "FLOAT",
                2 => "STRING",

                _ => unreachable!(),
            })
            .map(String::from)
            .collect::<Vec<String>>();

        println!("CREATE TABLE {table_name}");
        println!(
            "{}",
            atts.iter()
                .map(Clone::clone)
                .reduce(|acc, att| format!("{acc} {att}"))
                .unwrap()
        );
        println!(
            "{}",
            types
                .iter()
                .map(Clone::clone)
                .reduce(|acc, type_| format!("{acc} {type_}"))
                .unwrap()
        );

        if !catalog.create_table(&table_name, &atts, &types) {
            println!("CREATE TABLE {table_name} FAIL");
        }

        for j in 0..table_att_no {
            let dist = (i + 1) * 10 + j;
            println!("{} distinct = {}", atts[j], dist);
            catalog.set_no_distinct(&table_name, &atts[j], dist as i32);
        }

        let tuples = (i + 1) * 1000;
        println!("tuples = {tuples}");
        catalog.set_no_tuples(&table_name, tuples as i32);

        let path = format!("{table_name}.dat");
        println!("path = {path}");
        catalog.set_data_file(&table_name, &path);

        println!("CREATE TABLE {table_name} OK");
    }

    catalog.save()?;
    println!("{catalog}");

    println!("tables");
    catalog
        .get_tables()
        .iter()
        .for_each(|table| println!("{table}"));
    println!();

    let mut rng = rand::rng();
    for i in 0..10 {
        println!("{i}+++++++++++++");

        let r = rng.random_range(0..no_tables);
        let table_name = format!("T_{r}");

        let tuples = catalog.get_no_tuples(&table_name).unwrap();
        println!("{table_name} tuples = {tuples}");

        let path = format!(".{}", catalog.get_data_file(&table_name).unwrap());
        println!("{table_name} path = {path}");

        let atts = catalog.get_attributes(&table_name).unwrap();
        println!(
            "{}",
            atts.iter()
                .map(Clone::clone)
                .reduce(|acc, att| format!("{acc} {att}"))
                .unwrap()
        );

        let schema = catalog.get_schema(&table_name).unwrap();
        println!("{schema}");

        for _ in 0..2 {
            let s = rng.random_range(0..((r + 1) * no_atts));
            let att_name = format!("A_{r}_{s}");

            let distinct = catalog.get_no_distinct(&table_name, &att_name).unwrap();
            println!("{table_name}.{att_name} distinct = {distinct}");
        }
    }

    for i in 0..(no_tables / 2) {
        let table_name = format!("T_{i}");

        if catalog.drop_table(&table_name) {
            println!("DROP TABLE {table_name} OK");
        } else {
            println!("DROP TABLE {table_name} FAIL");
        }
    }

    Ok(())
}
