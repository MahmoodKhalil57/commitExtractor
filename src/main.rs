extern crate git2;
extern crate rusqlite;

use rusqlite::Connection;
use std::env;
use std::fs;

fn main() {
    let args: Vec<String> = env::args().collect();

    let db_path = args.get(2).map_or("git_info_llama.db", |s| s.as_str());

    let db_exists = fs::metadata(db_path).is_ok();
    let conn = Connection::open(db_path).expect("Failed to open database");

    // Check if the database file exists
    if !db_exists {
        // Call the create_database function to initialize your database tables.
        match create_database(&conn) {
            Ok(_) => println!("Database and tables created successfully!"),
            Err(e) => eprintln!("Error: {}", e),
        }
    }
}

fn create_database(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute(
        "CREATE TABLE commit_details (
            id TEXT PRIMARY KEY,
            author TEXT NOT NULL,
            date INTEGER NOT NULL,
            message TEXT NOT NULL
        )",
        {},
    )?;

    conn.execute(
        "CREATE TABLE commit_relation (
            parent TEXT NOT NULL,
            child TEXT NOT NULL,
            PRIMARY KEY (parent, child)
        )",
        {},
    )?;

    conn.execute(
        "CREATE TABLE ref_details (
            name TEXT NOT NULL,
            id TEXT NOT NULL,
            kind TEXT NOT NULL,
            PRIMARY KEY (name, id)
        )",
        {},
    )?;

    Ok(())
}
