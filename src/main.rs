extern crate git2;
extern crate rusqlite;

use git2::{Commit, Oid, Repository};
use rusqlite::{params, Connection, Result};
use std::env;
use std::fs;
use std::path::Path;

struct CommitDetails {
    id: String,
    author: String,
    date: i64,
    message: String,
    parents: Vec<Oid>,
}

fn main() {
    let args: Vec<String> = env::args().collect();

    let repository_path = args.get(1).map_or(".", |s| s.as_str());
    let db_path = args.get(2).map_or("git_info_llama.db", |s| s.as_str());

    let db_exists = fs::metadata(db_path).is_ok();
    let mut conn = Connection::open(db_path).expect("Failed to open database");

    // Check if the database file exists
    if !db_exists {
        // Call the create_database function to initialize your database tables.
        match create_database(&conn) {
            Ok(_) => println!("Database and tables created successfully!"),
            Err(e) => eprintln!("Error: {}", e),
        }
    }

    let path = to_absolute_path(repository_path).expect("Failed to get absolute path.");
    let repo = Repository::open(&path).expect("Failed to open the repository.");

    println!("Getting Commit Details...");
    get_commits_detail_array(&mut conn, &repo);
    println!("Done!");
}

fn to_absolute_path<P: AsRef<Path>>(path: P) -> std::io::Result<std::path::PathBuf> {
    let path = path.as_ref();

    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(env::current_dir()?.join(path))
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

fn get_commits_detail_array(conn: &mut Connection, repo: &Repository) {
    let mut revwalk = repo.revwalk().expect("Failed to get revwalk.");
    revwalk.push_head().expect("Failed to push head.");

    let all_commits: Vec<_> = revwalk.collect();

    for chunk in all_commits.chunks(50) {
        let mut chunk_commits = Vec::new();

        for oid in chunk {
            match oid {
                Ok(oid) => {
                    let commit = repo.find_commit(*oid).expect("Failed to find commit.");
                    let formatted_commit = extract_commit_details(&commit);

                    chunk_commits.push(formatted_commit);
                }
                Err(e) => println!("Failed to process commit: {}", e),
            }
        }
        batch_insert_commits(conn, &chunk_commits).expect("Failed to insert commits.");
    }
}

fn extract_commit_details(commit: &Commit) -> CommitDetails {
    let id = commit.id().to_string();
    let author = commit.author().name().unwrap_or("Unknown").to_string();
    let date = commit.time().seconds();
    let message = commit.message().unwrap_or("No message").to_string();
    let parents = commit.parent_ids().collect::<Vec<_>>();

    return CommitDetails {
        id,
        author,
        date,
        message,
        parents,
    };
}

fn batch_insert_commits(conn: &mut Connection, commits: &Vec<CommitDetails>) -> Result<()> {
    let insert_sql =
        "INSERT INTO commit_details (id, author, date, message) VALUES (?1, ?2, ?3, ?4)";

    for commit in commits {
        let tx = conn.transaction()?; // Begin a new transaction

        tx.execute(
            insert_sql,
            params![&commit.id, &commit.author, commit.date, &commit.message],
        )?;

        for parent in &commit.parents {
            tx.execute(
                "INSERT INTO commit_relation (parent, child) VALUES (?1, ?2)",
                params![parent.to_string(), commit.id],
            )
            .expect("Failed to insert commit relation.");
        }
        tx.commit()?; // Commit the transaction
    }

    Ok(())
}
