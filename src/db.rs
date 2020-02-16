use rusqlite::{params, Connection, Result};

pub struct Blob {
    id: u32,
    filename: String,
    time_created: time::Timespec,
    size: u64,
    hash: String,
    store_hash: String,
    parent_hash: Option<String>,
}

fn dbpath() -> &'static str {
    "data/meta.db"
}

pub fn prepare() -> Result<()> {
    let conn = Connection::open(dbpath())?;

    conn.execute(
        r#"
create table if not exists blobs (
    id              integer primary key,
    filename        text not null,
    time_created    text not null,
    size            integer not null, 
    hash            text not null,
    store_hash      text not null,
    parent_hash     text,

    foreign key (parent_hash) references blobs (hash)
)
    "#,
        params![],
    )?;

    Ok(())
}

pub fn insert(blob: Blob) -> Result<()> {
    let conn = Connection::open(dbpath())?;

    conn.execute(
        r#"
inesrt into blobs (filename, time_created, size, hash, store_hash, parent_hash)
    values (?1, ?2, ?3, ?4, ?5)"#,
        params![
            blob.filename,
            blob.time_created,
            blob.size as i64,
            blob.hash,
            blob.store_hash,
            blob.parent_hash
        ],
    )?;

    Ok(())
}
