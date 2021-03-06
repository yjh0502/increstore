use crate::prefix;
use log::info;
use rusqlite::{params, Connection, Result};

#[derive(Debug, Clone)]
pub struct Blob {
    pub id: u32,
    pub filename: String,
    pub time_created: time::OffsetDateTime,

    pub store_size: u64,
    pub content_size: u64,

    pub store_hash: String,
    pub content_hash: String,
    pub parent_hash: Option<String>,
}

impl Blob {
    pub fn compression_ratio(&self) -> f32 {
        self.store_size as f32 / self.content_size as f32
    }
    pub fn is_root(&self) -> bool {
        self.parent_hash.is_none()
    }
    pub fn is_genesis(&self) -> bool {
        // sqlite ROWID starts from 1
        self.id == 1
    }
}

pub fn dbpath() -> String {
    format!("{}/meta.db", prefix())
}

pub type Conn = rusqlite::Connection;

pub fn open() -> Result<rusqlite::Connection> {
    let mut wait_count = 0;
    loop {
        match Connection::open(dbpath()) {
            Ok(conn) => return Ok(conn),
            Err(e) => match e {
                rusqlite::Error::SqliteFailure(ref e2, ref _msg) => {
                    if e2.code == rusqlite::ErrorCode::DatabaseLocked {
                        if wait_count > 5 {
                            return Err(e);
                        }
                        info!("database is locked, waiting");
                        wait_count += 1;
                        std::thread::sleep(std::time::Duration::from_millis(1000));
                    }
                }
                e => {
                    return Err(e);
                }
            },
        }
    }
}

pub fn prepare(conn: &mut Conn) -> Result<()> {
    conn.execute(
        r#"
create table if not exists blobs (
    id              integer primary key,

    filename        text not null,
    time_created    text not null,

    store_size      integer not null,
    content_size    integer not null,

    store_hash      text not null unique,
    content_hash    text not null,
    parent_hash     text,

    foreign key (parent_hash) references blobs (hash)

)
    "#,
        params![],
    )?;

    Ok(())
}

pub fn all(conn: &mut Conn) -> Result<Vec<Blob>> {
    let mut stmt = conn.prepare(
        r#"
select
    id, filename, time_created,
    store_size, content_size, store_hash, content_hash, parent_hash
from blobs
"#,
    )?;

    let mut rows = Vec::new();
    for row_res in stmt.query_map(params![], decode_row)? {
        rows.push(row_res?);
    }
    Ok(rows)
}

pub fn by_filename(conn: &mut Conn, filename: &str) -> Result<Vec<Blob>> {
    let mut stmt = conn.prepare(
        r#"
select
    id, filename, time_created,
    store_size, content_size, store_hash, content_hash, parent_hash
from blobs
where filename = ?
"#,
    )?;

    let mut rows = Vec::new();
    for row_res in stmt.query_map(params![filename], decode_row)? {
        rows.push(row_res?);
    }
    Ok(rows)
}

pub fn by_content_hash(conn: &mut Conn, content_hash: &str) -> Result<Vec<Blob>> {
    let mut stmt = conn.prepare(
        r#"
select
    id, filename, time_created,
    store_size, content_size, store_hash, content_hash, parent_hash
from blobs
where content_hash = ?
"#,
    )?;

    let mut rows = Vec::new();
    for row_res in stmt.query_map(params![content_hash], decode_row)? {
        rows.push(row_res?);
    }
    Ok(rows)
}

fn decode_row(row: &rusqlite::Row) -> Result<Blob> {
    let store_size: i64 = row.get(3)?;
    let content_size: i64 = row.get(4)?;
    Ok(Blob {
        id: row.get(0)?,
        filename: row.get(1)?,
        time_created: row.get(2)?,
        store_size: store_size as u64,
        content_size: content_size as u64,
        store_hash: row.get(5)?,
        content_hash: row.get(6)?,

        parent_hash: row.get(7)?,
    })
}

pub fn latest(conn: &mut Conn) -> Result<Blob> {
    conn.query_row(
        r#"
select
    id, filename, time_created,
    store_size, content_size, store_hash, content_hash, parent_hash
from blobs
order by id desc
limit 1"#,
        params![],
        decode_row,
    )
}

pub fn insert(conn: &mut Conn, blob: &Blob) -> Result<bool> {
    let inserted = conn.execute(
        r#"
insert or ignore into blobs (
    filename,
    time_created,
    store_size,
    content_size,
    store_hash,
    content_hash,
    parent_hash
)
    values (?1, ?2, ?3, ?4, ?5, ?6, ?7)"#,
        params![
            blob.filename,
            blob.time_created,
            blob.store_size as i64,
            blob.content_size as i64,
            blob.store_hash,
            blob.content_hash,
            blob.parent_hash
        ],
    )?;

    Ok(inserted > 0)
}

pub fn rename(conn: &mut Conn, from_filename: &str, to_filename: &str) -> Result<bool> {
    let updated = conn.execute(
        r#"
    update blobs set filename = ?2 where filename = ?1
    "#,
        params![from_filename, to_filename],
    )?;
    Ok(updated > 0)
}

pub fn remove(conn: &mut Conn, blob: &Blob) -> Result<()> {
    conn.execute(
        r#"
delete from blobs where store_hash = ?1
"#,
        params![blob.store_hash],
    )?;
    Ok(())
}

pub fn roots(conn: &mut Conn) -> Result<Vec<Blob>> {
    let mut stmt = conn.prepare(
        r#"
select
    id, filename, time_created,
    store_size, content_size, store_hash, content_hash, parent_hash
from blobs
where parent_hash is null
"#,
    )?;

    let mut rows = Vec::new();
    for row_res in stmt.query_map(params![], decode_row)? {
        rows.push(row_res?);
    }
    Ok(rows)
}
