use rusqlite::{params, Connection, Result};

#[derive(Debug)]
pub struct Blob {
    pub id: u32,
    pub filename: String,
    pub time_created: time::Timespec,

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

pub fn get(content_hash: &str) -> Result<Blob> {
    let conn = Connection::open(dbpath())?;

    conn.query_row(
        r#"
select
    id, filename, time_created,
    store_size, content_size, store_hash, content_hash, parent_hash
from blobs
where content_hash = ?
"#,
        params![content_hash],
        decode_row,
    )
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

pub fn latest() -> Result<Blob> {
    let conn = Connection::open(dbpath())?;

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

pub fn insert(blob: &Blob) -> Result<()> {
    let conn = Connection::open(dbpath())?;

    conn.execute(
        r#"
insert into blobs (
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

    Ok(())
}
