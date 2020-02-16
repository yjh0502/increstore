use rusqlite::{params, Connection, Result};

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
