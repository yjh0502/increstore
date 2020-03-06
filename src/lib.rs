#[macro_use]
extern crate log;

use std::io;
use std::path::Path;
use stopwatch::Stopwatch;
use tempfile::*;

pub mod db;
mod hashrw;
pub mod zip;

use crate::zip::store_zip;
use hashrw::*;

pub fn prefix() -> &'static str {
    "data"
}

fn filepath(s: &str) -> String {
    format!("{}/objects/{}/{}", prefix(), &s[..2], &s[2..])
}

async fn store_delta<R, P1, P2>(
    src_reader: R,
    input_path: P1,
    dst_path: P2,
) -> std::io::Result<(WriteMetadata, WriteMetadata)>
where
    R: async_std::io::Read + std::marker::Unpin,
    P1: AsRef<Path>,
    P2: AsRef<Path>,
{
    use async_std::{fs, io};

    let input_file = fs::File::open(input_path.as_ref()).await?;
    let dst_file = fs::File::create(dst_path.as_ref()).await?;

    let mut input_file = HashRW::new(input_file);
    let mut dst_file = HashRW::new(dst_file);

    let cfg = xdelta3::stream::Xd3Config::new()
        .source_window_size(100_000_000)
        .no_compress(true)
        .level(0);
    xdelta3::stream::process_async(
        cfg,
        xdelta3::stream::ProcessMode::Encode,
        io::BufReader::new(&mut input_file),
        src_reader,
        io::BufWriter::new(&mut dst_file),
    )
    .await
    .expect("failed to encode");

    let input_meta = input_file.meta();
    let dst_meta = dst_file.meta();

    Ok((input_meta, dst_meta))
}

fn store_object<P>(src_path: NamedTempFile, dst_path: P) -> std::io::Result<()>
where
    P: AsRef<Path>,
{
    trace!(
        "store_object: src={:?}, dst={:?}",
        src_path.as_ref(),
        dst_path.as_ref()
    );

    if let Some(dir) = Path::new(dst_path.as_ref()).parent() {
        std::fs::create_dir_all(dir)?;
    } else {
        error!("failed to get a parent directory: {:?}", dst_path.as_ref());
    }
    src_path.persist(dst_path)?;
    Ok(())
}

fn update_blob(tmp_path: NamedTempFile, blob: &db::Blob) -> std::io::Result<()> {
    let path = filepath(&blob.store_hash);

    trace!("path={}", path);
    store_object(tmp_path, &path)?;

    db::insert(blob).expect("failed to insert blob");
    Ok(())
}

pub fn push_zip(input_filepath: &str) -> std::io::Result<()> {
    match db::latest() {
        Ok(latest) => {
            append_zip_delta(input_filepath, &latest)?;
        }
        Err(_e) => {
            append_zip_full(input_filepath)?;
        }
    };
    Ok(())
}

pub fn get(filename: &str, _out_filename: &str) -> std::io::Result<()> {
    let mut blobs = db::by_filename(filename).expect("db::by_filename");
    if blobs.is_empty() {
        panic!("unknown filename: {}", filename);
    }

    let mut decode_path = Vec::new();

    let mut blob = blobs.pop().unwrap();
    while let Some(parent_hash) = &blob.parent_hash {
        let mut blobs = db::by_content_hash(parent_hash).expect("db::by_content_hash");
        assert!(!blobs.is_empty());

        let old_blob = std::mem::replace(&mut blob, blobs.pop().unwrap());
        decode_path.push(old_blob);
    }
    decode_path.push(blob);

    decode_path.reverse();
    for blob in decode_path {
        debug!("{}", blob.filename);
    }

    Ok(())
}

fn append_zip_full(input_filepath: &str) -> io::Result<db::Blob> {
    trace!("append_zip_full: input_filepath={}", input_filepath);

    let blob = store_zip_blob(input_filepath)?;
    db::insert(&blob).expect("failed to insert blob");
    Ok(blob)
}

fn cleanup(hash: &str) -> std::io::Result<()> {
    let blobs = db::by_content_hash(hash).expect("db::get");

    let blob_has_backref = blobs.iter().find(|b| b.parent_hash.is_some()).is_some();

    if !blob_has_backref {
        // block does not have backref: root block
        return Ok(());
    }

    for blob in blobs {
        match blob.parent_hash {
            Some(ref parent_hash) => {
                cleanup(parent_hash)?;
            }
            None => {
                // non-root full blob, delete
                db::remove(&blob).expect("db::remove");
                std::fs::remove_file(&filepath(&blob.content_hash))?;
            }
        }
    }

    Ok(())
}

fn store_zip_blob(input_filepath: &str) -> std::io::Result<db::Blob> {
    let input_filename = Path::new(&input_filepath)
        .file_name()
        .unwrap()
        .to_str()
        .unwrap();

    let tmp_dir = format!("{}/tmp", prefix());
    std::fs::create_dir_all(&tmp_dir)?;

    let tmp_unzip_path = NamedTempFile::new_in(&tmp_dir)?;

    let meta = store_zip(input_filepath, tmp_unzip_path.path(), true)?;

    let input_blob = meta.blob(input_filename);
    let store_filepath = filepath(&input_blob.store_hash);
    store_object(tmp_unzip_path, &store_filepath)?;
    Ok(input_blob)
}

fn append_zip_delta(input_filepath: &str, latest: &db::Blob) -> std::io::Result<()> {
    debug!(
        "append_zip_delta: input_filepath={}, latest={}",
        input_filepath, latest.filename
    );

    let sw = Stopwatch::start_new();
    let input_blob = append_zip_full(input_filepath)?;
    let input_filepath = filepath(&input_blob.store_hash);
    let dt_store_zip = sw.elapsed_ms();

    let sw = Stopwatch::start_new();
    let blob = {
        let tmp_dir = format!("{}/tmp", prefix());
        std::fs::create_dir_all(&tmp_dir)?;

        let tmp_path = NamedTempFile::new_in(&tmp_dir)?;

        let src_hash = &latest.content_hash;
        let src_filepath = filepath(src_hash);

        let (_input_meta, dst_meta) = async_std::task::block_on(async {
            let src_file = async_std::fs::File::open(&src_filepath).await?;
            store_delta(src_file, &input_filepath, &tmp_path).await
        })?;

        let mut blob = dst_meta.blob(&input_blob.filename);
        blob.content_size = input_blob.content_size;
        blob.content_hash = input_blob.content_hash.clone();
        blob.parent_hash = Some(src_hash.to_owned());

        trace!(
            "content_hash={}, store_hash={}",
            blob.content_hash,
            blob.store_hash
        );
        update_blob(tmp_path, &blob)?;
        blob
    };
    let dt_store_delta = sw.elapsed_ms();

    info!(
        "append_zip_delta: ratio={:.02}% dt_store_zip={}ms, dt_store_delta={}ms",
        blob.compression_ratio() * 100.0,
        dt_store_zip,
        dt_store_delta,
    );

    if let Some(ref src_hash) = blob.parent_hash {
        cleanup(src_hash)?;
    }

    Ok(())
}

pub fn bench_zip(input_filepath: &str, parallel: bool) -> std::io::Result<()> {
    let tmp_dir = format!("{}/tmp", prefix());
    std::fs::create_dir_all(&tmp_dir)?;

    let tempfile = NamedTempFile::new_in(&tmp_dir)?;

    let ws = Stopwatch::start_new();
    let _meta = store_zip(input_filepath, tempfile.path(), parallel)?;
    info!("store_zip took {}ms", ws.elapsed_ms());
    Ok(())
}
