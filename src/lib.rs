#[macro_use]
extern crate log;

use pbr::ProgressBar;
use std::io;
use std::path::Path;
use stopwatch::Stopwatch;

pub mod db;
pub mod hashrw;

use hashrw::*;

fn zip_to_tar<R: io::Read + io::Seek, W: io::Write>(src: R, dst: W) -> io::Result<()> {
    let mut zip = zip::ZipArchive::new(src)?;
    let mut ar = tar::Builder::new(dst);

    let mut pb = ProgressBar::new(zip.len() as u64);

    for i in 0..zip.len() {
        let mut file = zip.by_index(i)?;
        let filename = file.name().to_owned();

        let mut header = tar::Header::new_gnu();
        header.set_path(&filename)?;
        header.set_size(file.size());

        if let Some(mode) = file.unix_mode() {
            header.set_mode(mode);
        } else {
            if file.is_dir() {
                header.set_mode(0o755);
            } else {
                header.set_mode(0o644);
            }
        }

        let unixtime = file.last_modified().to_time().to_timespec().sec;
        header.set_mtime(unixtime as u64);

        header.set_cksum();

        ar.append(&mut header, &mut file)?;
        pb.inc();
    }
    pb.finish();

    Ok(())
}

pub fn prefix() -> &'static str {
    "data"
}

fn filepath(s: &str) -> String {
    format!("{}/objects/{}/{}", prefix(), &s[..2], &s[2..])
}

fn store_zip(input_path: &str, dst_path: &str) -> std::io::Result<WriteMetadata> {
    let mut input_file = std::fs::File::open(input_path)?;
    let dst_file = std::fs::File::create(&dst_path)?;

    let mut dst_file = HashRW::new(dst_file);

    trace!("zip_to_tar: src={}, dst={}", &input_path, &dst_path);
    zip_to_tar(&mut input_file, io::BufWriter::new(&mut dst_file))?;

    Ok(dst_file.meta())
}

async fn store_delta<R: async_std::io::Read + std::marker::Unpin>(
    src_reader: R,
    input_path: &str,
    dst_path: &str,
) -> std::io::Result<(WriteMetadata, WriteMetadata)> {
    use async_std::{fs, io};

    let input_file = fs::File::open(input_path).await?;
    let dst_file = fs::File::create(dst_path).await?;

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

fn store_object(src_path: &str, dst_path: &str) -> std::io::Result<()> {
    trace!("store_object: src={}, dst={}", &src_path, &dst_path);

    if let Some(dir) = Path::new(&dst_path).parent() {
        std::fs::create_dir_all(dir)?;
    } else {
        error!("failed to get a parent directory: {}", dst_path);
    }
    std::fs::rename(src_path, dst_path)
}

fn update_blob(tmp_path: &str, blob: &db::Blob) -> std::io::Result<()> {
    let path = filepath(&blob.store_hash);

    trace!("path={}", path);
    store_object(tmp_path, &path)?;

    db::insert(blob).expect("failed to insert blob");
    Ok(())
}

pub fn push_zip(input_filepath: &str) -> std::io::Result<()> {
    match db::latest() {
        Ok(latest) => append_zip_delta(input_filepath, &latest),
        Err(_e) => append_zip_full(input_filepath),
    }
}

fn append_zip_full(input_filepath: &str) -> io::Result<()> {
    trace!("append_zip_full: input_filepath={}", input_filepath);

    let tmp_path = format!("{}/tmp", prefix());
    let input_filename = Path::new(&input_filepath)
        .file_name()
        .unwrap()
        .to_str()
        .unwrap();

    let meta = store_zip(input_filepath, &tmp_path)?;
    trace!("hash={}", meta.digest());

    let blob = meta.blob(input_filename);
    update_blob(&tmp_path, &blob)?;
    Ok(())
}

fn append_zip_delta(input_filepath: &str, latest: &db::Blob) -> std::io::Result<()> {
    debug!(
        "append_zip_delta: input_filepath={}, latest={}",
        input_filepath, latest.filename
    );

    let src_hash = &latest.content_hash;
    let src_filepath = filepath(src_hash);

    let tmp_unzip_path = format!("{}/tmp_unzip", prefix());
    let tmp_path = format!("{}/tmp", prefix());

    let input_filename = Path::new(&input_filepath)
        .file_name()
        .unwrap()
        .to_str()
        .unwrap();

    let sw = Stopwatch::start_new();
    let meta = store_zip(input_filepath, &tmp_unzip_path)?;
    let dt_store_zip = sw.elapsed_ms();

    let input_blob = meta.blob(input_filename);

    let sw = Stopwatch::start_new();
    let (_input_meta, dst_meta) = async_std::task::block_on(async {
        let src_file = async_std::fs::File::open(&src_filepath).await?;
        store_delta(src_file, &tmp_unzip_path, &tmp_path).await
    })?;
    let dt_store_delta = sw.elapsed_ms();

    let store_filename = filepath(&meta.blob(input_filename).store_hash);
    store_object(&tmp_unzip_path, &store_filename)?;

    let mut blob = dst_meta.blob(input_filename);
    blob.content_size = input_blob.content_size;
    blob.content_hash = input_blob.content_hash.clone();
    blob.parent_hash = Some(src_hash.to_owned());

    trace!(
        "content_hash={}, store_hash={}",
        blob.content_hash,
        blob.store_hash
    );
    update_blob(&tmp_path, &blob)?;

    // remove old object
    // TODO: smarter...
    std::fs::remove_file(&filepath(&latest.content_hash))?;

    info!(
        "append_zip_delta: ratio={:.02}% dt_store_zip={}ms, dt_store_delta={}ms",
        blob.compression_ratio() * 100.0,
        dt_store_zip,
        dt_store_delta,
    );

    Ok(())
}

pub fn main() -> io::Result<()> {
    env_logger::init();
    db::prepare().expect("failed to prepare");

    {
        let tmp_path = format!("{}/tmp", prefix());
        let input_filepath = &format!("{}/test.apk", prefix());
        append_zip_full(&input_filepath)?;

        let input_filename = Path::new(&input_filepath)
            .file_name()
            .unwrap()
            .to_str()
            .unwrap();

        let meta = store_zip(input_filepath, &tmp_path)?;
        trace!("hash={}", meta.digest());

        let blob = meta.blob(input_filename);
        update_blob(&tmp_path, &blob)?;
        blob
    };

    if true {
        let input_filepath = &format!("{}/test.apk", prefix());
        push_zip(&input_filepath)?;
    }

    Ok(())
}
