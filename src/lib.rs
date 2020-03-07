#[macro_use]
extern crate log;

use rayon::prelude::*;
use std::io;
use std::path::*;
use stopwatch::Stopwatch;
use tempfile::*;

pub mod db;
mod delta;
mod rw;
mod stats;
pub mod zip;

use crate::zip::store_zip;
use db::Blob;
use rw::*;
use stats::Stats;
use std::env;

pub fn max_root_blobs() -> usize {
    5
}

pub fn prefix() -> String {
    env::var("WORKDIR").unwrap_or("data".to_owned())
}

pub fn tmpdir() -> String {
    let tmp_dir = format!("{}/tmp", prefix());
    //TODO
    std::fs::create_dir_all(&tmp_dir).ok();
    tmp_dir
}

fn filepath(s: &str) -> PathBuf {
    format!("{}/objects/{}/{}", prefix(), &s[..2], &s[2..]).into()
}

fn store_object<P>(src_path: NamedTempFile, dst_path: P) -> io::Result<()>
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

fn update_blob(tmp_path: NamedTempFile, blob: &Blob) -> io::Result<()> {
    let path = filepath(&blob.store_hash);

    trace!("path={:?}", path);
    store_object(tmp_path, &path)?;

    // TODO: update id
    db::insert(blob).expect("failed to insert blob");
    Ok(())
}

pub fn push_zip(input_filepath: &str) -> io::Result<()> {
    append_zip(input_filepath)?;
    Ok(())
}

pub fn get(filename: &str, out_filename: &str) -> io::Result<()> {
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

    decode_path.reverse();

    assert!(blob.parent_hash.is_none());

    let tmp_dir = tmpdir();
    let mut old_tmpfile = NamedTempFile::new_in(&tmp_dir)?;
    let mut tmpfile = NamedTempFile::new_in(&tmp_dir)?;

    let mut src_filepath = filepath(&blob.content_hash);
    for delta_blob in decode_path {
        let delta_filepath = filepath(&delta_blob.store_hash);
        debug!("decode filename={}", delta_blob.filename);
        debug!("trace={:?}, input={:?}", src_filepath, delta_filepath);
        let (_input_meta, _dst_meta) = async_std::task::block_on(async {
            let src_file = async_std::fs::File::open(&src_filepath).await?;
            let input_file = async_std::fs::File::open(&delta_filepath).await?;
            let dst_file = async_std::fs::File::create(tmpfile.path()).await?;
            delta::delta(
                delta::ProcessMode::Decode,
                &src_file,
                &input_file,
                &dst_file,
            )
            .await
        })?;

        assert_eq!(delta_blob.content_hash, _dst_meta.digest());
        std::mem::swap(&mut tmpfile, &mut old_tmpfile);
        src_filepath = old_tmpfile.path().to_path_buf();
    }

    // result: tmpfile
    tmpfile.persist(out_filename)?;

    Ok(())
}

struct RootBlob<'a> {
    blob: &'a Blob,
    alias: &'a Blob,
    score: u64,
}

pub fn cleanup() -> io::Result<()> {
    let blobs = db::all().expect("db::all");
    let stats = Stats::from_blobs(blobs);

    let mut root_candidates = Vec::new();
    for (root_idx, root_blob) in stats.blobs.iter().enumerate() {
        if root_blob.parent_hash.is_some() {
            continue;
        }

        let mut aliases = stats.aliases(root_idx);
        if let Some(alias) = aliases.pop() {
            let score = stats.root_score(root_idx);
            root_candidates.push(RootBlob {
                blob: root_blob,
                alias,
                score,
            });
        }
    }

    root_candidates.sort_by_key(|blob| {
        // sort by score desc
        u64::max_value() - blob.score
    });

    {
        let mut s = String::new();
        for root_blob in &root_candidates {
            let alias = root_blob.alias;
            s += &format!(
                "{}={:.02}%,{} ",
                alias.id,
                alias.compression_ratio() * 100.0,
                bytesize::ByteSize(root_blob.score),
            );
        }
        debug!("root compression ratio: {}", s);
    }

    // TODO: store distances

    for root_blob in root_candidates.into_iter().skip(max_root_blobs()) {
        let root = root_blob.blob;
        db::remove(&root).expect("db::remove");
        std::fs::remove_file(&filepath(&root.content_hash))?;
    }

    Ok(())
}

fn store_zip_blob(input_filepath: &str) -> io::Result<Blob> {
    let input_filename = Path::new(&input_filepath)
        .file_name()
        .unwrap()
        .to_str()
        .unwrap();

    let tmp_dir = tmpdir();
    let tmp_unzip_path = NamedTempFile::new_in(&tmp_dir)?;

    let meta = store_zip(input_filepath, tmp_unzip_path.path(), true)?;

    let input_blob = meta.blob(input_filename);
    let store_filepath = filepath(&input_blob.store_hash);
    store_object(tmp_unzip_path, &store_filepath)?;
    Ok(input_blob)
}

fn append_zip_full(input_filepath: &str) -> io::Result<Blob> {
    trace!("append_zip_full: input_filepath={}", input_filepath);

    let blob = store_zip_blob(input_filepath)?;
    db::insert(&blob).expect("failed to insert blob");
    Ok(blob)
}

use std::sync::{atomic::AtomicUsize, Arc};

fn append_zip_delta(
    input_blob: &Blob,
    src_blob: &Blob,
    race: Arc<AtomicUsize>,
) -> io::Result<Option<Blob>> {
    let sw = Stopwatch::start_new();
    let input_filepath = filepath(&input_blob.content_hash);
    let blob = {
        let tmp_dir = tmpdir();
        let tmp_path = NamedTempFile::new_in(&tmp_dir)?;

        let src_hash = &src_blob.content_hash;
        let src_filepath = filepath(src_hash);

        let res = async_std::task::block_on(async {
            let src_file = async_std::fs::File::open(&src_filepath).await?;
            let input_file = async_std::fs::File::open(&input_filepath).await?;
            let dst_file = async_std::fs::File::create(tmp_path.path()).await?;

            let race = RaceWrite::new(dst_file, race);

            delta::delta(delta::ProcessMode::Encode, src_file, input_file, race).await
        });

        let (_input_meta, dst_meta) = match res {
            Ok(s) => s,
            Err(e) => {
                if e.kind() == io::ErrorKind::TimedOut {
                    // timeout from race
                    return Ok(None);
                } else {
                    return Err(e);
                }
            }
        };

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
        "append_zip_delta: ratio={:.02}%, dt_store_delta={}ms",
        blob.compression_ratio() * 100.0,
        dt_store_delta,
    );
    Ok(Some(blob))
}

fn ratio_summary(blobs: &[Blob]) -> String {
    let mut s = String::new();
    for blob in blobs {
        s += &format!("{}={:.02}% ", blob.id, blob.compression_ratio() * 100.0);
    }
    s
}

fn append_zip(input_filepath: &str) -> io::Result<()> {
    debug!("append_zip: input_filepath={}", input_filepath);

    let root_blobs = db::roots().expect("db::roots");

    let sw = Stopwatch::start_new();
    let input_blob = append_zip_full(input_filepath)?;
    info!("append_zip: dt_store_zip={}ms", sw.elapsed_ms(),);

    if root_blobs.is_empty() {
        return Ok(());
    }

    let race = Arc::new(AtomicUsize::new(0));

    let link_blobs = root_blobs
        .into_par_iter()
        .map(|root_blob| append_zip_delta(&input_blob, &root_blob, race.clone()))
        .collect::<io::Result<Vec<_>>>()?;

    let mut link_blobs = link_blobs.into_iter().filter_map(|v| v).collect::<Vec<_>>();

    link_blobs.sort_by_key(|blob| blob.store_size);

    debug!("compression ratio: {}", ratio_summary(&link_blobs));

    for blob in link_blobs.into_iter().skip(1) {
        db::remove(&blob).expect("db::remove");
        std::fs::remove_file(&filepath(&blob.store_hash))?;
    }

    cleanup()?;

    Ok(())
}

pub fn bench_zip(input_filepath: &str, parallel: bool) -> io::Result<()> {
    let tmp_dir = tmpdir();
    let tempfile = NamedTempFile::new_in(&tmp_dir)?;

    let ws = Stopwatch::start_new();
    let _meta = store_zip(input_filepath, tempfile.path(), parallel)?;
    info!("store_zip took {}ms", ws.elapsed_ms());
    Ok(())
}

pub fn debug_stats() -> io::Result<()> {
    let blobs = db::all().expect("db::all");

    let stats = Stats::from_blobs(blobs);
    info!("info\n{}", stats.size_info());

    Ok(())
}
