#[macro_use]
extern crate log;

use rayon::prelude::*;
use std::io;
use std::path::*;
use stopwatch::Stopwatch;
use tempfile::*;

pub mod db;
mod delta;
mod hashrw;
pub mod zip;

use crate::zip::store_zip;
use db::Blob;

pub fn max_root_blobs() -> usize {
    5
}

pub fn prefix() -> &'static str {
    "data"
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
            delta::delta(
                delta::ProcessMode::Decode,
                &src_file,
                &delta_filepath,
                &tmpfile.path(),
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

pub fn cleanup() -> io::Result<()> {
    let roots = db::roots().expect("db::roots");
    let mut root_candidates = Vec::new();

    for root_blob in roots {
        let blobs = db::by_content_hash(&root_blob.content_hash).expect("db::get");
        if let Some(backref) = blobs.into_iter().find(|b| b.parent_hash.is_some()) {
            root_candidates.push((root_blob, backref));
        }
    }

    root_candidates.sort_by_key(|(_root_blob, backref)| {
        //
        10000 - backref.store_size * 10000 / backref.content_size
    });

    {
        let mut s = String::new();
        for (_root, backref) in &root_candidates {
            s += &format!(
                "{}={:.02}% ",
                backref.id,
                backref.compression_ratio() * 100.0
            );
        }
        debug!("root compression ratio: {}", s);
    }

    // TODO: store distances

    for (root, _backref) in root_candidates.into_iter().skip(max_root_blobs()) {
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

fn append_zip_delta(input_blob: &Blob, src_blob: &Blob) -> io::Result<Blob> {
    let sw = Stopwatch::start_new();
    let input_filepath = filepath(&input_blob.content_hash);
    let blob = {
        let tmp_dir = tmpdir();
        let tmp_path = NamedTempFile::new_in(&tmp_dir)?;

        let src_hash = &src_blob.content_hash;
        let src_filepath = filepath(src_hash);

        let (_input_meta, dst_meta) = async_std::task::block_on(async {
            let src_file = async_std::fs::File::open(&src_filepath).await?;
            delta::delta(
                delta::ProcessMode::Encode,
                src_file,
                &input_filepath,
                &tmp_path,
            )
            .await
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
        "append_zip_delta: ratio={:.02}%, dt_store_delta={}ms",
        blob.compression_ratio() * 100.0,
        dt_store_delta,
    );
    Ok(blob)
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

    let mut link_blobs = root_blobs
        .into_par_iter()
        .map(|root_blob| append_zip_delta(&input_blob, &root_blob))
        .collect::<io::Result<Vec<_>>>()?;

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

pub fn calculate_depth(idx: usize, blobs: &[Blob], depths: &mut [usize]) -> usize {
    let blob = &blobs[idx];

    match blob.parent_hash {
        None => {
            depths[idx] = 1;
            1
        }
        Some(ref parent_hash) => {
            let mut min_depth = blobs.len();

            for (parent_idx, parent) in blobs.iter().enumerate() {
                if parent_idx == idx {
                    continue;
                }
                if &parent.content_hash != parent_hash {
                    continue;
                }

                let depth = if depths[parent_idx] == 0 {
                    calculate_depth(parent_idx, blobs, depths)
                } else {
                    depths[parent_idx]
                };
                if depth < min_depth {
                    min_depth = depth;
                }
            }
            trace!("{}={}", idx, min_depth + 1);
            depths[idx] = min_depth + 1;
            min_depth
        }
    }
}

pub fn debug_depth() -> io::Result<()> {
    let blobs = db::all().expect("db::all");

    let mut depths = Vec::with_capacity(blobs.len());
    depths.resize(blobs.len(), 0);

    for i in 0..blobs.len() {
        calculate_depth(i, &blobs, &mut depths);
    }

    let bucket_size = (blobs.len().next_power_of_two().trailing_zeros() as usize) + 1;
    let mut bucket = Vec::with_capacity(bucket_size);
    bucket.resize(bucket_size, 0);

    for i in 0..blobs.len() {
        let depth = depths[i];
        let bucket_idx = depth.next_power_of_two().trailing_zeros() as usize;
        bucket[bucket_idx] += 1;
    }

    for (i, count) in bucket.into_iter().enumerate() {
        let (start, end) = if i == 0 {
            (0, 1)
        } else {
            (1 << (i - 1), (1 << i) - 1)
        };
        println!("{:3}~{:3} = {}", start, end, count);
    }

    Ok(())
}
