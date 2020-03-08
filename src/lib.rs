use std::io;
use std::path::*;

use log::*;
use rayon::prelude::*;
use stopwatch::Stopwatch;
use tempfile::*;
use thiserror::Error;

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

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Rsqlite(#[from] rusqlite::Error),
    #[error(transparent)]
    TempFilePersist(#[from] tempfile::PersistError),
}

pub type Result<T> = std::result::Result<T, Error>;

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

fn filepath(s: &str) -> String {
    format!("{}/objects/{}/{}", prefix(), &s[..2], &s[2..]).into()
}

fn store_object<P>(src_path: NamedTempFile, dst_path: P) -> Result<()>
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

fn update_blob(tmp_path: NamedTempFile, blob: &Blob) -> Result<()> {
    let path = filepath(&blob.store_hash);

    trace!("path={:?}", path);
    store_object(tmp_path, &path)?;

    // TODO: update id
    db::insert(blob)?;
    Ok(())
}

pub fn get(filename: &str, out_filename: &str) -> Result<()> {
    let mut blobs = db::by_filename(filename)?;
    if blobs.is_empty() {
        panic!("unknown filename: {}", filename);
    }

    let mut decode_path = Vec::new();

    let mut blob = blobs.pop().unwrap();
    while let Some(parent_hash) = &blob.parent_hash {
        let mut blobs = db::by_content_hash(parent_hash)?;
        assert!(!blobs.is_empty());

        let old_blob = std::mem::replace(&mut blob, blobs.pop().unwrap());
        decode_path.push(old_blob);
    }

    decode_path.reverse();

    assert!(blob.parent_hash.is_none());

    let tmp_dir = tmpdir();
    let mut old_tmpfile = NamedTempFile::new_in(&tmp_dir)?;
    let mut tmpfile = NamedTempFile::new_in(&tmp_dir)?;

    let mut src_filepath = PathBuf::from(filepath(&blob.content_hash));
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

pub fn exists(filename: &str) -> Result<()> {
    let input_filename = Path::new(&filename).file_name().unwrap().to_str().unwrap();

    let blobs = db::by_filename(&input_filename)?;
    if blobs.is_empty() {
        std::process::exit(1);
    } else {
        println!("{}", blobs[0].store_hash);
    }
    Ok(())
}

struct RootBlob<'a> {
    blob: &'a Blob,
    alias: &'a Blob,
    score: u64,
}

pub fn cleanup() -> Result<()> {
    let blobs = db::all()?;
    let stats = Stats::from_blobs(blobs);

    let mut root_candidates = Vec::new();
    for (root_idx, root_blob) in stats.blobs.iter().enumerate() {
        if root_blob.parent_hash.is_some() {
            continue;
        }

        let mut aliases = stats.aliases(root_idx);
        if let Some(alias_idx) = aliases.pop() {
            let alias = &stats.blobs[alias_idx];
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
        db::remove(&root)?;
        std::fs::remove_file(&filepath(&root.content_hash))?;
    }

    Ok(())
}

fn store_zip_blob(input_filepath: &str) -> Result<Blob> {
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

fn append_zip_full(input_filepath: &str) -> Result<Blob> {
    trace!("append_zip_full: input_filepath={}", input_filepath);

    let blob = store_zip_blob(input_filepath)?;
    db::insert(&blob)?;
    Ok(blob)
}

use std::sync::{atomic::AtomicUsize, Arc};

fn append_zip_delta(
    input_blob: &Blob,
    src_blob: &Blob,
    race: Arc<AtomicUsize>,
) -> Result<Option<Blob>> {
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
                    return Err(e.into());
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

pub fn push_zip(input_filepath: &str) -> Result<()> {
    debug!("append_zip: input_filepath={}", input_filepath);

    let root_blobs = db::roots()?;

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
        .collect::<Result<Vec<_>>>()?;

    let mut link_blobs = link_blobs.into_iter().filter_map(|v| v).collect::<Vec<_>>();

    link_blobs.sort_by_key(|blob| blob.store_size);

    debug!("compression ratio: {}", ratio_summary(&link_blobs));

    for blob in link_blobs.into_iter().skip(1) {
        db::remove(&blob)?;
        std::fs::remove_file(&filepath(&blob.store_hash))?;
    }

    cleanup()?;

    Ok(())
}

pub fn bench_zip(input_filepath: &str, parallel: bool) -> Result<()> {
    let tmp_dir = tmpdir();
    let tempfile = NamedTempFile::new_in(&tmp_dir)?;

    let ws = Stopwatch::start_new();
    let _meta = store_zip(input_filepath, tempfile.path(), parallel)?;
    info!("store_zip took {}ms", ws.elapsed_ms());
    Ok(())
}

pub fn debug_stats() -> Result<()> {
    let blobs = db::all()?;

    let stats = Stats::from_blobs(blobs);
    println!("info\n{}", stats.size_info());

    Ok(())
}

pub fn debug_graph(filename: &str) -> Result<()> {
    use std::fmt::Write;

    let blobs = db::all()?;
    let stats = Stats::from_blobs(blobs);

    let mut s = String::new();
    writeln!(s, "digraph increstore {{").ok();
    writeln!(s, "  rankdir=\"LR\"").ok();

    let min_size = (stats.blobs.iter().map(|v| v.store_size).min().unwrap_or(10) as f32).log10();
    let max_size = (stats.blobs.iter().map(|v| v.store_size).max().unwrap_or(10) as f32).log10();

    let min_width = 0.4;
    let max_width = 2.0;
    let abs_min_width = 0.7;

    let size_project = |size: u64| {
        let size = (size as f32).log10();
        let ratio = (size - min_size) / (max_size - min_size);
        (min_width + (max_width - min_width) * ratio).max(abs_min_width)
    };

    for (idx, blob) in stats.blobs.iter().enumerate() {
        let name = stats.node_name(idx);
        let label = format!("{}\\n{}", name, bytesize::ByteSize(blob.store_size));

        let size = size_project(blob.store_size);
        let style = if blob.is_root() {
            "shape=doublecircle style=filled fillcolor=red"
        } else {
            "shape=circle"
        };
        writeln!(
            s,
            "  {} [label=\"{}\" width={:.02} fixedsize=true {}];",
            name, label, size, style
        )
        .ok();
    }

    {
        let mut spline_idx = 0;
        let mut spline = stats.node_name(spline_idx);
        loop {
            // TODO: handle root spline node
            if let Some(alias_idx) = stats.aliases(spline_idx).pop() {
                spline_idx = alias_idx;
            }
            let children = stats.children(spline_idx, true);
            if children.is_empty() {
                break;
            }

            let mut max_child_count = 0;
            let mut max_child_idx = 0;
            let mut update_child = |idx: usize| {
                let child_count = stats.depths[idx].child_count;
                if child_count > max_child_count {
                    max_child_count = child_count;
                    max_child_idx = idx;
                }
            };

            for child_idx in children {
                update_child(child_idx);
                for alias_idx in stats.aliases(child_idx) {
                    update_child(alias_idx);
                }
            }
            write!(spline, "->{}", stats.node_name(max_child_idx)).ok();
            spline_idx = max_child_idx;
        }
        writeln!(s, "  {} [style=invis weight=100]", spline).ok();
    }

    for (idx, _blob) in stats.blobs.iter().enumerate() {
        let node = &stats.depths[idx];
        if let Some(parent_idx) = node.parent_idx {
            writeln!(
                s,
                "  {} -> {};",
                stats.node_name(parent_idx),
                stats.node_name(idx),
            )
            .ok();
            //
        }
    }

    writeln!(s, "}}").ok();

    std::fs::write(filename, s)?;

    Ok(())
}

pub fn debug_list_files(roots: bool, non_roots: bool, long: bool) -> Result<()> {
    let blobs = db::all()?;
    for blob in blobs {
        if (roots && blob.is_root()) || (non_roots && !blob.is_root()) {
            let path = filepath(&blob.store_hash);
            if long {
                println!("{} {}", path, blob.filename);
            } else {
                println!("{}", path);
            }
        }
    }

    Ok(())
}
