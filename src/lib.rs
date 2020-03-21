use std::io;
use std::path::*;

use log::*;
use rayon::prelude::*;
use stopwatch::Stopwatch;
use tempfile::*;
use thiserror::Error;

mod batch;
pub mod db;
mod delta;
mod rw;
mod stats;
pub mod zip;

use crate::zip::store_zip;
pub use batch::import_urls;
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
    #[error(transparent)]
    Hyper(#[from] hyper::Error),
    #[error(transparent)]
    WalkDir(#[from] walkdir::Error),
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

fn update_blob(tmp_path: NamedTempFile, blob: &Blob) -> Result<bool> {
    let path = filepath(&blob.store_hash);

    trace!("path={:?}", path);
    store_object(tmp_path, &path)?;

    // TODO: update id
    db::insert(blob).map_err(Error::from)
}

pub fn get(filename: &str, out_filename: &str, dry_run: bool) -> Result<()> {
    let mut blob = match db::by_filename(filename)?.pop() {
        Some(blob) => blob,
        None => {
            eprintln!("unknown filename: {}", filename);
            //TODO
            return Ok(());
        }
    };

    let mut decode_path = Vec::new();

    //TODO: use graph?
    while let Some(parent_hash) = &blob.parent_hash {
        let parent_blob = db::by_content_hash(parent_hash)?
            .pop()
            .expect(&format!("no blob with content_hash {}", parent_hash));

        let old_blob = std::mem::replace(&mut blob, parent_blob);
        decode_path.push(old_blob);
    }

    decode_path.reverse();

    if dry_run {
        for blob in decode_path {
            println!("{} {}", filepath(&blob.store_hash), blob.filename);
        }
        return Ok(());
    }

    assert!(blob.parent_hash.is_none());

    let tmp_dir = tmpdir();
    let mut old_tmpfile = NamedTempFile::new_in(&tmp_dir)?;
    let mut tmpfile = NamedTempFile::new_in(&tmp_dir)?;

    let mut src_filepath = PathBuf::from(filepath(&blob.content_hash));
    for delta_blob in decode_path {
        let delta_filepath = filepath(&delta_blob.store_hash);
        debug!("decode filename={}", delta_blob.filename);
        debug!("trace={:?}, input={:?}", src_filepath, delta_filepath);
        let (_input_meta, dst_meta) = async_std::task::block_on(async {
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

        trace!("delta.content_hash={}", delta_blob.content_hash);
        trace!("dst.content_hash  ={}", dst_meta.digest());
        assert_eq!(delta_blob.content_hash, dst_meta.digest());
        std::mem::swap(&mut tmpfile, &mut old_tmpfile);
        src_filepath = old_tmpfile.path().to_path_buf();
    }

    // result: old_tmpfile
    old_tmpfile.persist(out_filename)?;

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

pub fn dehydrate() -> Result<()> {
    let blobs = db::all()?;
    let stats = Stats::from_blobs(blobs);

    let root_candidates = stats.root_candidates();
    for root_blob in root_candidates {
        let path = filepath(&root_blob.blob.content_hash);
        match std::fs::remove_file(&path) {
            Ok(()) => {
                info!("dehydrating blob={}", path);
            }
            Err(_e) => {
                info!(
                    "dehydrating blob={} failed, already dehydrated? err={:?}",
                    path, _e
                );
            }
        }
    }

    Ok(())
}

pub fn hydrate() -> Result<()> {
    let blobs = db::all()?;
    let stats = Stats::from_blobs(blobs);

    let root_candidates = stats.root_candidates();
    for root_blob in root_candidates {
        let path = filepath(&root_blob.blob.content_hash);
        info!("hydrating blob={}", path);
        get(&root_blob.blob.filename, &path, false)?;
    }

    Ok(())
}

pub fn cleanup() -> Result<()> {
    let blobs = db::all()?;
    let stats = Stats::from_blobs(blobs);

    let mut root_candidates = stats.root_candidates();
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

fn append_zip_full(input_filepath: &str) -> Result<Option<Blob>> {
    trace!("append_zip_full: input_filepath={}", input_filepath);

    let blob = store_zip_blob(input_filepath)?;
    if db::insert(&blob)? {
        Ok(Some(blob))
    } else {
        Ok(None)
    }
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
        if !update_blob(tmp_path, &blob)? {
            info!(
                "append_zip_delta: failed to insert, store_hash={}",
                blob.store_hash
            );
            return Ok(None);
        }
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
    let input_blob = match append_zip_full(input_filepath)? {
        Some(blob) => blob,
        None => {
            info!("append_zip: content already exists, skipping");
            return Ok(());
        }
    };
    info!("append_zip: dt_store_zip={}ms", sw.elapsed_ms(),);

    if root_blobs.is_empty() {
        info!("append_zip: no root blobs: genesis");
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
        let spine = stats.spine();

        for (idx, pair) in spine.windows(2).enumerate() {
            writeln!(
                s,
                "{}->{}[label=\"{}\"];",
                stats.node_name(pair[0]),
                stats.node_name(pair[1]),
                idx
            )
            .ok();
        }

        for (i, idx) in spine.into_iter().enumerate() {
            let name = stats.node_name(idx);
            if i == 0 {
                writeln!(s, "{}", name).ok();
            } else {
                writeln!(s, "->{}", name).ok();
            }
        }
        writeln!(s, " [style=invis weight=100]").ok();
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

pub fn debug_list_files(genesis: bool, roots: bool, non_roots: bool, long: bool) -> Result<()> {
    let blobs = db::all()?;
    for (idx, blob) in blobs.into_iter().enumerate() {
        let is_root = blob.is_root();

        // TODO: better genesis check?
        let should_print = (roots && is_root) || (non_roots && !is_root) || (genesis && idx == 0);

        if !should_print {
            continue;
        }

        let path = filepath(&blob.store_hash);
        if long {
            println!("{} {}", path, blob.filename);
        } else {
            println!("{}", path);
        }
    }

    Ok(())
}

fn path_to_hash(mut path: PathBuf, root: &Path) -> Option<String> {
    let mut s = String::new();
    while let Some(name) = path.file_name() {
        let file_name = name.to_str()?;
        s = file_name.to_owned() + &s;

        path.pop();
        if path == root {
            break;
        }
    }
    Some(s)
}

pub fn debug_blobs() -> Result<()> {
    let blobs = db::all()?;

    // check blob store
    {
        use std::collections::hash_map::Entry;
        use std::collections::HashMap;

        let pathstr = format!("{}/objects", prefix());
        let objectdir = Path::new(&pathstr);

        let mut objects = HashMap::new();
        for entry in walkdir::WalkDir::new(&objectdir) {
            let entry = entry?;
            if entry.file_type().is_dir() {
                continue;
            }
            let hash = match path_to_hash(entry.path().to_path_buf(), &objectdir) {
                Some(hash) => hash,
                None => {
                    error!("failed to get hash from path: {:?}", entry.path());
                    continue;
                }
            };
            objects.insert(hash, entry.metadata()?);
        }

        for blob in &blobs {
            match objects.entry(blob.store_hash.clone()) {
                Entry::Occupied(ent) => {
                    let (_k, v) = ent.remove_entry();
                    if v.len() != blob.store_size {
                        error!(
                            "invalid file size: expected={}, actual={}",
                            blob.store_size,
                            v.len()
                        );
                    }
                }
                Entry::Vacant(_ent) => {
                    error!("blob not exists: {}", blob.store_hash);
                }
            }
        }

        for (k, _v) in objects {
            error!("unexpected blob: {}", k);
        }
    }

    // check if all blobs are reachable from a genesis blob
    {
        let stats = Stats::from_blobs(blobs);
        let mut reached = Vec::with_capacity(stats.blobs.len());
        reached.resize(stats.blobs.len(), false);
        mark_reached(0, &stats, &mut reached);

        for (idx, reached) in reached.iter().enumerate() {
            if stats.blobs[idx].is_root() {
                continue;
            }

            if !reached {
                error!("blob not reachable, idx={}", idx);
            }
        }
    }

    Ok(())
}

fn mark_reached(idx: usize, stats: &Stats, reached: &mut [bool]) {
    reached[idx] = true;
    for child_idx in stats.children(idx, true) {
        mark_reached(child_idx, stats, reached);
    }
}

pub fn validate() -> Result<()> {
    let blobs = db::all()?;
    let stats = Stats::from_blobs(blobs);

    validate_blob_root(0, stats)?;

    Ok(())
}

pub fn validate_blob_root(idx: usize, stats: Stats) -> Result<()> {
    let blob = &stats.blobs[idx];

    let src_filepath = filepath(&blob.store_hash);
    validate_blob_children(0, &src_filepath, &stats)?;

    Ok(())
}

fn validate_blob_children<P: AsRef<Path>>(
    parent_idx: usize,
    src_filepath: P,
    stats: &Stats,
) -> Result<()> {
    // let children = stats.children(parent_idx, true);
    // validate_blob_children_par(&children, src_filepath, stats)

    let mut children = stats.children(parent_idx, true);
    children.sort_by_key(|idx| stats.depths[*idx].child_count);

    let last = children.pop();
    for child_idx in children {
        let tmpfile = validate_blob_delta(child_idx, &src_filepath, &stats)?;
        validate_blob_children(child_idx, tmpfile, stats)?;
    }

    if let Some(child_idx) = last {
        // drop src_filepath (probably NamedTempFile itself) while handling last child
        let tmpfile = validate_blob_delta(child_idx, src_filepath, &stats)?;
        validate_blob_children(child_idx, tmpfile, stats)?;
    }
    Ok(())
}

#[allow(unused)]
fn validate_blob_children_par<P: AsRef<Path>>(
    children_indices: &[usize],
    src_filepath: P,
    stats: &Stats,
) -> Result<()> {
    let len = children_indices.len();
    match len {
        0 => Ok(()),
        1 => {
            let child_idx = children_indices[0];
            let tmpfile = validate_blob_delta(child_idx, &src_filepath, &stats)?;
            validate_blob_children(child_idx, tmpfile, stats)
        }
        _ => {
            let mid = children_indices.len() / 2;
            let slice0 = &children_indices[..mid];
            let slice1 = &children_indices[mid..];

            let path = src_filepath.as_ref();
            let (r0, r1) = rayon::join(
                || validate_blob_children_par(slice0, path, stats),
                || validate_blob_children_par(slice1, path, stats),
            );
            r0?;
            r1?;
            Ok(())
        }
    }
}

fn validate_blob_delta<P>(idx: usize, src_filepath: P, stats: &Stats) -> Result<NamedTempFile>
where
    P: AsRef<Path>,
{
    let blob = &stats.blobs[idx];
    let delta_filepath = filepath(&blob.store_hash);
    let tmpfile = NamedTempFile::new_in(&tmpdir())?;
    let dst_filepath = tmpfile.path();

    let sw = Stopwatch::start_new();

    let (_input_meta, dst_meta) = if false {
        let mut rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(async move {
            let src_file = tokio::fs::File::open(src_filepath.as_ref()).await?;
            let input_file = tokio::fs::File::open(&delta_filepath).await?;
            let dst_file = tokio::fs::File::create(dst_filepath).await?;
            delta::delta(
                delta::ProcessMode::Decode,
                Compat::new(src_file),
                Compat::new(input_file),
                Compat::new(dst_file),
            )
            .await
        })?
    } else {
        async_std::task::block_on(async move {
            if false {
                // async_std based
                let input_file = async_std::fs::File::open(&delta_filepath).await?;
                let src_file = async_std::fs::File::open(src_filepath.as_ref()).await?;
                let dst_file = async_std::fs::File::create(dst_filepath).await?;
                delta::delta(delta::ProcessMode::Decode, src_file, input_file, dst_file).await
            } else {
                // mmap based
                let input_file = rw::MmapBuf::from_path(&delta_filepath)?;
                let src_file = rw::MmapBuf::from_path(src_filepath)?;
                let dst_file =
                    rw::MmapBufMut::from_path_len(dst_filepath, blob.content_size as usize)?;
                delta::delta(delta::ProcessMode::Decode, src_file, input_file, dst_file).await
            }
        })?
    };

    debug!(
        "validate took={}ms filename={}",
        sw.elapsed_ms(),
        blob.filename
    );

    assert_eq!(blob.content_hash, dst_meta.digest());
    assert_eq!(blob.content_size, dst_meta.len());

    Ok(tmpfile)
}

fn file_hash(filename: &str) -> Result<String> {
    const BUF_SIZE: usize = 8 * 1024 * 1024;

    use std::io::Read;

    let file = std::fs::File::open(filename)?;
    let mut reader = rw::HashRW::new(file);

    let mut buf = Vec::with_capacity(BUF_SIZE);
    buf.resize(BUF_SIZE, 0u8);

    while reader.read(&mut buf)? != 0 {
        //
    }

    Ok(reader.meta().digest())
}

pub fn debug_hash(filename: &str) -> Result<()> {
    let hash = file_hash(filename)?;
    println!("{}", hash);

    Ok(())
}
