use std::path::*;

pub use failure::Error;
use log::*;
use rayon::prelude::*;
use stopwatch::Stopwatch;
use tempfile::*;

pub mod db;
mod delta;
mod gz;
mod rw;
mod stats;
mod validate;
pub mod zip;

use crate::zip::store_zip;
use db::Blob;
use rw::*;
use stats::Stats;
use std::env;
pub use validate::validate;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Copy)]
pub enum FileType {
    Zip,
    Gz,
    Plain,
}

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

fn filerootpath() -> String {
    format!("{}/objects", prefix()).into()
}

fn filepath(s: &str) -> String {
    format!("{}/{}/{}", filerootpath(), &s[..2], &s[2..]).into()
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

fn update_blob(conn: &mut db::Conn, tmp_path: NamedTempFile, blob: &Blob) -> Result<bool> {
    let path = filepath(&blob.store_hash);

    trace!("path={:?}", path);
    store_object(tmp_path, &path)?;

    // TODO: update id
    db::insert(conn, blob).map_err(Error::from)
}

pub fn get(conn: &mut db::Conn, filename: &str, out_filename: &str, dry_run: bool) -> Result<()> {
    let mut blob = match db::by_filename(conn, filename)?.pop() {
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
        let parent_blob = db::by_content_hash(conn, parent_hash)?
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
        let dst_meta = delta::delta_file(
            delta::ProcessMode::Decode,
            src_filepath,
            delta_filepath,
            tmpfile.path(),
        )?
        .expect("should not fail");

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

pub fn exists(conn: &mut db::Conn, filename: &str) -> Result<()> {
    let input_filename = Path::new(&filename).file_name().unwrap().to_str().unwrap();

    let blobs = db::by_filename(conn, &input_filename)?;
    if blobs.is_empty() {
        std::process::exit(1);
    } else {
        println!("{}", blobs[0].store_hash);
    }
    Ok(())
}

pub fn cleanup0(conn: &mut db::Conn) -> Result<()> {
    use std::collections::HashSet;

    // find all blobs
    let root = filerootpath();
    let files = walkdir::WalkDir::new(&root)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .map(|e| -> Result<String> {
            // relative path
            let path = e.path();
            let relative_str = path.to_str().ok_or_else(|| {
                Error::from(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "failed to convert path to str",
                ))
            })?;
            Ok(relative_str.to_owned())
        })
        .collect::<Result<Vec<_>>>()?;

    let mut expected_files = HashSet::new();
    for blob in db::all(conn)? {
        if blob.is_genesis() || !blob.is_root() {
            expected_files.insert(filepath(&blob.store_hash));
        }
    }

    for file in files {
        if expected_files.contains(&file) {
            continue;
        }

        eprintln!("unknown file, delete: path={}", file);
        std::fs::remove_file(&file).ok();
    }

    Ok(())
}

pub fn rename(conn: &mut db::Conn, from_filename: &str, to_filename: &str) -> Result<()> {
    let renamed = db::rename(conn, from_filename, to_filename)?;
    if !renamed {
        error!("file not exists: {}", from_filename);
    }
    Ok(())
}

pub fn dehydrate(conn: &mut db::Conn) -> Result<()> {
    let blobs = db::all(conn)?;
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

pub fn hydrate(conn: &mut db::Conn) -> Result<()> {
    let blobs = db::all(conn)?;
    let stats = Stats::from_blobs(blobs);

    let root_candidates = stats.root_candidates();
    for root_blob in root_candidates {
        let path = filepath(&root_blob.blob.content_hash);
        info!("hydrating blob={}", path);
        get(conn, &root_blob.blob.filename, &path, false)?;
    }

    Ok(())
}

fn archive_add_file<W>(ar: &mut tar::Builder<W>, path: &str) -> Result<()>
where
    W: std::io::Write,
{
    let meta = std::fs::metadata(path)?;
    let size = meta.len();

    let mut header = tar::Header::new_gnu();
    let strip_path = Path::new(path)
        .strip_prefix(&prefix())
        .expect("invalid file");
    header.set_path(strip_path)?;
    header.set_size(size);
    header.set_mode(0o644);

    if let Ok(time) = meta.modified() {
        if let Ok(duration) = time.duration_since(std::time::SystemTime::UNIX_EPOCH) {
            header.set_mtime(duration.as_secs());
        }
    }

    header.set_cksum();

    debug!("add file name={:?}, size={}", strip_path, size);

    let file = std::fs::File::open(path)?;
    ar.append(&header, file)?;
    Ok(())
}

fn archive0<W>(conn: &mut db::Conn, w: W) -> Result<()>
where
    W: std::io::Write,
{
    let mut ar = tar::Builder::new(w);
    archive_add_file(&mut ar, &db::dbpath())?;

    let blobs = db::all(conn)?;
    for blob in blobs {
        if blob.is_genesis() || !blob.is_root() {
            archive_add_file(&mut ar, &filepath(&blob.store_hash))?;
        }
    }
    Ok(())
}

pub fn archive(conn: &mut db::Conn, filename: &str) -> Result<()> {
    if filename != "-" {
        let file = std::fs::File::create(filename)?;
        archive0(conn, file)
    } else {
        let stdout = std::io::stdout();
        let out = stdout.lock();
        archive0(conn, out)
    }
}

pub fn cleanup(conn: &mut db::Conn) -> Result<()> {
    let blobs = db::all(conn)?;
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

    let mut root_indices = vec![];
    for root_blob in root_candidates.iter().take(max_root_blobs()) {
        root_indices.push(root_blob.blob.id);
    }
    let mut latest_id: Option<u32> = None;
    for root_blob in &root_candidates {
        if let Some(ref mut latest_id) = latest_id {
            if root_blob.blob.id > *latest_id {
                *latest_id = root_blob.blob.id;
            }
        }
    }
    if let Some(latest_id) = latest_id {
        root_indices.push(latest_id);
    }

    // TODO: store distances
    for root_blob in root_candidates.into_iter() {
        if root_indices.contains(&root_blob.blob.id) {
            continue;
        }

        let root = root_blob.blob;
        db::remove(conn, &root)?;
        std::fs::remove_file(&filepath(&root.content_hash))?;
    }

    Ok(())
}

fn store_blob<F>(input_filepath: &str, f: F) -> Result<Blob>
where
    F: FnOnce(&Path, &Path) -> std::io::Result<WriteMetadata>,
{
    let input_filename = Path::new(&input_filepath)
        .file_name()
        .unwrap()
        .to_str()
        .unwrap();

    let tmp_dir = tmpdir();
    let tmp_unzip_path = NamedTempFile::new_in(&tmp_dir)?;

    let meta = f(Path::new(input_filepath), tmp_unzip_path.path())?;

    let input_blob = meta.blob(input_filename);
    let store_filepath = filepath(&input_blob.store_hash);
    store_object(tmp_unzip_path, &store_filepath)?;
    Ok(input_blob)
}

fn append_full(conn: &mut db::Conn, input_filepath: &str, ty: FileType) -> Result<Option<Blob>> {
    trace!("append_full: input_filepath={} ty={:?}", input_filepath, ty);

    let blob = match ty {
        FileType::Zip => store_blob(input_filepath, |p1, p2| store_zip(p1, p2, true))?,
        FileType::Gz => store_blob(input_filepath, |p1, p2| gz::store_gz(p1, p2))?,
        FileType::Plain => store_blob(input_filepath, |p1, p2| gz::store_plain(p1, p2))?,
    };
    if db::insert(conn, &blob)? {
        Ok(Some(blob))
    } else {
        Ok(None)
    }
}

use std::sync::{atomic::AtomicUsize, Arc};

fn append_delta(
    input_blob: &Blob,
    src_blob: &Blob,
    race: Arc<AtomicUsize>,
) -> Result<Option<(NamedTempFile, Blob)>> {
    let sw = Stopwatch::start_new();
    let input_filepath = filepath(&input_blob.content_hash);

    let (tmp, blob) = {
        let tmp_dir = tmpdir();
        let tmp_path = NamedTempFile::new_in(&tmp_dir)?;

        let src_hash = &src_blob.content_hash;
        let src_filepath = filepath(src_hash);

        let mut handle = delta::delta_file_handle(
            delta::ProcessMode::Encode,
            src_filepath,
            input_filepath,
            tmp_path.path(),
        )?;

        while let None = handle.try_wait()? {
            // sleep for 1 second
            std::thread::sleep(std::time::Duration::from_millis(1000));

            // check output file size
            if let Ok(metadata) = std::fs::metadata(tmp_path.path()) {
                let len = metadata.len();
                let loaded = race.load(std::sync::atomic::Ordering::Relaxed);
                if loaded > 0 && len > loaded as u64 {
                    handle.kill()?;
                    return Ok(None);
                }
            }
        }

        {
            let metadata = std::fs::metadata(tmp_path.path())?;
            let loaded = race.load(std::sync::atomic::Ordering::Relaxed);
            if loaded < metadata.len() as usize {
                race.store(
                    metadata.len() as usize,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }
        }
        let dst_meta = match handle.meta()? {
            Some(meta) => meta,
            None => {
                return Ok(None);
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
        (tmp_path, blob)
    };
    let dt_store_delta = sw.elapsed_ms();

    info!(
        "append_delta: ratio={:.02}%, dt_store_delta={}ms",
        blob.compression_ratio() * 100.0,
        dt_store_delta,
    );
    Ok(Some((tmp, blob)))
}

fn ratio_summary(blobs: &[(NamedTempFile, Blob)]) -> String {
    let mut s = String::new();
    for blob in blobs {
        let blob = &blob.1;
        s += &format!("{}={:.02}% ", blob.id, blob.compression_ratio() * 100.0);
    }
    s
}

pub fn push(conn: &mut db::Conn, input_filepath: &str, ty: FileType) -> Result<()> {
    debug!("push: input_filepath={}", input_filepath);

    let root_blobs = db::roots(conn)?;

    let sw = Stopwatch::start_new();
    let input_blob = match append_full(conn, input_filepath, ty)? {
        Some(blob) => blob,
        None => {
            info!("push: content already exists, skipping");
            return Ok(());
        }
    };
    info!("push: append_full={}ms", sw.elapsed_ms(),);

    if root_blobs.is_empty() {
        info!("push: no root blobs: genesis");
        return Ok(());
    }

    let race = Arc::new(AtomicUsize::new(0));

    let link_blobs = root_blobs
        .into_par_iter()
        .map(|root_blob| append_delta(&input_blob, &root_blob, race.clone()))
        .collect::<Result<Vec<_>>>()?;

    let mut link_blobs = link_blobs.into_iter().filter_map(|v| v).collect::<Vec<_>>();

    link_blobs.sort_by_key(|blob| blob.1.store_size);

    debug!("compression ratio: {}", ratio_summary(&link_blobs));

    let (tmp_path, blob) = link_blobs.into_iter().next().expect("no blobs");
    // optimal block
    if !update_blob(conn, tmp_path, &blob)? {
        info!(
            "append_delta: failed to insert, store_hash={}",
            blob.store_hash
        );
    }

    cleanup(conn)?;

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

pub fn debug_stats(conn: &mut db::Conn) -> Result<()> {
    let blobs = db::all(conn)?;

    let stats = Stats::from_blobs(blobs);
    println!("info\n{}", stats.size_info());

    Ok(())
}

pub fn debug_graph(conn: &mut db::Conn, filename: &str) -> Result<()> {
    use std::fmt::Write;

    let blobs = db::all(conn)?;
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

pub fn debug_list_files(
    conn: &mut db::Conn,
    genesis: bool,
    roots: bool,
    non_roots: bool,
    long: bool,
) -> Result<()> {
    let blobs = db::all(conn)?;
    for blob in blobs.into_iter() {
        let is_root = blob.is_root();

        // TODO: better genesis check?
        let should_print =
            (roots && is_root) || (non_roots && !is_root) || (genesis && blob.is_genesis());

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

pub fn debug_blobs(conn: &mut db::Conn) -> Result<()> {
    let blobs = db::all(conn)?;

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
