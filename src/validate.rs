use super::*;

pub fn validate(conn: &mut db::Conn) -> Result<()> {
    let blobs = db::all(conn)?;
    let stats = Stats::from_blobs(blobs);

    validate_blob_root(0, stats)?;

    Ok(())
}

pub fn validate_blob_root(idx: usize, stats: Stats) -> Result<()> {
    let stats = Arc::new(stats);
    let src_filepath = filepath(&stats.blobs[idx].store_hash);

    validate_blob_children(0, src_filepath, stats)?;

    Ok(())
}

fn validate_blob_children<P>(parent_idx: usize, src_filepath: P, stats: Arc<Stats>) -> Result<()>
where
    P: AsRef<Path> + Send + Sync,
{
    let mut children = stats.children(parent_idx, true);
    children.sort_by_key(|idx| stats.child_count(*idx));

    let last = children.pop();
    let src_path_buf = src_filepath.as_ref().to_path_buf();
    for child_idx in children {
        validate_blob_children0(child_idx, src_path_buf.clone(), stats.clone())?;
    }

    if let Some(child_idx) = last {
        // drop src_filepath (probably NamedTempFile itself) while handling last child
        validate_blob_children0(child_idx, src_filepath, stats)?;
    }
    Ok(())
}

fn validate_blob_children0<'a, P>(
    child_idx: usize,
    src_filepath: P,
    stats: Arc<Stats>,
) -> Result<()>
where
    P: AsRef<Path> + Send + Sync + 'a,
{
    if stats.child_count(child_idx) == 1 {
        // leaf node
        validate_blob_delta_null(child_idx, src_filepath, stats)?;
    } else {
        // non-leaf node
        let tmpfile = validate_blob_delta(child_idx, src_filepath, stats.clone())?;
        validate_blob_children(child_idx, tmpfile, stats)?;
    }
    Ok(())
}

fn validate_blob_delta<P>(idx: usize, src_filepath: P, stats: Arc<Stats>) -> Result<NamedTempFile>
where
    P: AsRef<Path>,
{
    let dst_file = NamedTempFile::new_in(&tmpdir())?;
    let dst_file = validate_blob_delta0(idx, src_filepath, &stats, dst_file)?;
    Ok(dst_file)
}

fn validate_blob_delta_null<P>(idx: usize, src_filepath: P, stats: Arc<Stats>) -> Result<()>
where
    P: AsRef<Path>,
{
    let dst_file = NamedTempFile::new_in(&tmpdir())?;
    validate_blob_delta0(idx, src_filepath, &stats, dst_file)?;
    Ok(())
}

fn validate_blob_delta0<P>(
    idx: usize,
    src_filepath: P,
    stats: &Stats,
    dst_file: NamedTempFile,
) -> Result<NamedTempFile>
where
    P: AsRef<Path>,
{
    let blob = &stats.blobs[idx];
    let delta_filepath = filepath(&blob.store_hash);

    let sw = Stopwatch::start_new();
    let mode = delta::ProcessMode::Decode;

    let dst_meta = {
        // mmap based
        let input_file = delta_filepath;
        let src_file = src_filepath;
        delta::delta_file(mode, src_file, input_file, dst_file.path())?.unwrap()
    };

    let throughput = 1000 * dst_meta.len() / sw.elapsed_ms() as u64;
    debug!(
        "validate took={}ms {}/s filename={}",
        sw.elapsed_ms(),
        bytesize::ByteSize(throughput),
        blob.filename
    );

    assert_eq!(blob.content_hash, dst_meta.digest());
    assert_eq!(blob.content_size, dst_meta.len());

    Ok(dst_file)
}
