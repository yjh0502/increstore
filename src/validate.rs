use super::*;

pub fn validate() -> Result<()> {
    let blobs = db::all()?;
    let stats = Stats::from_blobs(blobs);

    validate_blob_root(0, stats)?;

    Ok(())
}

pub fn validate_blob_root(idx: usize, stats: Stats) -> Result<()> {
    let stats = Arc::new(stats);
    let src_filepath = filepath(&stats.blobs[idx].store_hash);
    block_on(validate_blob_children(0, src_filepath, stats))?;

    Ok(())
}

async fn validate_blob_children<P>(
    parent_idx: usize,
    src_filepath: P,
    stats: Arc<Stats>,
) -> Result<()>
where
    P: AsRef<Path> + Send + Sync,
{
    let mut children = stats.children(parent_idx, true);
    children.sort_by_key(|idx| stats.child_count(*idx));

    let last = children.pop();
    let src_path_buf = src_filepath.as_ref().to_path_buf();
    let mut handles = Vec::new();
    for child_idx in children {
        let f = validate_blob_children0(child_idx, src_path_buf.clone(), stats.clone());
        if stats.child_count(child_idx) == 1 {
            handles.push(async_std::task::spawn(f));
        } else {
            f.await?;
        }
    }

    // wait for all async tasks
    for handle in handles {
        handle.await?;
    }

    if let Some(child_idx) = last {
        // drop src_filepath (probably NamedTempFile itself) while handling last child
        validate_blob_children0(child_idx, src_filepath, stats).await?;
    }
    Ok(())
}

use futures::future::*;
fn validate_blob_children0<'a, P>(
    child_idx: usize,
    src_filepath: P,
    stats: Arc<Stats>,
) -> BoxFuture<'a, Result<()>>
where
    P: AsRef<Path> + Send + Sync + 'a,
{
    if stats.child_count(child_idx) == 1 {
        // leaf node
        validate_blob_delta_null(child_idx, src_filepath, stats).boxed()
    } else {
        // non-leaf node
        let f = async move {
            let tmpfile = validate_blob_delta(child_idx, src_filepath, stats.clone()).await?;
            validate_blob_children(child_idx, tmpfile, stats).await
        };
        f.boxed()
    }
}

async fn validate_blob_delta<P>(
    idx: usize,
    src_filepath: P,
    stats: Arc<Stats>,
) -> Result<NamedTempFile>
where
    P: AsRef<Path>,
{
    let dst_file = NamedTempFile::new_in(&tmpdir())?;
    let dst_file = validate_blob_delta0(idx, src_filepath, &stats, Some(dst_file))
        .await?
        .unwrap();
    Ok(dst_file)
}

async fn validate_blob_delta_null<P>(idx: usize, src_filepath: P, stats: Arc<Stats>) -> Result<()>
where
    P: AsRef<Path>,
{
    validate_blob_delta0(idx, src_filepath, &stats, None).await?;
    Ok(())
}

async fn validate_blob_delta0<P>(
    idx: usize,
    src_filepath: P,
    stats: &Stats,
    dst_file: Option<NamedTempFile>,
) -> Result<Option<NamedTempFile>>
where
    P: AsRef<Path>,
{
    let blob = &stats.blobs[idx];
    let delta_filepath = filepath(&blob.store_hash);

    let sw = Stopwatch::start_new();
    let mode = delta::ProcessMode::Decode;

    let (_input_meta, dst_meta) = {
        if false {
            // async_std based
            let input_file = async_std::fs::File::open(&delta_filepath).await?;
            let src_file = async_std::fs::File::open(src_filepath.as_ref()).await?;

            match dst_file {
                Some(ref file) => {
                    let dst_file = async_std::fs::File::create(file.path()).await?;
                    delta::delta(mode, src_file, input_file, dst_file).await?
                }
                None => delta::delta(mode, src_file, input_file, futures::io::sink()).await?,
            }
        } else {
            // mmap based
            let input_file = rw::MmapBuf::from_path(&delta_filepath)?;
            let src_file = rw::MmapBuf::from_path(src_filepath)?;

            match dst_file {
                Some(ref file) => {
                    let dst_file =
                        rw::MmapBufMut::from_path_len(file.path(), blob.content_size as usize)?;
                    delta::delta(mode, src_file, input_file, dst_file).await?
                }
                None => delta::delta(mode, src_file, input_file, futures::io::sink()).await?,
            }
        }
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
