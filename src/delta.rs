use crate::hashrw::*;
use std::path::Path;

pub use xdelta3::stream::ProcessMode;

pub async fn delta<R, P1, P2>(
    op: xdelta3::stream::ProcessMode,
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
        op,
        io::BufReader::new(&mut input_file),
        src_reader,
        io::BufWriter::new(&mut dst_file),
    )
    .await
    .expect("failed to encode/decode");

    let input_meta = input_file.meta();
    let dst_meta = dst_file.meta();

    Ok((input_meta, dst_meta))
}
