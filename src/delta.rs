use crate::hashrw::*;

pub use xdelta3::stream::ProcessMode;

pub async fn delta<R1, R2, W>(
    op: xdelta3::stream::ProcessMode,
    src_reader: R1,
    input_reader: R2,
    dst: W,
) -> std::io::Result<(WriteMetadata, WriteMetadata)>
where
    R1: async_std::io::Read + std::marker::Unpin,
    R2: async_std::io::Read + std::marker::Unpin,
    W: async_std::io::Write + std::marker::Unpin,
{
    use async_std::io;

    let mut input_reader = HashRW::new(input_reader);
    let mut dst = HashRW::new(dst);

    let cfg = xdelta3::stream::Xd3Config::new()
        .source_window_size(100_000_000)
        .no_compress(true)
        .level(0);
    xdelta3::stream::process_async(
        cfg,
        op,
        &mut input_reader,
        src_reader,
        io::BufWriter::new(&mut dst),
    )
    .await
    .expect("failed to encode/decode");

    let input_meta = input_reader.meta();
    let dst_meta = dst.meta();

    Ok((input_meta, dst_meta))
}
