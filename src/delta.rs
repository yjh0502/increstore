use crate::rw::*;
use std::marker::Unpin;
use tokio::io::*;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

pub use xdelta3::stream::ProcessMode;

/// uses std::io::Result to trigger TimedOut
pub async fn delta<R1, R2, W>(
    op: xdelta3::stream::ProcessMode,
    src_reader: R1,
    input_reader: R2,
    dst: W,
) -> std::io::Result<(WriteMetadata, WriteMetadata)>
where
    R1: AsyncRead + Unpin,
    R2: AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut input_reader = HashRW::new(input_reader);
    let mut dst = HashRW::new(dst);

    let cfg = xdelta3::stream::Xd3Config::new()
        .source_window_size(100_000_000)
        .no_compress(true)
        .level(0);

    xdelta3::stream::process_async(
        cfg,
        op,
        (&mut input_reader).compat(),
        src_reader.compat(),
        (&mut dst).compat_write(),
    )
    .await?;

    let input_meta = input_reader.meta();
    let dst_meta = dst.meta();

    Ok((input_meta, dst_meta))
}
