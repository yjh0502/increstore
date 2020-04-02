use crate::rw::*;
use std::path::*;

pub fn store_gz<P1, P2>(input_path: P1, dst_path: P2) -> std::io::Result<WriteMetadata>
where
    P1: AsRef<Path>,
    P2: AsRef<Path>,
{
    let input_file = std::fs::File::open(input_path)?;
    let mut dst_file = std::fs::File::open(dst_path)?;
    let mut decoder = flate2::read::GzDecoder::new(input_file);

    let mut out_file = HashRW::new(&mut dst_file);

    std::io::copy(&mut decoder, &mut out_file)?;
    Ok(out_file.meta())
}
