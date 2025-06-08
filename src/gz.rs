use super::Result;
use crate::rw::*;
use std::path::*;

pub fn store_gz<P1, P2>(input_path: P1, dst_path: P2) -> Result<WriteMetadata>
where
    P1: AsRef<Path>,
    P2: AsRef<Path>,
{
    let input_file = std::fs::File::open(input_path)?;
    let mut dst_file = std::fs::File::create(dst_path)?;
    let mut decoder = flate2::read::GzDecoder::new(input_file);

    let mut out_file = HashRW::new(&mut dst_file);

    std::io::copy(&mut decoder, &mut out_file)?;
    Ok(out_file.meta())
}

pub fn store_plain<P1, P2>(input_path: P1, dst_path: P2) -> Result<WriteMetadata>
where
    P1: AsRef<Path>,
    P2: AsRef<Path>,
{
    let mut input_file = std::fs::File::open(input_path)?;
    let mut dst_file = std::fs::File::create(dst_path)?;
    let mut out_file = HashRW::new(&mut dst_file);

    std::io::copy(&mut input_file, &mut out_file)?;
    Ok(out_file.meta())
}
