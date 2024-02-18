use crate::rw::*;
use crate::delta;
use std::path::*;

pub fn store_gz<P1, P2>(input_path: P1, dst_path: P2) -> std::io::Result<WriteMetadata>
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

pub fn store_xz<P1, P2>(input_path: P1, dst_path: P2) -> std::io::Result<WriteMetadata>
where
    P1: AsRef<Path>,
    P2: AsRef<Path>,
{
    let input_file = std::fs::File::open(input_path)?;
    let mut dst_file = std::fs::File::create(dst_path)?;
    let mut decoder = xz2::read::XzDecoder::new(input_file);

    let mut out_file = HashRW::new(&mut dst_file);

    std::io::copy(&mut decoder, &mut out_file)?;
    Ok(out_file.meta())
}

pub fn store_zstd<P1, P2>(input_path: P1, dst_path: P2) -> std::io::Result<WriteMetadata>
where
    P1: AsRef<Path>,
    P2: AsRef<Path>,
{
    log::info!("store_zstd: {:?}", input_path.as_ref());
    let input_file = std::fs::File::open(input_path)?;
    let mut dst_file = std::fs::File::create(dst_path)?;
    let mut decoder = zstd::stream::read::Decoder::new(input_file)?;

    let mut out_file = HashRW::new(&mut dst_file);

    std::io::copy(&mut decoder, &mut out_file)?;
    Ok(out_file.meta())
}

pub fn store_plain<P1, P2>(input_path: P1, dst_path: P2) -> std::io::Result<WriteMetadata>
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

pub fn store_raw<P, R>(mut read: R, dst_path: P) -> std::io::Result<WriteMetadata>
where
    P: AsRef<Path>,
    R: std::io::Read,
{
    let mut dst_file = std::fs::File::create(dst_path)?;
    let mut out_file = HashRW::new(&mut dst_file);

    std::io::copy(&mut read, &mut out_file)?;
    Ok(out_file.meta())
}

pub fn store_delta<P1, P2, P3>(src_path: P1, input_path: P2, dst_path: P3) -> std::io::Result<WriteMetadata>
where
    P1: AsRef<Path>,
    P2: AsRef<Path>,
    P3: AsRef<Path>,
{
    let mut dst_file = std::fs::File::create(dst_path)?;
    let mut out_file = HashRW::new(&mut dst_file);

    // mmap based
    let mode = delta::ProcessMode::Encode;
    let input_file = std::fs::File::open(&input_path)?;
    let src_file = std::fs::File::open(src_path)?;
    delta::delta(mode, input_file, src_file, &mut out_file)?;

    Ok(out_file.meta())
}
