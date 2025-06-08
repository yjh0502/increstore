use std::io;
use std::path::Path;

use anyhow::Result;
use log::*;
use pbr::ProgressBar;

use crate::rw::*;

struct TarEntry {
    header: tar::Header,
    data: Vec<u8>,
}

fn zip_to_tarentry<R>(zipar: &mut zip::ZipArchive<R>, idx: usize) -> Result<TarEntry>
where
    R: io::Read + io::Seek,
{
    let mut file = zipar.by_index(idx)?;
    let filename = file.name().to_owned();

    let mut header = tar::Header::new_ustar();
    if let Err(e) = header.set_path(&filename) {
        return Err(anyhow::anyhow!(
            "Failed to set path in tar header: e={}, filename={}",
            e,
            filename
        ));
    }
    header.set_size(file.size());

    if let Some(mode) = file.unix_mode() {
        header.set_mode(mode);
    } else {
        if file.is_dir() {
            header.set_mode(0o755);
        } else {
            header.set_mode(0o644);
        }
    }

    if let Some(t) = file.last_modified() {
        use std::convert::TryFrom;

        if let Ok(unixtime) = time::OffsetDateTime::try_from(t) {
            header.set_mtime(unixtime.unix_timestamp() as u64);
        }
    }

    header.set_cksum();

    let mut data = Vec::with_capacity(file.size() as usize);
    io::copy(&mut file, &mut data)?;

    Ok(TarEntry { header, data })
}

#[allow(unused)]
fn zip_to_tar<R: io::Read + io::Seek, W: io::Write>(src: R, dst: W) -> Result<()> {
    let mut zip = zip::ZipArchive::new(src)?;
    let mut ar = tar::Builder::new(dst);

    let mut pb = ProgressBar::new(zip.len() as u64);

    for i in 0..zip.len() {
        let entry = zip_to_tarentry(&mut zip, i)?;
        ar.append(&entry.header, entry.data.as_slice())?;
        pb.inc();
    }
    pb.finish();

    Ok(())
}

pub fn store_zip<P1, P2>(input_path: P1, dst_path: P2, _parallel: bool) -> Result<WriteMetadata>
where
    P1: AsRef<Path>,
    P2: AsRef<Path>,
{
    trace!(
        "zip_to_tar: src={:?}, dst={:?}",
        input_path.as_ref(),
        dst_path.as_ref()
    );

    let dst_file = std::fs::File::create(dst_path.as_ref())?;
    let mut dst_file = HashRW::new(dst_file);

    let mut input_file = std::fs::File::open(input_path.as_ref())?;
    zip_to_tar(&mut input_file, io::BufWriter::new(&mut dst_file))?;

    Ok(dst_file.meta())
}
