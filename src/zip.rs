use std::io;
use std::path::Path;
use std::sync::Arc;

use futures::prelude::*;
use log::*;
use pbr::ProgressBar;

use crate::rw::*;

struct TarEntry {
    header: tar::Header,
    data: Vec<u8>,
}

fn zip_to_tarentry<R>(zipar: &mut zip::ZipArchive<R>, idx: usize) -> io::Result<TarEntry>
where
    R: io::Read + io::Seek,
{
    let mut file = zipar.by_index(idx)?;
    let filename = file.name().to_owned();

    let mut header = tar::Header::new_ustar();
    header.set_path(&filename)?;
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

    let unixtime = file.last_modified().to_time().to_timespec().sec;
    header.set_mtime(unixtime as u64);

    header.set_cksum();

    let mut data = Vec::with_capacity(file.size() as usize);
    io::copy(&mut file, &mut data)?;

    Ok(TarEntry { header, data })
}

#[allow(unused)]
fn zip_to_tar_par<P: AsRef<Path>, W: io::Write>(src_path: P, dst: W) -> io::Result<()> {
    const PAR_JOBS: usize = 8;

    let mut files = Vec::new();
    let mut file_len = 0;
    for _ in 0..PAR_JOBS {
        let file = std::fs::File::open(&src_path)?;
        let zipar = zip::ZipArchive::new(std::io::BufReader::new(file))?;
        file_len = zipar.len();
        let file = Arc::new(std::sync::RwLock::new(zipar));
        files.push(file);
    }

    let mut f_list = Vec::new();
    for i in 0..file_len {
        let file_idx = i % PAR_JOBS;
        let file_lock = files[file_idx].clone();
        f_list.push((i, file_lock));
    }

    let mut pb = ProgressBar::new(file_len as u64);
    let mut ar = tar::Builder::new(dst);
    let res = stream::iter(f_list)
        .map(|(i, file_lock)| {
            tokio::task::spawn_blocking(move || {
                let file = &mut file_lock.write().expect("failed to acquire lock");
                let res = zip_to_tarentry(file, i);
                res
            })
            .map(|res| res.expect("failed to spawn"))
        })
        .buffered(PAR_JOBS * 16)
        .try_fold((pb, ar), |(mut pb, mut ar), entry| {
            match ar.append(&entry.header, entry.data.as_slice()) {
                Ok(_) => {
                    pb.inc();
                    future::ready(Ok((pb, ar)))
                }
                Err(e) => future::ready(Err(e)),
            }
        });

    let rt = tokio::runtime::Runtime::new()?;
    let (mut pb, _ar) = rt.block_on(res)?;
    pb.finish();

    Ok(())
}

#[allow(unused)]
fn zip_to_tar<R: io::Read + io::Seek, W: io::Write>(src: R, dst: W) -> io::Result<()> {
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

pub fn store_zip<P1, P2>(
    input_path: P1,
    dst_path: P2,
    parallel: bool,
) -> std::io::Result<WriteMetadata>
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

    if parallel {
        zip_to_tar_par(
            input_path,
            io::BufWriter::with_capacity(1024 * 1024 * 8, &mut dst_file),
        )?;
    } else {
        let mut input_file = std::fs::File::open(input_path.as_ref())?;
        zip_to_tar(&mut input_file, io::BufWriter::new(&mut dst_file))?;
    }

    Ok(dst_file.meta())
}
