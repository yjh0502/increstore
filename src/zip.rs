use std::io;
use std::path::Path;

use pbr::ProgressBar;

use crate::hashrw::*;

fn zip_to_tar<R: io::Read + io::Seek, W: io::Write>(src: R, dst: W) -> io::Result<()> {
    let mut zip = zip::ZipArchive::new(src)?;
    let mut ar = tar::Builder::new(dst);

    let mut pb = ProgressBar::new(zip.len() as u64);

    for i in 0..zip.len() {
        let mut file = zip.by_index(i)?;
        let filename = file.name().to_owned();

        let mut header = tar::Header::new_gnu();
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

        ar.append(&mut header, &mut file)?;
        pb.inc();
    }
    pb.finish();

    Ok(())
}

pub fn store_zip<P1, P2>(input_path: P1, dst_path: P2) -> std::io::Result<WriteMetadata>
where
    P1: AsRef<Path>,
    P2: AsRef<Path>,
{
    let mut input_file = std::fs::File::open(input_path.as_ref())?;
    let dst_file = std::fs::File::create(dst_path.as_ref())?;

    let mut dst_file = HashRW::new(dst_file);

    trace!(
        "zip_to_tar: src={:?}, dst={:?}",
        input_path.as_ref(),
        dst_path.as_ref()
    );
    zip_to_tar(&mut input_file, io::BufWriter::new(&mut dst_file))?;

    Ok(dst_file.meta())
}
