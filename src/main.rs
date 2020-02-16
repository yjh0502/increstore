use std::io;

mod db;

struct HashWrite<W> {
    hash: sha1::Sha1,
    w: W,
}

impl<W: io::Write> io::Write for HashWrite<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self.w.write(buf) {
            Ok(n) => {
                self.hash.update(&buf[..n]);
                Ok(n)
            }
            Err(e) => Err(e),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.w.flush()
    }
}

impl<W> HashWrite<W> {
    fn new(w: W) -> Self {
        Self {
            hash: sha1::Sha1::new(),
            w,
        }
    }

    fn digest(&self) -> sha1::Digest {
        self.hash.digest()
    }
}

fn zip_to_tar<R: io::Read + io::Seek, W: io::Write>(src: &mut R, dst: &mut W) -> io::Result<()> {
    let mut zip = zip::ZipArchive::new(src)?;
    let mut ar = tar::Builder::new(dst);

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
    }

    Ok(())
}

fn main() -> io::Result<()> {
    db::prepare().expect("failed to prepare");

    let mut src_file = std::fs::File::open("data/test.apk")?;
    let dst_file = std::fs::File::create("data/test.tar")?;

    let mut dst_file = HashWrite::new(io::BufWriter::new(dst_file));

    zip_to_tar(&mut src_file, &mut dst_file)?;

    println!("hash={:?}", dst_file.digest());

    Ok(())
}
