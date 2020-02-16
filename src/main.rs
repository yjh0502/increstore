use std::io;
use std::path::Path;

mod db;

#[derive(Clone)]
pub struct WriteMetadata {
    size: u64,
    time_created: time::Timespec,
    hash: sha1::Sha1,
}

impl WriteMetadata {
    fn blob(&self, filename: &str) -> db::Blob {
        let digest = self.hash.digest();
        db::Blob {
            id: 0,
            filename: filename.to_owned(),
            time_created: self.time_created,
            size: self.size,
            content_hash: format!("{}", digest),
            store_hash: format!("{}", digest),
            parent_hash: None,
        }
    }

    fn digest(&self) -> sha1::Digest {
        self.hash.digest()
    }
}

pub struct HashWrite<W> {
    meta: WriteMetadata,
    w: W,
}

impl<W> HashWrite<W> {
    fn new(w: W) -> Self {
        Self {
            meta: WriteMetadata {
                size: 0,
                time_created: time::now().to_timespec(),
                hash: sha1::Sha1::new(),
            },
            w,
        }
    }

    fn meta(&self) -> WriteMetadata {
        self.meta.clone()
    }
}

impl<W: io::Write> io::Write for HashWrite<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self.w.write(buf) {
            Ok(n) => {
                self.meta.size += n as u64;
                self.meta.hash.update(&buf[..n]);
                Ok(n)
            }
            Err(e) => Err(e),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.w.flush()
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

fn prefix() -> &'static str {
    "data"
}

fn filepath(digest: sha1::Digest) -> String {
    let s = format!("{}", digest);
    format!("{}/objects/{}/{}", prefix(), &s[..2], &s[2..])
}

fn store_zip(src_path: &str, dst_path: &str) -> std::io::Result<WriteMetadata> {
    let mut src_file = std::fs::File::open(src_path)?;
    let dst_file = std::fs::File::create(&dst_path)?;

    let mut dst_file = HashWrite::new(io::BufWriter::new(dst_file));

    zip_to_tar(&mut src_file, &mut dst_file)?;

    Ok(dst_file.meta())
}

fn update_meta(tmp_path: &str, filename: &str, meta: &WriteMetadata) -> std::io::Result<()> {
    let path = filepath(meta.digest());

    if let Some(dir) = Path::new(&path).parent() {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::rename(tmp_path, path)?;

    let blob = meta.blob(filename);

    db::insert(blob).expect("failed to insert blob");
    Ok(())
}

fn main() -> io::Result<()> {
    db::prepare().expect("failed to prepare");

    let tmp_path = format!("{}/tmp", prefix());

    let src_filepath = &format!("{}/test.apk", prefix());
    let src_filename = Path::new(&src_filepath)
        .file_name()
        .unwrap()
        .to_str()
        .unwrap();

    let meta = store_zip(src_filepath, &tmp_path)?;
    println!("hash={}", meta.digest());
    update_meta(&tmp_path, src_filename, &meta)?;

    Ok(())
}
