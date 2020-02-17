#[macro_use]
extern crate log;

use async_std::task::ready;
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
    fn new() -> Self {
        Self {
            size: 0,
            time_created: time::now().to_timespec(),
            hash: sha1::Sha1::new(),
        }
    }
    fn blob(&self, filename: &str) -> db::Blob {
        let digest = self.hash.digest();
        db::Blob {
            id: 0,
            filename: filename.to_owned(),
            time_created: self.time_created,
            store_size: self.size,
            content_size: self.size,
            store_hash: format!("{}", digest),
            content_hash: format!("{}", digest),
            parent_hash: None,
        }
    }

    fn digest(&self) -> sha1::Digest {
        self.hash.digest()
    }
}

pub struct HashRW<W> {
    meta: WriteMetadata,
    w: W,
}

impl<W> HashRW<W> {
    fn new(w: W) -> Self {
        HashRW {
            meta: WriteMetadata::new(),
            w,
        }
    }

    fn meta(&self) -> WriteMetadata {
        self.meta.clone()
    }
}

impl<W: io::Write> io::Write for HashRW<W> {
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

use std::marker::Unpin;
use std::pin::Pin;
use std::task::{Context, Poll};

impl<W> async_std::io::Write for HashRW<W>
where
    W: async_std::io::Write + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        ctx: &mut Context,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut s = self.as_mut();
        let w = Pin::new(&mut s.w);
        match ready!(w.poll_write(ctx, buf)) {
            Ok(n) => {
                s.meta.size += n as u64;
                s.meta.hash.update(&buf[..n]);
                Poll::Ready(Ok(n))
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<io::Result<()>> {
        let mut s = self.as_mut();
        let w = Pin::new(&mut s.w);
        w.poll_flush(ctx)
    }

    fn poll_close(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<io::Result<()>> {
        let mut s = self.as_mut();
        let w = Pin::new(&mut s.w);
        w.poll_close(ctx)
    }
}

impl<W> async_std::io::Read for HashRW<W>
where
    W: async_std::io::Read + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        ctx: &mut Context,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let mut s = self.as_mut();
        let w = Pin::new(&mut s.w);
        match ready!(w.poll_read(ctx, buf)) {
            Ok(res) => {
                s.meta.hash.update(&buf[..res]);
                Poll::Ready(Ok(res))
            }
            Err(e) => Poll::Ready(Err(e)),
        }
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

fn filepath(s: &str) -> String {
    format!("{}/objects/{}/{}", prefix(), &s[..2], &s[2..])
}

fn store_zip(input_path: &str, dst_path: &str) -> std::io::Result<WriteMetadata> {
    let mut input_file = std::fs::File::open(input_path)?;
    let dst_file = std::fs::File::create(&dst_path)?;

    let mut dst_file = HashRW::new(io::BufWriter::new(dst_file));

    zip_to_tar(&mut input_file, &mut dst_file)?;

    Ok(dst_file.meta())
}

async fn store_delta<R: async_std::io::Read + std::marker::Unpin>(
    src_reader: R,
    input_path: &str,
    dst_path: &str,
) -> std::io::Result<(WriteMetadata, WriteMetadata)> {
    let input_file = async_std::fs::File::open(input_path).await?;
    let dst_file = async_std::fs::File::create(dst_path).await?;

    let mut input_file = HashRW::new(input_file);
    let mut dst_file = HashRW::new(dst_file);

    xdelta3::stream::encode_async(src_reader, &mut input_file, &mut dst_file)
        .await
        .expect("failed to encode");

    let input_meta = input_file.meta();
    let dst_meta = dst_file.meta();

    Ok((input_meta, dst_meta))
}

fn store_object(src_path: &str, dst_path: &str) -> std::io::Result<()> {
    if let Some(dir) = Path::new(&dst_path).parent() {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::rename(src_path, dst_path)
}

fn update_blob(tmp_path: &str, blob: &db::Blob) -> std::io::Result<()> {
    let path = filepath(&blob.store_hash);

    info!("path={}", path);
    store_object(tmp_path, &path)?;

    db::insert(blob).expect("failed to insert blob");
    Ok(())
}

fn append_zip(input_filepath: &str) -> std::io::Result<()> {
    let latest = db::latest().expect("failed to get latest row");

    let src_hash = &latest.content_hash;
    let src_filepath = filepath(src_hash);

    let tmp_unzip_path = format!("{}/tmp_unzip", prefix());
    let tmp_path = format!("{}/tmp", prefix());

    let input_filename = Path::new(&input_filepath)
        .file_name()
        .unwrap()
        .to_str()
        .unwrap();

    let meta = store_zip(input_filepath, &tmp_unzip_path)?;
    let input_blob = meta.blob(input_filename);

    let (_input_meta, dst_meta) = async_std::task::block_on(async {
        let src_file = async_std::fs::File::open(&src_filepath).await?;
        store_delta(src_file, &tmp_unzip_path, &tmp_path).await
    })?;

    let mut blob = dst_meta.blob(input_filename);
    blob.content_size = input_blob.content_size;
    blob.content_hash = input_blob.content_hash.clone();

    debug!(
        "content_hash={}, store_hash={}",
        blob.content_hash, blob.store_hash
    );
    update_blob(&tmp_path, &blob)?;
    Ok(())
}

pub fn main() -> io::Result<()> {
    env_logger::init();

    db::prepare().expect("failed to prepare");

    {
        let tmp_path = format!("{}/tmp", prefix());

        let input_filepath = &format!("{}/test.apk", prefix());
        let input_filename = Path::new(&input_filepath)
            .file_name()
            .unwrap()
            .to_str()
            .unwrap();

        let meta = store_zip(input_filepath, &tmp_path)?;
        debug!("hash={}", meta.digest());

        let blob = meta.blob(input_filename);
        update_blob(&tmp_path, &blob)?;
        blob
    };

    if true {
        let input_filepath = &format!("{}/test.apk", prefix());
        append_zip(&input_filepath)?;
    }

    Ok(())
}
