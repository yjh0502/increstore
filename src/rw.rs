use futures::ready;
use std::io;
use tokio::io::ReadBuf;

use highway::*;

use super::db;

#[derive(Clone)]
pub struct WriteMetadata {
    size: u64,
    time_created: time::OffsetDateTime,

    // hash: sha1::Sha1,
    hash0: SseHash,
}

fn digest_hex(bytes: [u64; 4]) -> String {
    use std::fmt::Write;

    let mut s = String::new();
    for val in &bytes {
        write!(&mut s, "{:016x}", val).unwrap();
    }
    s
}

impl WriteMetadata {
    pub fn new() -> Self {
        // TODO
        let key = highway::Key([1, 2, 3, 4]);
        Self {
            size: 0,
            time_created: time::OffsetDateTime::now_utc(),
            // hash: sha1::Sha1::new(),
            hash0: SseHash::new(key).unwrap(),
        }
    }

    pub fn blob(&self, filename: &str) -> db::Blob {
        let digest = self.digest();
        db::Blob {
            id: 0,
            filename: filename.to_owned(),
            time_created: self.time_created,
            store_size: self.size,
            content_size: self.size,
            store_hash: digest.clone(),
            content_hash: digest.clone(),
            parent_hash: None,
        }
    }

    pub fn digest(&self) -> String {
        let digest = self.hash0.clone().finalize256();
        digest_hex(digest)
    }

    pub fn len(&self) -> u64 {
        self.size
    }
}

pub struct HashRW<W> {
    meta: WriteMetadata,
    w: W,
}

impl<W> HashRW<W> {
    pub fn new(w: W) -> Self {
        HashRW {
            meta: WriteMetadata::new(),
            w,
        }
    }

    pub fn meta(&self) -> WriteMetadata {
        self.meta.clone()
    }
}

impl<W: io::Read> io::Read for HashRW<W> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // debug!("HashRw::write size={}", buf.len());
        match self.w.read(buf) {
            Ok(n) => {
                self.meta.size += n as u64;
                // self.meta.hash.update(&buf[..n]);
                self.meta.hash0.append(&buf[..n]);
                Ok(n)
            }
            Err(e) => Err(e),
        }
    }
}

impl<W: io::Write> io::Write for HashRW<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // debug!("HashRw::read size={}", buf.len());
        match self.w.write(buf) {
            Ok(n) => {
                self.meta.size += n as u64;
                // self.meta.hash.update(&buf[..n]);
                self.meta.hash0.append(&buf[..n]);
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

impl<W> tokio::io::AsyncWrite for HashRW<W>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        ctx: &mut Context,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        // debug!("HashRw::poll_write size={}", buf.len());

        let mut s = self.as_mut();
        let w = Pin::new(&mut s.w);
        match ready!(w.poll_write(ctx, buf)) {
            Ok(n) => {
                s.meta.size += n as u64;
                // s.meta.hash.update(&buf[..n]);
                s.meta.hash0.append(&buf[..n]);
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

    fn poll_shutdown(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<io::Result<()>> {
        let mut s = self.as_mut();
        let w = Pin::new(&mut s.w);
        w.poll_shutdown(ctx)
    }
}

impl<W> tokio::io::AsyncRead for HashRW<W>
where
    W: tokio::io::AsyncRead + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        ctx: &mut Context,
        buf: &mut ReadBuf,
    ) -> Poll<io::Result<()>> {
        // debug!("HashRw::poll_read size={}", buf.len());

        let mut s = self.as_mut();
        let w = Pin::new(&mut s.w);
        match ready!(w.poll_read(ctx, buf)) {
            Ok(()) => {
                s.meta.hash0.append(buf.filled());
                Poll::Ready(Ok(()))
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

pub struct RaceWrite<W> {
    race: Arc<AtomicUsize>,
    size: usize,
    w: W,
}

impl<W> RaceWrite<W> {
    pub fn new(w: W, race: Arc<AtomicUsize>) -> Self {
        Self { race, size: 0, w }
    }

    fn update_race(&mut self) {
        let ordering = Ordering::SeqCst;

        let mut value = self.race.load(ordering);
        while value < self.size {
            if let Err(loaded) = self
                .race
                .compare_exchange(value, self.size, ordering, ordering)
            {
                if loaded >= self.size {
                    break;
                } else {
                    value = loaded;
                }
            }
        }
    }
}

impl<W> Drop for RaceWrite<W> {
    fn drop(&mut self) {
        self.update_race()
    }
}

impl<W> std::io::Write for RaceWrite<W>
where
    W: std::io::Write,
{
    fn write(&mut self, buf: &[u8]) -> std::result::Result<usize, std::io::Error> {
        let race_size = self.race.load(Ordering::SeqCst);
        if race_size > 0 && race_size < self.size + buf.len() {
            return Err(io::Error::new(io::ErrorKind::Other, "race"));
        }

        match self.w.write(buf) {
            Ok(len) => {
                self.size += len;
                Ok(len)
            }
            Err(e) => Err(e),
        }
    }

    fn flush(&mut self) -> std::result::Result<(), std::io::Error> {
        self.w.flush()
    }
}

impl<W> tokio::io::AsyncWrite for RaceWrite<W>
where
    W: tokio::io::AsyncWrite + Unpin,
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
                s.size += n;
                let race_size = s.race.load(Ordering::SeqCst);
                if race_size == 0 || race_size > s.size {
                    Poll::Ready(Ok(n))
                } else {
                    // TODO: use signal channel other than io::Error?
                    Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, "race")))
                }
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<io::Result<()>> {
        let mut s = self.as_mut();
        let w = Pin::new(&mut s.w);
        w.poll_flush(ctx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<io::Result<()>> {
        let mut s = self.as_mut();

        s.update_race();

        let w = Pin::new(&mut s.w);
        w.poll_shutdown(ctx)
    }
}

pub struct MmapBuf {
    #[allow(unused)]
    file: std::fs::File,
    map: memmap::Mmap,
    offset: usize,
    len: usize,
}

impl MmapBuf {
    pub fn from_path<P: AsRef<std::path::Path>>(path: P) -> io::Result<Self> {
        let file = std::fs::File::open(path)?;
        let meta = file.metadata()?;
        let map = unsafe { memmap::Mmap::map(&file)? };

        Ok(Self {
            file,
            map,
            offset: 0,
            len: meta.len() as usize,
        })
    }

    fn remaining(&self) -> usize {
        self.len - self.offset
    }
}

impl tokio::io::AsyncRead for MmapBuf {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let len = buf.capacity().min(self.remaining());

        let offset = self.offset;
        let offset_end = self.offset + len;
        {
            let src = &self.map[offset..offset_end];
            buf.put_slice(src);
        }
        self.as_mut().offset = offset_end;

        Poll::Ready(Ok(()))
    }
}

pub struct MmapBufMut {
    #[allow(unused)]
    file: std::fs::File,
    map: memmap::MmapMut,
    offset: usize,
    len: usize,
}

impl MmapBufMut {
    #[allow(unused)]
    pub fn from_path_len<P: AsRef<std::path::Path>>(path: P, len: usize) -> io::Result<Self> {
        use std::fs::*;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        file.set_len(len as u64)?;

        let map = unsafe { memmap::MmapMut::map_mut(&file).expect("MmapMut::map_mut") };

        Ok(Self {
            file,
            map,
            offset: 0,
            len,
        })
    }

    fn remaining(&self) -> usize {
        self.len - self.offset
    }
}

impl tokio::io::AsyncWrite for MmapBufMut {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let len = buf.len().min(self.remaining());

        let offset = self.offset;
        let offset_end = self.offset + len;
        {
            let mut s = self.as_mut();
            (&mut s.map[offset..offset_end]).copy_from_slice(&buf[..len]);
            s.offset = offset_end;
        }

        Poll::Ready(Ok(len))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::io::*;

    #[test]
    fn hash_rw_ref() {
        let body = b"hello, world";

        let key = highway::Key([1, 2, 3, 4]);

        let mut hash = SseHash::new(key).unwrap();
        hash.append(&body[..]);
        let digest = hash.finalize256();

        assert_eq!(
            digest_hex(digest),
            "9be0f68afedc92f37c093966e0e2f9055cefa64b9567657a8af8f88eb280d6b2"
        );
    }

    #[test]
    fn hash_rw_write() {
        let body = b"hello, world";
        let mut rw = HashRW::new(Vec::new());

        rw.write_all(body).expect("failed to write");

        assert_eq!(
            rw.meta().digest(),
            "9be0f68afedc92f37c093966e0e2f9055cefa64b9567657a8af8f88eb280d6b2"
        );
    }

    #[test]
    fn hash_rw_read() {
        let body = b"hello, world";
        let mut dst = Vec::new();

        let mut rw = HashRW::new(&body[..]);

        rw.read_to_end(&mut dst).expect("failed to write");

        assert_eq!(
            rw.meta().digest(),
            "9be0f68afedc92f37c093966e0e2f9055cefa64b9567657a8af8f88eb280d6b2"
        );
    }

    #[test]
    fn race() {
        use std::io::Write;

        let shared = Arc::new(AtomicUsize::new(0));

        let mut r1 = RaceWrite::new(Vec::<u8>::new(), shared.clone());
        let mut r2 = RaceWrite::new(Vec::<u8>::new(), shared.clone());

        assert_eq!(shared.load(Ordering::SeqCst), 0);

        assert!(r1.write_all(&[1, 2, 3, 4]).is_ok());
        std::mem::drop(r1);

        assert_eq!(shared.load(Ordering::SeqCst), 4);

        assert!(r2.write_all(&[1, 2, 3, 4]).is_ok());

        let res = r2.write_all(&[1]);
        assert!(res.is_err());
        let e = res.err().unwrap();

        assert_eq!(e.kind(), io::ErrorKind::Other);

        assert_eq!(shared.load(Ordering::SeqCst), 4);
    }
}
