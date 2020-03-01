use std::io;

use async_std::task::ready;

use super::db;

#[derive(Clone)]
pub struct WriteMetadata {
    size: u64,
    time_created: time::Timespec,
    hash: sha1::Sha1,
}

impl WriteMetadata {
    pub fn new() -> Self {
        Self {
            size: 0,
            time_created: time::now().to_timespec(),
            hash: sha1::Sha1::new(),
        }
    }

    pub fn blob(&self, filename: &str) -> db::Blob {
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

    pub fn digest(&self) -> sha1::Digest {
        self.hash.digest()
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
