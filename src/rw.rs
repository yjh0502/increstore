use std::io;

use async_std::task::ready;
use highway::*;

use super::db;

#[derive(Clone)]
pub struct WriteMetadata {
    size: u64,
    time_created: time::Timespec,

    // hash: sha1::Sha1,
    hash0: SseHash,
}

impl WriteMetadata {
    pub fn new() -> Self {
        // TODO
        let key = highway::Key([1, 2, 3, 4]);
        Self {
            size: 0,
            time_created: time::now().to_timespec(),
            // hash: sha1::Sha1::new(),
            hash0: SseHash::new(&key).unwrap(),
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
        use std::fmt::Write;
        let digest = self.hash0.clone().finalize256();
        let mut s = String::new();
        for val in &digest {
            write!(&mut s, "{:016x}", val).unwrap();
        }
        s
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
        // debug!("HashRw::write size={}", buf.len());
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

impl<W> async_std::io::Write for HashRW<W>
where
    W: async_std::io::Write + Unpin,
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
        // debug!("HashRw::poll_read size={}", buf.len());

        let mut s = self.as_mut();
        let w = Pin::new(&mut s.w);
        match ready!(w.poll_read(ctx, buf)) {
            Ok(n) => {
                // s.meta.hash.update(&buf[..n]);
                s.meta.hash0.append(&buf[..n]);
                Poll::Ready(Ok(n))
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
}

impl<W> Drop for RaceWrite<W> {
    fn drop(&mut self) {
        let mut value = self.race.load(Ordering::SeqCst);
        while value < self.size {
            self.race
                .compare_and_swap(value, self.size, Ordering::SeqCst);
            value = self.race.load(Ordering::SeqCst);
        }
    }
}

impl<W> async_std::io::Write for RaceWrite<W>
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
                s.size += n;
                let race_size = s.race.load(Ordering::SeqCst);
                if race_size == 0 || race_size <= s.size / 2 {
                    Poll::Ready(Ok(n))
                } else {
                    // TODO: use signal channel other than io::Error?
                    Poll::Ready(Err(io::Error::new(io::ErrorKind::TimedOut, "race")))
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

    fn poll_close(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<io::Result<()>> {
        let mut s = self.as_mut();
        s.race.store(s.size, Ordering::SeqCst);

        let w = Pin::new(&mut s.w);
        w.poll_close(ctx)
    }
}
