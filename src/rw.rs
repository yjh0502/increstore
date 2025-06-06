use std::io;

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

    pub fn append(&mut self, buf: &[u8]) {
        self.hash0.append(buf);
        self.size += buf.len() as u64;
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
                self.meta.append(&buf[..n]);
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
                self.meta.append(&buf[..n]);
                Ok(n)
            }
            Err(e) => Err(e),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.w.flush()
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
