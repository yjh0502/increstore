use crate::db::Blob;

#[derive(Default)]
pub struct Stats {
    root_count: usize,
    root_total_size: u64,

    non_root_count: usize,
    non_root_store_size: u64,
    non_root_content_size: u64,

    // depths
    depths: Vec<usize>,
}

impl Stats {
    pub fn from_blobs(blobs: &[Blob]) -> Self {
        let mut stats = Stats::default();

        for blob in blobs {
            stats.add_blob(blob);
        }

        stats.depths = Vec::with_capacity(blobs.len());
        stats.depths.resize(blobs.len(), 0);

        for i in 0..blobs.len() {
            calculate_depth(i, &blobs, &mut stats.depths);
        }

        stats
    }

    fn add_blob(&mut self, blob: &Blob) {
        match &blob.parent_hash {
            None => {
                self.root_count += 1;
                self.root_total_size += blob.content_size;
            }
            Some(_parent_hash) => {
                self.non_root_count += 1;
                self.non_root_store_size += blob.store_size;
                self.non_root_content_size += blob.content_size;
            }
        }
    }

    pub fn size_info(&self) -> String {
        use bytesize::ByteSize;
        use std::fmt::Write;

        let mut s = String::new();
        writeln!(
            s,
            "total count={}, size={}",
            self.root_count + self.non_root_count,
            ByteSize(self.root_total_size + self.non_root_store_size)
        )
        .ok();

        writeln!(
            s,
            "root count={}, size={}, avg={}",
            self.root_count,
            ByteSize(self.root_total_size),
            ByteSize(self.root_total_size / self.root_count as u64)
        )
        .ok();

        let compression_ratio =
            (self.non_root_store_size as f32) * 100.0 / (self.non_root_content_size as f32);

        writeln!(
            s,
            "non_root count={}, store_size={}, content_size={}, compression={:.2}% ({:.2}x)",
            self.non_root_count,
            ByteSize(self.non_root_store_size),
            ByteSize(self.non_root_content_size),
            compression_ratio,
            100.0 / compression_ratio
        )
        .ok();

        let len = self.depths.len();

        let bucket_size = (len.next_power_of_two().trailing_zeros() as usize) + 1;
        let mut bucket = Vec::with_capacity(bucket_size);
        bucket.resize(bucket_size, 0);

        for i in 0..len {
            let depth = self.depths[i];
            let bucket_idx = depth.next_power_of_two().trailing_zeros() as usize;
            bucket[bucket_idx] += 1;
        }

        while let Some(0) = bucket.last().clone() {
            bucket.pop();
        }

        writeln!(s, "## depth destribution").ok();
        for (i, count) in bucket.into_iter().enumerate() {
            let (start, end) = if i == 0 {
                (0, 0)
            } else {
                (1 << (i - 1), (1 << i) - 1)
            };
            writeln!(s, "{:3}~{:3} = {}", start, end, count).ok();
        }

        s
    }
}

fn calculate_depth(idx: usize, blobs: &[Blob], depths: &mut [usize]) -> usize {
    let blob = &blobs[idx];

    match blob.parent_hash {
        None => {
            depths[idx] = 1;
            1
        }
        Some(ref parent_hash) => {
            let mut min_depth = blobs.len();

            for (parent_idx, parent) in blobs.iter().enumerate() {
                if parent_idx == idx {
                    continue;
                }
                if &parent.content_hash != parent_hash {
                    continue;
                }

                let depth = if depths[parent_idx] == 0 {
                    calculate_depth(parent_idx, blobs, depths)
                } else {
                    depths[parent_idx]
                };
                if depth < min_depth {
                    min_depth = depth;
                }
            }
            trace!("{}={}", idx, min_depth + 1);
            depths[idx] = min_depth + 1;
            min_depth
        }
    }
}
