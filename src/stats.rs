use crate::db::Blob;
use log::*;

pub struct RootBlob<'a> {
    pub blob: &'a Blob,
    pub alias: &'a Blob,
    pub score: u64,
}

#[derive(Default)]
pub struct GraphNode {
    pub depth: usize,
    pub child_count: usize,
    pub parent_idx: Option<usize>,
}

#[derive(Default)]
pub struct Stats {
    root_count: usize,
    root_total_size: u64,

    non_root_count: usize,
    non_root_store_size: u64,
    non_root_content_size: u64,

    // depths
    pub blobs: Vec<Blob>,
    pub depths: Vec<GraphNode>,
}

impl Stats {
    pub fn from_blobs(blobs: Vec<Blob>) -> Self {
        let mut stats = Stats::default();
        let len = blobs.len();

        for blob in &blobs {
            stats.add_blob(blob);
        }

        stats.depths = Vec::with_capacity(blobs.len());
        stats.depths.resize_with(blobs.len(), Default::default);
        stats.blobs = blobs;

        for i in 0..len {
            calculate_depth(i, &stats.blobs, &mut stats.depths);
        }

        for i in 0..len {
            stats.add_child_count(i);
        }

        stats
    }

    fn add_child_count(&mut self, idx: usize) {
        self.depths[idx].child_count += 1;
        if let Some(parent_idx) = self.depths[idx].parent_idx {
            self.add_child_count(parent_idx);
        } else {
            for alias_idx in self.aliases(idx) {
                self.add_child_count(alias_idx);
            }
        }
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

    fn root_age(&self, root_idx: usize) -> usize {
        let max_idx = self.blobs.len();
        let last_idx = self
            .children(root_idx, true)
            .into_iter()
            .max()
            .unwrap_or(root_idx);

        max_idx - last_idx
    }

    /// heuristic cost saving of the blob
    /// criteria 1: estimated space saving from root blob: store_size * children_count
    /// criteria 2: blob diversity
    pub fn root_score(&self, root_idx: usize) -> u64 {
        let mut aliases = self.aliases(root_idx);
        match aliases.pop() {
            None => u64::max_value(),
            Some(alias_idx) => {
                let alias = &self.blobs[alias_idx];

                let max_unused_age = 100;
                let age = (self.root_age(root_idx) as u64).min(max_unused_age);

                return alias.store_size * (max_unused_age - age) / max_unused_age;
                /*
                let len = self.children(root_idx).len();
                let multiplier = (len as f32).sqrt().ceil() as u64 + 1;
                return alias.store_size * multiplier;
                let saving_score = (alias.content_size - alias.store_size) * multiplier as u64;

                // TODO: eval?
                let diversity_score = alias.store_size * 100;

                saving_score + diversity_score
                */
            }
        }
    }

    /// TODO: fix name
    pub fn aliases(&self, idx: usize) -> Vec<usize> {
        let blob0 = &self.blobs[idx];
        let mut aliases = Vec::new();
        for (blob_idx, blob) in self.blobs.iter().enumerate() {
            if blob.store_hash == blob0.store_hash {
                continue;
            }
            if blob.content_hash == blob0.content_hash {
                aliases.push(blob_idx);
            }
        }
        aliases
    }

    /// TODO: for graphviz
    pub fn node_name(&self, idx: usize) -> String {
        let aliases = self.aliases(idx);
        for blob_idx in aliases {
            let blob = &self.blobs[blob_idx];
            if blob.is_root() {
                return format!("B{}", blob_idx);
            }
        }
        //
        format!("B{}", idx)
    }

    pub fn children_all(&self, idx: usize) -> Vec<usize> {
        let mut children = self.children(idx, true);
        for alias_idx in self.aliases(idx) {
            children.extend(self.children(alias_idx, true));
        }

        children.sort();
        children.dedup();
        return children;
    }

    pub fn children(&self, idx: usize, include_root: bool) -> Vec<usize> {
        let mut children = Vec::new();

        for (child_idx, _child) in self.blobs.iter().enumerate() {
            if Some(idx) != self.depths[child_idx].parent_idx {
                continue;
            }

            // excludes children with full blob alias
            if !include_root {
                let aliases = self.aliases(child_idx);
                if aliases
                    .into_iter()
                    .find(|idx| self.blobs[*idx].is_root())
                    .is_some()
                {
                    continue;
                }
            }

            children.push(child_idx);
        }
        children
    }

    //TODO: name
    pub fn root_candidates(&self) -> Vec<RootBlob> {
        let mut root_candidates = Vec::new();
        for (root_idx, root_blob) in self.blobs.iter().enumerate() {
            if root_blob.parent_hash.is_some() {
                continue;
            }

            let mut aliases = self.aliases(root_idx);
            if let Some(alias_idx) = aliases.pop() {
                let alias = &self.blobs[alias_idx];
                let score = self.root_score(root_idx);
                root_candidates.push(RootBlob {
                    blob: root_blob,
                    alias,
                    score,
                });
            }
        }

        root_candidates
    }

    pub fn size_info(&self) -> String {
        use bytesize::ByteSize;
        use std::fmt::Write;

        let mut s = String::new();

        // stats
        {
            writeln!(s, "## stats").ok();
            writeln!(
                s,
                "  total count={}, size={}",
                self.root_count + self.non_root_count,
                ByteSize(self.root_total_size + self.non_root_store_size)
            )
            .ok();

            writeln!(
                s,
                "  root count={}, size={}, avg={}",
                self.root_count,
                ByteSize(self.root_total_size),
                ByteSize(self.root_total_size / self.root_count as u64)
            )
            .ok();

            let compression_ratio =
                (self.non_root_store_size as f32) * 100.0 / (self.non_root_content_size as f32);

            writeln!(
                s,
                "  non_root count={}, store_size={}, content_size={}, avg={}, compression={:.2}% ({:.2}x)",
                self.non_root_count,
                ByteSize(self.non_root_store_size),
                ByteSize(self.non_root_content_size),
                ByteSize(self.non_root_store_size / self.non_root_count as u64),
                compression_ratio,
                100.0 / compression_ratio
            )
            .ok();
        }

        // root blobs
        {
            writeln!(s, "## root blobs").ok();
            for (idx, blob) in self.blobs.iter().enumerate() {
                if blob.is_root() {
                    let mut aliases = self.aliases(idx);
                    match aliases.pop() {
                        None => {
                            writeln!(
                                s,
                                "  blob idx={} content_size={} genesis",
                                idx,
                                ByteSize(blob.content_size)
                            )
                            .ok();
                        }
                        Some(alias_idx) => {
                            writeln!(
                                s,
                                "  blob idx={} age={} content_size={} ratio={:.2}% child_count={} score={}",
                                idx,
                                self.root_age(idx),
                                ByteSize(blob.content_size),
                                self.blobs[alias_idx].compression_ratio()*100.0,
                                self.children(idx, true).len(),
                                ByteSize(self.root_score(idx))
                            )
                            .ok();
                        }
                    }
                }
            }
        }

        // depth
        {
            let len = self.depths.len();

            let bucket_size = (len.next_power_of_two().trailing_zeros() as usize) + 1;
            let mut bucket = Vec::with_capacity(bucket_size);
            bucket.resize(bucket_size, 0);

            for i in 0..len {
                let depth = self.depths[i].depth;
                let bucket_idx = depth.next_power_of_two().trailing_zeros() as usize;
                bucket[bucket_idx] += 1;
            }

            while let Some(0) = bucket.last().clone() {
                bucket.pop();
            }

            writeln!(s, "## depth ditribution").ok();
            for (i, count) in bucket.into_iter().enumerate() {
                let (start, end) = if i == 0 {
                    (0, 0)
                } else {
                    (1 << (i - 1), (1 << i) - 1)
                };
                writeln!(s, "{:3}~{:3} = {}", start, end, count).ok();
            }
        }

        s
    }
}

fn calculate_depth(idx: usize, blobs: &[Blob], depths: &mut [GraphNode]) {
    let blob = &blobs[idx];

    match blob.parent_hash {
        None => {
            depths[idx] = GraphNode {
                depth: 1,
                child_count: 0,
                parent_idx: None,
            };
        }

        Some(ref parent_hash) => {
            let mut min_depth = blobs.len();
            let mut min_idx = 0;

            for (parent_idx, parent) in blobs.iter().enumerate() {
                if parent_idx == idx {
                    continue;
                }
                if &parent.content_hash != parent_hash {
                    continue;
                }

                if depths[parent_idx].depth == 0 {
                    calculate_depth(parent_idx, blobs, depths)
                }
                let depth = depths[parent_idx].depth;
                if depth < min_depth {
                    min_depth = depth;
                    min_idx = parent_idx;
                }
            }

            trace!("{}={}", idx, min_depth + 1);
            depths[idx] = GraphNode {
                depth: min_depth + 1,
                child_count: 0,
                parent_idx: Some(min_idx),
            };
        }
    }
}
