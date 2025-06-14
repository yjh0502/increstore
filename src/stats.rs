use crate::db::Blob;
use bytesize::ByteSize;
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

    pub children_indices: Vec<usize>,
    pub alias_indices: Vec<usize>,
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

    pub fn child_count(&self, idx: usize) -> usize {
        self.depths[idx].child_count
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
        self.depths[idx].alias_indices.clone()
    }

    /// TODO: for graphviz
    pub fn node_name(&self, idx: usize) -> String {
        let aliases = self.aliases(idx);
        for blob_idx in aliases {
            let blob = &self.blobs[blob_idx];
            if blob.is_root() {
                return format!("V{}", blob.id);
            }
        }
        let blob = &self.blobs[idx];
        format!("V{}", blob.id)
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

        for child_idx in &self.depths[idx].children_indices {
            // excludes children with full blob alias
            if !include_root {
                let aliases = self.aliases(*child_idx);
                if aliases
                    .into_iter()
                    .find(|idx| self.blobs[*idx].is_root())
                    .is_some()
                {
                    continue;
                }
            }

            children.push(*child_idx);
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

    pub fn spine(&self) -> Vec<usize> {
        // TODO: genesis
        let mut spine_idx = 0;
        let mut spine = Vec::new();
        loop {
            spine.push(spine_idx);
            let children = self.children_all(spine_idx);
            if children.is_empty() {
                break;
            }

            // for debugging
            let mut candidates = Vec::new();

            let mut max_child_count = 0;
            let mut max_child_idx = 0;
            let mut update_child = |idx: usize| {
                let child_count = self.depths[idx].child_count;
                if child_count > max_child_count {
                    max_child_count = child_count;
                    max_child_idx = idx;
                }

                candidates.push((self.node_name(idx), child_count));
            };

            for child_idx in children {
                update_child(child_idx);
                for alias_idx in self.aliases(child_idx) {
                    update_child(alias_idx);
                }
            }
            trace!("candidates={:?}", candidates);
            spine_idx = max_child_idx;
        }
        return spine;
    }

    pub fn size_info(&self) -> String {
        use std::fmt::Write;

        let mut s = String::new();

        // stats
        {
            writeln!(s, "## stats").ok();
            writeln!(
                s,
                "  total count={}, size={}, dehydrated={}",
                self.root_count + self.non_root_count,
                ByteSize(self.root_total_size + self.non_root_store_size),
                ByteSize(self.blobs[0].store_size + self.non_root_store_size),
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
            let mut hist = Histogram::default();
            let mut max_depth = 0;
            for depth in &self.depths {
                hist.add(depth.depth);
                max_depth = max_depth.max(depth.depth);
            }

            writeln!(s, "## depth distribution (max={})", max_depth).ok();
            writeln!(s, "{}", hist.print()).ok();

            let mut hist_size = Histogram::default();
            for blob in &self.blobs {
                if blob.is_root() {
                    continue;
                }

                hist_size.add(blob.store_size as usize);
            }

            writeln!(s, "## size distribution").ok();
            writeln!(s, "{}", hist_size.print()).ok();
        }

        s
    }
}

#[derive(Default)]
struct Histogram {
    bucket: Vec<usize>,
}
impl Histogram {
    fn add(&mut self, val: usize) {
        let bucket_idx = val.next_power_of_two().trailing_zeros() as usize;
        while self.bucket.len() <= bucket_idx {
            self.bucket.push(0);
        }
        self.bucket[bucket_idx] += 1;
    }

    fn print(&self) -> String {
        use std::fmt::Write;

        let mut s = String::new();
        let mut trim_start = true;

        for (i, count) in self.bucket.iter().enumerate() {
            let count = *count;
            if trim_start && count == 0 {
                continue;
            }
            trim_start = false;
            let (start, end) = if i == 0 {
                (0, 0)
            } else {
                (1 << (i - 1), (1 << i) - 1)
            };
            writeln!(
                s,
                "{:>9} - {:>9}| {}",
                format!("{}", ByteSize(start)),
                format!("{}", ByteSize(end)),
                count
            )
            .ok();
        }
        s
    }
}

fn calculate_depth(idx: usize, blobs: &[Blob], depths: &mut [GraphNode]) {
    let blob = &blobs[idx];

    match blob.parent_hash {
        None => {
            depths[idx].depth = 1;
        }

        Some(ref parent_hash) => {
            let mut min_depth = blobs.len();
            let mut min_idx = 0;

            for (other_idx, other) in blobs.iter().enumerate() {
                // aliases
                if other.content_hash == blob.content_hash {
                    depths[idx].alias_indices.push(other_idx);
                    depths[other_idx].alias_indices.push(idx);
                }
                if other_idx == idx {
                    continue;
                }

                let parent_idx = other_idx;
                let parent = other;

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

                depths[parent_idx].children_indices.push(idx);
            }

            trace!("{}={}", idx, min_depth + 1);
            depths[idx].depth = min_depth + 1;
            depths[idx].parent_idx = Some(min_idx);
        }
    }
}
