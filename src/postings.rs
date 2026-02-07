use crate::DocId;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Postings {
    pub bitmap: RoaringBitmap,
    /// Term Frequency statistic: DocId -> Count
    pub frequencies: HashMap<DocId, u32>,
}

impl Postings {
    pub fn new() -> Self {
        Self {
            bitmap: RoaringBitmap::new(),
            frequencies: HashMap::new(),
        }
    }

    pub fn add_doc(&mut self, doc_id: DocId) {
        self.bitmap.insert(doc_id as u32);
        *self.frequencies.entry(doc_id).or_insert(0) += 1;
    }

    pub fn contains(&self, doc_id: DocId) -> bool {
        self.bitmap.contains(doc_id as u32)
    }

    pub fn len(&self) -> usize {
        self.bitmap.len() as usize
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.bitmap.is_empty()
    }
}

impl Default for Postings {
    fn default() -> Self {
        Self::new()
    }
}
