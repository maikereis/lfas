use crate::DocId;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Postings {
    bitmap: RoaringBitmap,
    /// Term Frequency: How many times a term appears in a specific document.
    frequencies: HashMap<DocId, u32>,
}

impl Postings {
    pub fn new() -> Self {
        Self {
            bitmap: RoaringBitmap::new(),
            frequencies: HashMap::new(),
        }
    }

    /// Records an occurrence of a term in a document.
    pub fn add_occurrence(&mut self, doc_id: DocId) {
        self.bitmap.insert(doc_id as u32);
        *self.frequencies.entry(doc_id).or_insert(0) += 1;
    }

    /// Merges another Postings list into this one (useful for parallel indexing).
    pub fn merge(&mut self, other: Postings) {
        self.bitmap |= other.bitmap;
        for (doc_id, count) in other.frequencies {
            *self.frequencies.entry(doc_id).or_insert(0) += count;
        }
    }

    pub fn bitmap(&self) -> &RoaringBitmap {
        &self.bitmap
    }

    pub fn term_frequency(&self, doc_id: DocId) -> u32 {
        *self.frequencies.get(&doc_id).unwrap_or(&0)
    }

    pub fn frequencies(&self) -> &HashMap<DocId, u32> {
        &self.frequencies
    }

    pub fn contains(&self, doc_id: DocId) -> bool {
        self.bitmap.contains(doc_id as u32)
    }

    pub fn len(&self) -> usize {
        self.bitmap.len() as usize
    }

    pub fn is_empty(&self) -> bool {
        self.bitmap.is_empty()
    }
}

impl Default for Postings {
    fn default() -> Self {
        Self::new()
    }
}
