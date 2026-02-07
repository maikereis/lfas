use crate::DocId;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Postings {
    pub bitmap: RoaringBitmap,
    // Term Frequency statistic: DocId -> Count
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
        // Manning statistics: increment local frequency
        *self.frequencies.entry(doc_id).or_insert(0) += 1;
    }

    pub fn contains(&self, doc_id: DocId) -> bool {
        self.bitmap.contains(doc_id as u32)
    }

    pub fn len(&self) -> usize {
        self.bitmap.len() as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_postings_is_empty() {
        let postings = Postings::new();
        assert_eq!(postings.len(), 0);
        assert!(postings.frequencies.is_empty());
    }

    #[test]
    fn test_add_single_doc() {
        let mut postings = Postings::new();
        let doc_id = 42;

        postings.add_doc(doc_id);

        assert!(postings.contains(doc_id));
        assert_eq!(postings.len(), 1);
        assert_eq!(postings.frequencies.get(&doc_id), Some(&1));
    }

    #[test]
    fn test_add_multiple_occurrences_same_doc() {
        let mut postings = Postings::new();
        let doc_id = 10;

        // Simulate the term appearing 3 times in the same document
        postings.add_doc(doc_id);
        postings.add_doc(doc_id);
        postings.add_doc(doc_id);

        // The bitmap tracks unique documents, so len should be 1
        assert_eq!(postings.len(), 1);
        // The frequency should correctly reflect 3 hits
        assert_eq!(postings.frequencies.get(&doc_id), Some(&3));
    }

    #[test]
    fn test_add_different_documents() {
        let mut postings = Postings::new();
        postings.add_doc(1);
        postings.add_doc(2);

        assert_eq!(postings.len(), 2);
        assert!(postings.contains(1));
        assert!(postings.contains(2));
        assert_eq!(postings.frequencies.len(), 2);
    }

    #[test]
    fn test_contains_non_existent_doc() {
        let postings = Postings::new();
        assert!(!postings.contains(999));
    }
}
