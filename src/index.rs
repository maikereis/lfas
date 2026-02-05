use std::borrow::Cow;
use std::collections::BTreeMap;

pub type DocId = usize;
pub type Term = String;

pub struct InvertedIndex {
    // A Map where:
    // Key = The Word (Term)
    // Value = A set of unique Document IDs containing that word
    pub index: BTreeMap<Term, Vec<DocId>>,
}


impl InvertedIndex {
    pub fn new() -> Self {
        Self { index: BTreeMap::new() }
    }

    pub fn add(&mut self, id: DocId, term: Cow<str>) {
        let entry = self.index.entry(term.into()).or_insert_with(Vec::new);

        // binary_search returns the index where the item SHOULD be if not found
        if let Err(pos) = entry.binary_search(&id){
            entry.insert(pos,id);
        }
    }

    pub fn get_postings(&self, term: &str) -> Option<&Vec<DocId>> {
        self.index.get(term)
    }

    pub fn intersect(a: &[DocId], b: &[DocId]) -> Vec<DocId> {
        let mut result = Vec::new();
        let (mut i, mut j) = (0,0);

        while i < a.len() && j < b.len() {
            if a[i]==b[j] {
                result.push(a[i]);
                i+=1;
                j+=1;
            } else if a[i] < b[j] {
                i+=1;
            } else {
                j+=1;
            }
        }
        result
    }

    pub fn union(a: &[DocId], b: &[DocId]) -> Vec<DocId> {
        let mut result = Vec::with_capacity(a.len() + b.len());
        let (mut i, mut j) = (0, 0);
        
        while i < a.len() || j < b.len() {
            if i < a.len() && (j == b.len() || a[i] < b[j]) {
                result.push(a[i]);
                i += 1;
            } else if j < b.len() && (i == a.len() || b[j] < a[i]) {
                result.push(b[j]);
                j += 1;
            } else {
                // Values are equal: push once and advance both to maintain uniqueness
                result.push(a[i]);
                i += 1;
                j += 1;
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tokenizer::tokenize;
    use std::borrow::Cow;

    #[test]
    fn test_full_indexing_and_search() {
        let mut idx = InvertedIndex::new();
        
        // Document 1: Mauriti in Belém
        let doc1_text = "Travessa Mauriti, Belém, Pará";
        for token in tokenize(doc1_text) {
            idx.add(1, Cow::Owned(token));
        }

        // Document 2: Mauriti in another city (hypothetical)
        let doc2_text = "Rua Mauriti, Santarém";
        for token in tokenize(doc2_text) {
            idx.add(2, Cow::Owned(token));
        }

        // 1. Check Retrieval
        let mauriti_postings = idx.get_postings("mauriti").expect("Term should exist");
        assert_eq!(mauriti_postings, &vec![1, 2]);

        // 2. Check AND (Mauriti AND Santarem)
        let santarem_postings = idx.get_postings("santarem").expect("Term should exist");
        let and_results = InvertedIndex::intersect(mauriti_postings, santarem_postings);
        assert_eq!(and_results, vec![2]);

        // 3. Check OR (Belem OR Santarem)
        let belem_postings = idx.get_postings("belem").expect("Term should exist");
        let or_results = InvertedIndex::union(belem_postings, santarem_postings);
        assert_eq!(or_results, vec![1, 2]);
        
        println!("Search Logic Verified!");
    }
}