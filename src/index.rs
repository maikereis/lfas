use crate::DocId;
use crate::postings::Postings;
use roaring::RoaringBitmap;
use std::collections::BTreeMap;
use std::hash::Hash;

pub struct InvertedIndex<F>
where
    F: Hash + Eq + Clone,
{
    pub postings: BTreeMap<(F, String), Postings>,
}

impl<F> InvertedIndex<F>
where
    F: Hash + Eq + Clone + Ord,
{
    pub fn new() -> Self {
        Self {
            postings: BTreeMap::new(),
        }
    }

    pub fn add_term(&mut self, id: DocId, field: F, term: String) {
        // Correctly update both bitmap and frequencies
        self.postings
            .entry((field, term))
            .or_insert_with(Postings::new)
            .add_doc(id);
    }

    /// Returns a reference to the full Postings (including frequency map)
    pub fn get_postings(&self, field: F, term: &str) -> Option<&Postings> {
        self.postings.get(&(field, term.to_string()))
    }

    /// Set operations return only the bitmap to avoid hashmap overhead
    pub fn intersect_terms(&self, field: F, terms: &[&str]) -> RoaringBitmap {
        let mut result = RoaringBitmap::new();
        let mut first = true;

        for term in terms {
            if let Some(p) = self.get_postings(field.clone(), term) {
                if first {
                    result = p.bitmap.clone();
                    first = false;
                } else {
                    result &= &p.bitmap;
                }
            } else {
                // In an AND query, if one term is missing, the intersection is empty.
                return RoaringBitmap::new();
            }
        }
        result
    }

    /// Returns a bitmap of DocIds present in ANY of the terms for this field.
    pub fn union_terms(&self, field: F, terms: &[&str]) -> RoaringBitmap {
        let mut result = RoaringBitmap::new();
        for term in terms {
            if let Some(p) = self.get_postings(field.clone(), term) {
                result |= &p.bitmap;
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tokenizer::tokenize;
    // We import this to use the .iter() or .to_vec() methods if needed

    #[derive(Hash, Eq, PartialEq, Clone, Copy, Ord, PartialOrd, Debug)]
    enum AddressField {
        Street,
        Municipality,
        Number,
    }

    #[test]
    fn test_address_field_inverted_index() {
        let mut idx = InvertedIndex::<AddressField>::new();

        // Document 1: A specific address in Belém
        let doc1_id = 1;
        let addr1 = [
            (AddressField::Street, "Travessa Mauriti"),
            (AddressField::Municipality, "Belém"),
            (AddressField::Number, "100"),
        ];

        for (field, text) in addr1 {
            for token in tokenize(text) {
                idx.add_term(doc1_id, field, token);
            }
        }

        // Document 2: A similar street but different city/number
        let doc2_id = 2;
        let addr2 = [
            (AddressField::Street, "Avenida Mauriti"),
            (AddressField::Municipality, "Santarém"),
            (AddressField::Number, "500"),
        ];

        for (field, text) in addr2 {
            for token in tokenize(text) {
                idx.add_term(doc2_id, field, token);
            }
        }

        // Assert: "mauriti" should exist in the Street field for both documents
        let street_postings = idx
            .get_postings(AddressField::Street, "mauriti")
            .expect("Term not found");
        // RoaringBitmap contains u32, so we check inclusion
        assert!(street_postings.contains(1));
        assert!(street_postings.contains(2));
        assert_eq!(street_postings.len(), 2);

        // Assert: "belem" should ONLY exist in the Municipality field for Doc 1
        let city_postings = idx
            .get_postings(AddressField::Municipality, "belem")
            .expect("Term not found");
        assert!(city_postings.contains(1));
        assert_eq!(city_postings.len(), 1);

        // Assert: Searching for "belem" in the Street field should return None
        let wrong_field = idx.get_postings(AddressField::Street, "belem");
        assert!(wrong_field.is_none());
    }

    #[test]
    fn test_set_operations() {
        let mut idx = InvertedIndex::<AddressField>::new();

        let doc1_id = 1;
        for token in tokenize("Travessa Mauriti") {
            idx.add_term(doc1_id, AddressField::Street, token);
        }
        for token in tokenize("Belém") {
            idx.add_term(doc1_id, AddressField::Municipality, token);
        }

        let doc2_id = 2;
        for token in tokenize("Avenida Mauriti") {
            idx.add_term(doc2_id, AddressField::Street, token);
        }
        for token in tokenize("Santarém") {
            idx.add_term(doc2_id, AddressField::Municipality, token);
        }

        let intersection = idx.intersect_terms(AddressField::Street, &["avenida", "mauriti"]);
        assert!(intersection.contains(2));
        assert!(!intersection.contains(1));
        assert_eq!(intersection.len(), 1);

        let union = idx.union_terms(AddressField::Municipality, &["belem", "santarem"]);
        assert!(union.contains(1));
        assert!(union.contains(2));
        assert_eq!(union.len(), 2);

        let no_match = idx.intersect_terms(AddressField::Street, &["travessa", "santarem"]);
        assert!(no_match.is_empty());
    }
}
