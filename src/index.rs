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
        self.postings
            .entry((field, term))
            .or_insert_with(Postings::new)
            .add_doc(id);
    }

    pub fn get_postings(&self, field: F, term: &str) -> Option<&Postings> {
        self.postings.get(&(field, term.to_string()))
    }

    pub fn term_bitmap(&self, field: F, term: &str) -> RoaringBitmap {
        self.get_postings(field, term)
            .map(|p| p.bitmap.clone())
            .unwrap_or_else(RoaringBitmap::new)
    }

    pub fn intersect(bitmaps: &[RoaringBitmap]) -> RoaringBitmap {
        if bitmaps.is_empty() {
            return RoaringBitmap::new();
        }

        let mut iter = bitmaps.iter();
        let mut result = iter.next().unwrap().clone();
        for bm in iter {
            result &= bm; // Note: RoaringBitmap uses &= &bm internally
        }
        result
    }

    pub fn union(bitmaps: &[RoaringBitmap]) -> RoaringBitmap {
        let mut result = RoaringBitmap::new();
        for bm in bitmaps {
            result |= bm;
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tokenizer::tokenize;

    #[derive(Hash, Eq, PartialEq, Clone, Copy, Ord, PartialOrd, Debug)]
    enum AddressField {
        Street,
        Municipality,
    }

    #[test]
    fn test_address_field_inverted_index() {
        let mut idx = InvertedIndex::<AddressField>::new();

        let doc1_id = 1;
        let addr1 = [
            (AddressField::Street, "Travessa Mauriti"),
            (AddressField::Municipality, "Belém"),
        ];

        for (field, text) in addr1 {
            for token in tokenize(text) {
                idx.add_term(doc1_id, field, token);
            }
        }

        let street_postings = idx
            .get_postings(AddressField::Street, "mauriti")
            .expect("Term not found");
        assert!(street_postings.contains(1));

        // Test Term Frequency (BM25 prerequisite)
        assert_eq!(*street_postings.frequencies.get(&1).unwrap(), 1);
    }

    #[test]
    fn test_generic_set_operations() {
        let mut idx = InvertedIndex::<AddressField>::new();

        // Doc 1: Travessa Mauriti, Belém
        idx.add_term(1, AddressField::Street, "travessa".to_string());
        idx.add_term(1, AddressField::Street, "mauriti".to_string());
        idx.add_term(1, AddressField::Municipality, "belem".to_string());

        // Doc 2: Avenida Mauriti, Santarém
        idx.add_term(2, AddressField::Street, "avenida".to_string());
        idx.add_term(2, AddressField::Street, "mauriti".to_string());
        idx.add_term(2, AddressField::Municipality, "santarem".to_string());

        // 1. Intra-field Intersection (Street: avenida AND mauriti)
        let bm1 = idx.term_bitmap(AddressField::Street, "avenida");
        let bm2 = idx.term_bitmap(AddressField::Street, "mauriti");
        let intersection = InvertedIndex::<AddressField>::intersect(&[bm1, bm2]);

        assert!(intersection.contains(2));
        assert!(!intersection.contains(1));

        // 2. Intra-field Union (Municipality: belem OR santarem)
        let bm3 = idx.term_bitmap(AddressField::Municipality, "belem");
        let bm4 = idx.term_bitmap(AddressField::Municipality, "santarem");
        let union = InvertedIndex::<AddressField>::union(&[bm3, bm4]);

        assert_eq!(union.len(), 2);

        // 3. Inter-field Intersection (Street: mauriti AND Municipality: belem)
        // This is where the generic approach shines!
        let bm_mauriti = idx.term_bitmap(AddressField::Street, "mauriti");
        let bm_belem = idx.term_bitmap(AddressField::Municipality, "belem");
        let inter_field = InvertedIndex::<AddressField>::intersect(&[bm_mauriti, bm_belem]);

        assert!(inter_field.contains(1));
        assert!(!inter_field.contains(2));
        assert_eq!(inter_field.len(), 1);
    }
}
