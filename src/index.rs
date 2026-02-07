use crate::DocId;
use crate::postings::Postings;
use crate::storage::PostingsStorage;
use roaring::RoaringBitmap;
use std::hash::Hash;
use std::marker::PhantomData;

pub struct InvertedIndex<F, S>
where
    F: Hash + Eq + Clone + Ord + Copy,
    S: PostingsStorage<F>,
{
    pub storage: S,
    _phantom: PhantomData<F>,
}

impl<F, S> InvertedIndex<F, S>
where
    F: Hash + Eq + Clone + Ord + Copy,
    S: PostingsStorage<F>,
{
    pub fn new(storage: S) -> Self {
        Self {
            storage,
            _phantom: PhantomData,
        }
    }

    pub fn add_term(&mut self, id: DocId, field: F, term: String) {
        let mut postings = self
            .storage
            .get(field, &term)
            .unwrap_or_default()
            .unwrap_or_else(Postings::new);

        postings.add_doc(id);

        self.storage.put(field, term, postings).unwrap();
    }

    pub fn get_postings(&self, field: F, term: &str) -> Option<Postings> {
        use log::debug;
        let result = self.storage.get(field, term).ok().flatten();
        if let Some(ref postings) = result {
            debug!("[INDEX] Found {} docs for term '{}'", postings.len(), term);
        }
        result
    }

    pub fn term_bitmap(&self, field: F, term: &str) -> RoaringBitmap {
        self.get_postings(field, term)
            .map(|p| p.bitmap)
            .unwrap_or_else(RoaringBitmap::new)
    }

    pub fn intersect(bitmaps: &[RoaringBitmap]) -> RoaringBitmap {
        if bitmaps.is_empty() {
            return RoaringBitmap::new();
        }

        let mut iter = bitmaps.iter();
        let mut result = iter.next().unwrap().clone();
        for bm in iter {
            result &= bm;
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
    use crate::storage::InMemoryStorage;
    use crate::tokenizer::tokenize;

    #[derive(Hash, Eq, PartialEq, Clone, Copy, Ord, PartialOrd, Debug)]
    enum AddressField {
        Street,
        Municipality,
    }

    #[test]
    fn test_address_field_inverted_index() {
        let storage = InMemoryStorage::new();
        let mut idx = InvertedIndex::<AddressField, InMemoryStorage<AddressField>>::new(storage);

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
        let storage = InMemoryStorage::new();
        let mut idx = InvertedIndex::<AddressField, InMemoryStorage<AddressField>>::new(storage);

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
        let intersection =
            InvertedIndex::<AddressField, InMemoryStorage<AddressField>>::intersect(&[bm1, bm2]);

        assert!(intersection.contains(2));
        assert!(!intersection.contains(1));

        // 2. Intra-field Union (Municipality: belem OR santarem)
        let bm3 = idx.term_bitmap(AddressField::Municipality, "belem");
        let bm4 = idx.term_bitmap(AddressField::Municipality, "santarem");
        let union =
            InvertedIndex::<AddressField, InMemoryStorage<AddressField>>::union(&[bm3, bm4]);

        assert_eq!(union.len(), 2);

        // 3. Inter-field Intersection (Street: mauriti AND Municipality: belem)
        let bm_mauriti = idx.term_bitmap(AddressField::Street, "mauriti");
        let bm_belem = idx.term_bitmap(AddressField::Municipality, "belem");
        let inter_field =
            InvertedIndex::<AddressField, InMemoryStorage<AddressField>>::intersect(&[
                bm_mauriti, bm_belem,
            ]);

        assert!(inter_field.contains(1));
        assert!(!inter_field.contains(2));
        assert_eq!(inter_field.len(), 1);
    }
}
