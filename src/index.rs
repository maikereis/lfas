use crate::DocId;
use crate::postings::Postings;
use crate::storage::PostingsStorage;
use roaring::RoaringBitmap;
use std::collections::HashMap;
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

        postings.add_occurrence(id);

        self.storage.put(field, term, postings).unwrap();
    }

    pub fn add_batch(&mut self, batch: Vec<(DocId, Vec<(F, String)>)>) {
        // We aggregate all the terms of the batch into memory first.
        // This avoids the constant Get-Modify-Put in LMDB.
        let mut temp_map: HashMap<(F, String), Postings> = HashMap::new();

        for (id, fields) in batch {
            for (field, term) in fields {
                temp_map.entry((field, term))
                    .or_insert_with(Postings::new)
                    .add_occurrence(id);
            }
        }

        for ((field, term), batch_postings) in temp_map {
            let mut existing_postings = self.storage
                .get(field, &term)
                .unwrap_or_default()
                .unwrap_or_else(Postings::new);
                
            existing_postings.merge(batch_postings);
            
            self.storage.put(field, term, existing_postings).unwrap();
        }
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
            .map(|p| p.bitmap().clone())
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
