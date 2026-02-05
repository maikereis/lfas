use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;

pub type DocId = usize;

/// Keeps track of document lengths and global field stats.
pub struct FieldMetadata<F> {
    // doc_id -> field -> length
    pub lengths: HashMap<DocId, HashMap<F, usize>>,
    // field -> total_tokens_in_corpus (for avgdl calculation)
    pub total_field_lengths: HashMap<F, usize>,
    pub total_docs: usize,
}

impl<F> FieldMetadata<F> where F: Hash + Eq + Clone {
    pub fn new() -> Self {
        Self {
            lengths: HashMap::new(),
            total_field_lengths: HashMap::new(),
            total_docs: 0,
        }
    }
}

/// Term -> (Field, DocId) -> Frequency
pub struct InvertedIndex<F> where F: Hash + Eq + Clone {
    // Composite key (Field, Term)
    // The value is a Vec of tuples: (DocId, TermFrequency)
    pub postings: BTreeMap<(F, String), Vec<(DocId, u32)>>,
}

impl<F> InvertedIndex<F> where F: Hash + Eq + Clone + Ord {
    pub fn new() -> Self {
        Self { postings: BTreeMap::new() }
    }

    pub fn add_term(&mut self, id: DocId, field: F, term: String) {
        let entry = self.postings.entry((field, term)).or_default();
        
        if let Some((last_id, count)) = entry.last_mut() {
            if *last_id == id {
                *count += 1;
                return;
            }
        }
        entry.push((id, 1));
    }

    /// Helper for tests to get only the DocIds for a term in a specific field
    pub fn get_postings(&self, field: F, term: &str) -> Option<Vec<DocId>> {
        self.postings.get(&(field, term.to_string()))
            .map(|list| list.iter().map(|(id, _freq)| *id).collect())
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    // Assuming your tokenizer is accessible here
    use crate::tokenizer::tokenize; 

    #[derive(Hash, Eq, PartialEq, Clone, Copy, Ord, PartialOrd, Debug)]
    enum AddressField {
        Street,
        Neighborhood,
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
        let street_postings = idx.get_postings(AddressField::Street, "mauriti").expect("Term not found");
        assert_eq!(street_postings, vec![1, 2]);

        // Assert: "belem" should ONLY exist in the Municipality field for Doc 1
        let city_postings = idx.get_postings(AddressField::Municipality, "belem").expect("Term not found");
        assert_eq!(city_postings, vec![1]);

        // Assert: Searching for "belem" in the Street field should return None
        let wrong_field = idx.get_postings(AddressField::Street, "belem");
        assert!(wrong_field.is_none());
    }

    #[test]
    fn test_field_metadata_tracking() {
        let mut meta = FieldMetadata::<AddressField>::new();
        let doc_id = 101;

        // Manually simulating the indexing process for metadata
        let fields = vec![
            (AddressField::Street, vec!["rua", "augusta"]),
            (AddressField::Neighborhood, vec!["consolação"]),
        ];

        meta.total_docs += 1;
        let doc_entry = meta.lengths.entry(doc_id).or_default();

        for (field, tokens) in fields {
            let len = tokens.len();
            doc_entry.insert(field, len);
            
            let total_field_len = meta.total_field_lengths.entry(field).or_insert(0);
            *total_field_len += len;
        }

        // Verify metadata integrity
        assert_eq!(meta.total_docs, 1);
        assert_eq!(meta.lengths[&doc_id][&AddressField::Street], 2);
        assert_eq!(meta.total_field_lengths[&AddressField::Neighborhood], 1);
    }
}