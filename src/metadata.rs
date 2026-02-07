use crate::DocId;
use std::collections::HashMap;
use std::hash::Hash;

/// Keeps track of document lengths and global field stats.
pub struct FieldMetadata<F> {
    // doc_id -> field -> length
    pub lengths: HashMap<DocId, HashMap<F, usize>>,
    // field -> total_tokens_in_corpus (for avgdl calculation)
    pub total_field_lengths: HashMap<F, usize>,
    pub total_docs: usize,
    pub term_df: HashMap<(F, String), usize>
}

impl<F> FieldMetadata<F>
where
    F: Hash + Eq + Clone,
{
    pub fn new() -> Self {
        Self {
            lengths: HashMap::new(),
            total_field_lengths: HashMap::new(),
            total_docs: 0,
            term_df: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::FieldMetadata;

    #[derive(Hash, Eq, PartialEq, Clone, Copy, Ord, PartialOrd, Debug)]
    enum AddressField {
        Street,
        Neighborhood,
    }

    #[test]
    fn test_field_metadata_tracking() {
        // FieldMetadata logic remains the same as it doesn't use the InvertedIndex bitmaps directly
        let mut meta = FieldMetadata::<AddressField>::new();
        let doc_id = 101;

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

        assert_eq!(meta.total_docs, 1);
        assert_eq!(meta.lengths[&doc_id][&AddressField::Street], 2);
        assert_eq!(meta.total_field_lengths[&AddressField::Neighborhood], 1);
    }
}
