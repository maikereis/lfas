use serde::{Deserialize, Serialize};

use crate::DocId;
use std::collections::HashMap;
use std::hash::Hash;

/// Keeps track of document lengths and global field stats.
#[derive(Serialize, Deserialize)]
pub struct FieldMetadata<F> 
where 
    F: Hash + Eq + Clone
{
    /// doc_id -> field -> length
    pub lengths: HashMap<DocId, HashMap<F, usize>>,
    /// field -> total_tokens_in_corpus (for avgdl calculation)
    pub total_field_lengths: HashMap<F, usize>,
    /// Total number of documents in the index
    pub total_docs: usize,
    /// Document frequency: (field, term) -> count
    pub term_df: HashMap<(F, String), usize>,
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

impl<F> Default for FieldMetadata<F>
where
    F: Hash + Eq + Clone,
{
    fn default() -> Self {
        Self::new()
    }
}
