use crate::postings::Postings;
use crate::{DocId, index::InvertedIndex, metadata::FieldMetadata, storage::PostingsStorage};
use roaring::RoaringBitmap;
use std::collections::HashMap;

pub struct BM25FScorer<F> {
    pub k1: f32,
    pub field_weights: HashMap<F, f32>,
    pub field_b: HashMap<F, f32>,
}

impl<F> BM25FScorer<F>
where
    F: std::hash::Hash + Eq + Clone + Copy + Ord,
{
    pub fn score<S>(
        &self,
        matches: RoaringBitmap,
        query_tokens: &[(F, String)],
        index: &InvertedIndex<F, S>,
        metadata: &FieldMetadata<F>,
    ) -> Vec<(DocId, f32)>
    where
        S: PostingsStorage<F>,
    {
        self.score_taat_cached(matches, query_tokens, index, metadata)
    }

    /// Score term-at-a-time with BATCH transaction optimization
    fn score_taat_cached<S>(
        &self,
        candidates: RoaringBitmap,
        query_tokens: &[(F, String)],
        index: &InvertedIndex<F, S>,
        metadata: &FieldMetadata<F>,
    ) -> Vec<(DocId, f32)>
    where
        S: PostingsStorage<F>,
    {
        use crate::timing::Timer;
        use log::{debug, info};

        let cache_timer = Timer::new("term-at-a-time::cache_postings");
        
        // Use batch operation with single transaction
        let query_list: Vec<(F, String)> = query_tokens.iter()
            .map(|(f, t)| (*f, t.clone()))
            .collect();
        
        let mut postings_cache: HashMap<(F, String), Postings> = HashMap::new();
        
        // Try batch operation first (works for LMDB)
        match index.storage.get_batch(&query_list) {
            Ok(results) => {
                info!("[SCORER] Using BATCH operation - single transaction for {} terms", query_list.len());
                for (query, postings_opt) in query_list.iter().zip(results) {
                    if let Some(postings) = postings_opt {
                        postings_cache.insert(query.clone(), postings);
                    }
                }
            }
            Err(_) => {
                // Fallback for storage types without batch support
                info!("[SCORER] Batch failed, falling back to individual gets");
                for (field, term) in query_tokens {
                    if let Some(postings) = index.get_postings(*field, term) {
                        postings_cache.insert((*field, term.clone()), postings);
                    }
                }
            }
        }
        
        drop(cache_timer);
        info!("[SCORER] Cached {} postings in memory", postings_cache.len());

        let avg_timer = Timer::new("term-at-a-time::precompute");
        let avg_lengths = self.calculate_avg_lengths(metadata);
        let mut idf_cache: HashMap<(F, String), f32> = HashMap::new();
        for (field, term) in query_tokens {
            let key = (*field, term.clone());
            let idf = self.calculate_idf(term, *field, metadata);
            idf_cache.insert(key, idf);
        }
        
        drop(avg_timer);
        debug!("[SCORER] Precomputed {} IDF values", idf_cache.len());

        // Score accumulator - only allocate for candidates
        let score_timer = Timer::new("term-at-a-time::accumulate_scores");
        let mut accumulators: HashMap<DocId, f32> = HashMap::new();
        
        let mut term_hits = 0u64;
        let mut term_misses = 0u64;

        // For each term, update scores of ALL matching candidates at once
        for (field, term) in query_tokens {
            let key = (*field, term.clone());
            
            let Some(postings) = postings_cache.get(&key) else {
                term_misses += candidates.len() as u64;
                continue;
            };
            
            term_hits += 1;
            
            // Get precomputed values for this term
            let idf = idf_cache.get(&key).unwrap_or(&0.0);
            let weight = *self.field_weights.get(field).unwrap_or(&1.0);
            let b = *self.field_b.get(field).unwrap_or(&0.75);
            let avgdl = *avg_lengths.get(field).unwrap_or(&1.0);
            
            // Iterate through posting list once, update all matching candidates
            for doc_id in postings.bitmap().iter() {
                let doc_id = doc_id as usize;
                
                // Skip if not in candidate set
                if !candidates.contains(doc_id as u32) {
                    continue;
                }
                
                // Get term frequency from cached posting
                let tf = *postings.frequencies().get(&doc_id).unwrap_or(&0);
                
                // Get document length (this is in-memory metadata)
                let dl = *metadata.lengths
                    .get(&doc_id)
                    .and_then(|fields| fields.get(field))
                    .unwrap_or(&0) as f32;
                
                // BM25F calculation
                let weighted_tf = (tf as f32 * weight) / (1.0 + b * (dl / avgdl - 1.0));
                let contribution = idf * (weighted_tf / (self.k1 + weighted_tf));
                
                // Accumulate score
                *accumulators.entry(doc_id).or_insert(0.0) += contribution;
            }
        }
        
        drop(score_timer);
        
        debug!(
            "[SCORER] Stats: {} term hits, {} term misses",
            term_hits, term_misses
        );
        
        info!("[SCORER] Accumulated scores for {} documents", accumulators.len());

        // Sort results
        let sort_timer = Timer::new("term-at-a-time::sort_results");
        let mut scores: Vec<_> = accumulators.into_iter().collect();
        scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        drop(sort_timer);

        if !scores.is_empty() {
            info!(
                "[SCORER] Complete: {} documents scored, top: {:.4}, median: {:.4}, bottom: {:.4}",
                scores.len(),
                scores.first().map(|(_, s)| *s).unwrap_or(0.0),
                scores.get(scores.len() / 2).map(|(_, s)| *s).unwrap_or(0.0),
                scores.last().map(|(_, s)| *s).unwrap_or(0.0)
            );
        }

        scores
    }

    fn calculate_avg_lengths(
        &self,
        metadata: &FieldMetadata<F>,
    ) -> std::collections::HashMap<F, f32> {
        metadata
            .total_field_lengths
            .iter()
            .map(|(&f, &total)| (f, total as f32 / metadata.total_docs as f32))
            .collect()
    }

    fn calculate_idf(
        &self, 
        term: &str, 
        field: F, 
        metadata: &FieldMetadata<F>
    ) -> f32 {
        // O(1) Lookup replaced the expensive storage iteration
        let df = metadata.get_df(&field, term) as f32;
        let total_docs = metadata.total_docs as f32;

        // Standard BM25 IDF formula
        ((total_docs - df + 0.5) / (df + 0.5) + 1.0).ln()
    }
}
