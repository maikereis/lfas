use crate::{DocId, index::InvertedIndex, metadata::FieldMetadata, storage::PostingsStorage};
use roaring::RoaringBitmap;
use std::collections::HashMap;

pub struct BM25FScorer<F> {
    pub k1: f32,
    pub field_weights: std::collections::HashMap<F, f32>,
    pub field_b: std::collections::HashMap<F, f32>,
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
        use crate::timing::Timer;
        use log::{debug, info};

        let _timer = Timer::new("BM25FScorer::score");

        let num_candidates = matches.len();
        let num_tokens = query_tokens.len();

        info!(
            "[SCORER] Starting scoring: {} candidates × {} query tokens = {} operations",
            num_candidates,
            num_tokens,
            num_candidates as u64 * num_tokens as u64
        );

        let avg_timer = Timer::new("calculate_avg_lengths");
        let avg_lengths = self.calculate_avg_lengths(metadata);
        drop(avg_timer);

        debug!(
            "[SCORER] Computed avg lengths for {} fields",
            avg_lengths.len()
        );

        let idf_timer = Timer::new("precalculate_idfs");
        let mut idf_cache = HashMap::new();
        for (field, term) in query_tokens {
            let key = (*field, term.as_str());
            if !idf_cache.contains_key(&key) {
                let idf = self.calculate_idf(term, *field, metadata);
                idf_cache.insert(key, idf);
            }
        }
        drop(idf_timer);

        debug!("[SCORER] Pre-calculated {} IDF values", idf_cache.len());

        let scoring_timer = Timer::new("score_documents");
        let mut scores = Vec::with_capacity(num_candidates as usize);

        let mut postings_hits = 0u64;
        let mut postings_misses = 0u64;
        let mut freq_hits = 0u64;
        let mut freq_misses = 0u64;

        for doc_id_u32 in matches.iter() {
            let doc_id = doc_id_u32 as usize;
            let mut doc_score = 0.0;

            for (field, term) in query_tokens {
                let weight = *self.field_weights.get(field).unwrap_or(&1.0);
                let b = *self.field_b.get(field).unwrap_or(&0.75);

                let mut weighted_tf = 0.0;

                if let Some(postings) = index.get_postings(*field, term) {
                    postings_hits += 1;

                    if let Some(&tf) = postings.frequencies.get(&doc_id) {
                        freq_hits += 1;

                        let dl = *metadata
                            .lengths
                            .get(&doc_id)
                            .and_then(|f| f.get(field))
                            .unwrap_or(&0) as f32;
                        let avgdl = *avg_lengths.get(field).unwrap_or(&1.0);

                        weighted_tf = (tf as f32 * weight) / (1.0 + b * (dl / avgdl - 1.0));
                    } else {
                        freq_misses += 1;
                    }
                } else {
                    postings_misses += 1;
                }

                let idf = idf_cache.get(&(*field, term.as_str())).unwrap_or(&0.0);
                doc_score += idf * (weighted_tf / (self.k1 + weighted_tf));
            }

            scores.push((doc_id, doc_score));
        }
        drop(scoring_timer);

        debug!(
            "[SCORER] Postings lookup: {} hits, {} misses ({:.1}% hit rate)",
            postings_hits,
            postings_misses,
            if postings_hits + postings_misses > 0 {
                100.0 * postings_hits as f32 / (postings_hits + postings_misses) as f32
            } else {
                0.0
            }
        );

        debug!(
            "[SCORER] Frequency lookup: {} hits, {} misses ({:.1}% hit rate)",
            freq_hits,
            freq_misses,
            if freq_hits + freq_misses > 0 {
                100.0 * freq_hits as f32 / (freq_hits + freq_misses) as f32
            } else {
                0.0
            }
        );

        let sort_timer = Timer::new("sort_results");
        scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        drop(sort_timer);

        if !scores.is_empty() {
            info!(
                "[SCORER] Completed: {} documents scored, top score: {:.4}, median: {:.4}, bottom: {:.4}",
                scores.len(),
                scores.first().map(|(_, s)| *s).unwrap_or(0.0),
                scores.get(scores.len() / 2).map(|(_, s)| *s).unwrap_or(0.0),
                scores.last().map(|(_, s)| *s).unwrap_or(0.0)
            );
        } else {
            info!("[SCORER] Completed: no documents scored");
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
