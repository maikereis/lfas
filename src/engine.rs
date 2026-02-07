use crate::index::InvertedIndex;
use crate::metadata::FieldMetadata;
use crate::scorer::BM25FScorer;
use crate::storage::PostingsStorage;
use crate::timing::Timer;
use crate::tokenizer::tokenize_structured;
use crate::{SearchHit, StructuredQuery};
use log::{debug, info};
use roaring::RoaringBitmap;
use std::collections::HashMap;
use std::hash::Hash;

pub struct SearchEngine<F, S>
where
    F: Hash + Eq + Clone + Ord + Copy,
    S: PostingsStorage<F>,
{
    pub index: InvertedIndex<F, S>,
    pub metadata: FieldMetadata<F>,
    pub scorer: BM25FScorer<F>,
}

impl<F, S> SearchEngine<F, S>
where
    F: Hash + Eq + Clone + Ord + Copy + std::fmt::Debug,
    S: PostingsStorage<F>,
{
    pub fn with_storage(storage: S) -> Self {
        let field_weights = HashMap::new();

        Self {
            index: InvertedIndex::new(storage),
            metadata: FieldMetadata::new(),
            scorer: BM25FScorer {
                k1: 1.2,
                field_weights,
                field_b: HashMap::new(),
            },
        }
    }

    pub fn execute(&self, query: StructuredQuery<F>, _blocking_k: usize) -> Vec<SearchHit> {
        info!("[SEARCH] Starting search execution");
        let search_timer = Timer::new("SearchEngine::execute");

        // ROUND 1: Use DISTINCTIVE tokens to find candidates
        info!("[SEARCH] ROUND 1: Finding candidates using distinctive tokens");
        let round1_timer = Timer::new("Round1::FindCandidates");

        let mut candidates = RoaringBitmap::new();
        let mut all_query_tokens: Vec<(F, String)> = Vec::new();

        for (field, text) in &query.fields {
            debug!("[SEARCH] Processing field {:?}: '{}'", field, text);
            let token_set = tokenize_structured(text);

            info!(
                "[SEARCH]   Field {:?} - Distinctive tokens: {}, All tokens: {}",
                field,
                token_set.distinctive.len(),
                token_set.all.len()
            );

            // Round 1: Union of distinctive tokens (any match qualifies)
            for token in &token_set.distinctive {
                if let Some(postings) = self.index.get_postings(*field, token) {
                    let before = candidates.len();
                    candidates |= &postings.bitmap;
                    let after = candidates.len();
                    debug!(
                        "[SEARCH]     Token '{}' added {} candidates (total: {} -> {})",
                        token,
                        after - before,
                        before,
                        after
                    );
                }
            }

            // Collect ALL tokens for Round 2 scoring
            for token in token_set.all {
                all_query_tokens.push((*field, token));
            }
        }

        // FALLBACK: If no distinctive tokens found candidates, use rarest tokens
        if candidates.is_empty() && !all_query_tokens.is_empty() {
            info!("[SEARCH] FALLBACK: No distinctive tokens found candidates, using rarest tokens");

            // Use pre-computed document frequency from metadata
            let mut token_rareness: Vec<(&F, &String, usize)> = Vec::new();

            for (field, token) in &all_query_tokens {
                if let Some(&df) = self.metadata.term_df.get(&(*field, token.clone())) {
                    token_rareness.push((field, token, df));
                }
            }

            // Sort by rarity (smallest document frequency = most selective)
            token_rareness.sort_by_key(|(_, _, df)| *df);

            // Use up to 5 rarest tokens to build candidate set
            let k_rarest = 5.min(token_rareness.len());
            info!("[SEARCH] Using {} rarest tokens for fallback", k_rarest);

            for (field, token, df) in token_rareness.iter().take(k_rarest) {
                if let Some(postings) = self.index.get_postings(**field, token) {
                    let before = candidates.len();
                    candidates |= &postings.bitmap;
                    let after = candidates.len();
                    info!(
                        "[SEARCH]   Fallback token '{}' (df={}) added {} candidates (total: {})",
                        token,
                        df,
                        after - before,
                        after
                    );
                }
            }
        }

        drop(round1_timer);
        info!(
            "[SEARCH] ROUND 1 Complete: {} candidates found",
            candidates.len()
        );

        if candidates.is_empty() {
            info!("[SEARCH] No candidates found, returning empty results");
            return vec![];
        }

        // ROUND 2: Score candidates using ALL tokens (including weak n-grams)
        info!(
            "[SEARCH] ROUND 2: Scoring {} candidates with {} query tokens",
            candidates.len(),
            all_query_tokens.len()
        );

        let round2_timer = Timer::new("Round2::ScoreCandidates");
        let scored_results =
            self.scorer
                .score(candidates, &all_query_tokens, &self.index, &self.metadata);
        drop(round2_timer);

        info!("[SEARCH] Scored {} documents", scored_results.len());

        // Take top-k results
        let final_results: Vec<SearchHit> = scored_results
            .into_iter()
            .take(query.top_k)
            .map(|(doc_id, score)| {
                debug!("[SEARCH] Result: doc_id={}, score={}", doc_id, score);
                SearchHit { doc_id, score }
            })
            .collect();

        drop(search_timer);
        info!("[SEARCH] Returning {} results", final_results.len());

        final_results
    }
}
