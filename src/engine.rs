use crate::index::InvertedIndex;
use crate::metadata::FieldMetadata;
use crate::scorer::BM25FScorer;
use crate::storage::PostingsStorage;
use crate::timing::Timer;
use crate::tokenizer::tokenize_structured;
use crate::{RecordField, SearchHit, StructuredQuery};
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

impl<S> SearchEngine<RecordField, S>
where
    S: PostingsStorage<RecordField>,
{
    pub fn with_storage(storage: S) -> Self {
        let mut field_weights = HashMap::new();

        // Use _f32 suffix to ensure types match the f32 in BM25FScorer
        field_weights.insert(RecordField::Numero, 10.0_f32);
        field_weights.insert(RecordField::Cep, 8.0_f32);
        field_weights.insert(RecordField::Rua, 5.0_f32);
        field_weights.insert(RecordField::Municipio, 3.0_f32);
        field_weights.insert(RecordField::Bairro, 2.0_f32);
        field_weights.insert(RecordField::Complemento, 1.5_f32);
        field_weights.insert(RecordField::Estado, 1.0_f32);
        field_weights.insert(RecordField::TipoLogradouro, 0.5_f32);
        field_weights.insert(RecordField::Nome, 1.0_f32);

        let mut field_b = HashMap::new();

        field_b.insert(RecordField::Numero, 0.0_f32);
        field_b.insert(RecordField::Cep, 0.0_f32);
        field_b.insert(RecordField::Estado, 0.0_f32);
        field_b.insert(RecordField::Rua, 0.75_f32);
        field_b.insert(RecordField::Municipio, 0.5_f32);
        field_b.insert(RecordField::Bairro, 0.75_f32);
        field_b.insert(RecordField::Complemento, 0.5_f32);
        field_b.insert(RecordField::TipoLogradouro, 0.0_f32);
        field_b.insert(RecordField::Nome, 0.75_f32);

        Self {
            index: InvertedIndex::new(storage),
            metadata: FieldMetadata::new(),
            scorer: BM25FScorer {
                k1: 1.2_f32,
                field_weights,
                field_b,
            },
        }
    }
}

impl<F, S> SearchEngine<F, S>
where
    F: Hash + Eq + Clone + Ord + Copy + std::fmt::Debug,
    S: PostingsStorage<F>,
{
    pub fn execute(&self, query: StructuredQuery<F>, _blocking_k: usize) -> Vec<SearchHit> {
        info!("[SEARCH] Starting search execution");
        let search_timer = Timer::new("SearchEngine::execute");

        info!("[SEARCH] ROUND 1: Building candidates (per-field intersect, cross-field union)");
        let round1_timer = Timer::new("Round1::FindCandidates");

        // per_field_candidates[field] = intersection of that field's distinctive token bitmaps
        let mut per_field_candidates: HashMap<F, RoaringBitmap> = HashMap::new();
        let mut all_query_tokens: Vec<(F, String)> = Vec::new();

        for (field, text) in &query.fields {
            debug!("[SEARCH] Processing field {:?}: '{}'", field, text);
            let token_set = tokenize_structured(text);

            info!(
                "[SEARCH]   Field {:?} — distinctive: {}, all: {}",
                field,
                token_set.distinctive.len(),
                token_set.all.len()
            );

            // Round 1: Union of distinctive tokens (any match qualifies)

            // Within-field: intersect distinctive token bitmaps.
            // We start with None (meaning "universe") and narrow down with each token.
            let mut field_bitmap: Option<RoaringBitmap> = None;

            for token in &token_set.distinctive {
                match self.index.get_postings(*field, token) {
                    Some(postings) => {
                        let before = field_bitmap.as_ref().map(|b| b.len()).unwrap_or(u64::MAX);
                        field_bitmap = Some(match field_bitmap {
                            None => postings.bitmap().clone(),
                            Some(existing) => existing & postings.bitmap(),
                        });
                        let after = field_bitmap.as_ref().map(|b| b.len()).unwrap_or(0);
                        debug!(
                            "[SEARCH]     Token '{}' narrowed field candidates: {} → {}",
                            token, before, after
                        );
                    }
                    None => {
                        // Token not in index for this field.
                        // For a strict AND we would zero out the bitmap here.
                        // Instead we skip it so a single missing token (e.g. a typo)
                        // doesn't eliminate all candidates for the field.
                        debug!(
                            "[SEARCH]     Token '{}' not found in field {:?}, skipping",
                            token, field
                        );
                    }
                }
            }

            if let Some(bitmap) = field_bitmap {
                if !bitmap.is_empty() {
                    info!(
                        "[SEARCH]   Field {:?} contributed {} candidates",
                        field,
                        bitmap.len()
                    );
                    // Cross-field: union
                    per_field_candidates
                        .entry(*field)
                        .and_modify(|e| *e |= &bitmap)
                        .or_insert(bitmap);
                } else {
                    info!(
                        "[SEARCH]   Field {:?} intersection is empty — skipping",
                        field
                    );
                }
            }

            // Collect ALL tokens for Round 2 scoring regardless of field result
            for token in token_set.all {
                all_query_tokens.push((*field, token));
            }
        }

        // Merge all per-field bitmaps into one candidate set
        let mut candidates = RoaringBitmap::new();
        for bitmap in per_field_candidates.values() {
            candidates |= bitmap;
        }

        drop(round1_timer);
        info!(
            "[SEARCH] ROUND 1 Complete: {} candidates from {} fields",
            candidates.len(),
            per_field_candidates.len()
        );

        if candidates.is_empty() {
            info!("[SEARCH] FALLBACK: no distinctive-token candidates, using rarest tokens");

            let mut token_rareness: Vec<(&F, &String, usize)> = Vec::new();
            for (field, token) in &all_query_tokens {
                if let Some(&df) = self.metadata.term_df.get(&(*field, token.clone())) {
                    token_rareness.push((field, token, df));
                }
            }

            // Sort ascending by document frequency — rarest first
            token_rareness.sort_by_key(|(_, _, df)| *df);

            let k_rarest = 5.min(token_rareness.len());
            info!("[SEARCH] Using {} rarest tokens for fallback", k_rarest);

            for (field, token, df) in token_rareness.iter().take(k_rarest) {
                if let Some(postings) = self.index.get_postings(**field, token) {
                    let before = candidates.len();
                    candidates |= postings.bitmap();
                    info!(
                        "[SEARCH]   Fallback token '{}' (df={}) added {} candidates (total: {})",
                        token,
                        df,
                        candidates.len() - before,
                        candidates.len()
                    );
                }
            }
        }

        if candidates.is_empty() {
            info!("[SEARCH] No candidates found, returning empty results");
            return vec![];
        }

        // ROUND 2: Score candidates with full BM25F over ALL query tokens    //
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
