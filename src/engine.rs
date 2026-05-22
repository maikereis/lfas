//! Search engine core — candidate retrieval and BM25F scoring.
//!
//! This module owns [`SearchEngine`], the central struct that ties together the
//! inverted index, per-field metadata, and the BM25F scorer.  It exposes two
//! execution paths:
//!
//! * [`SearchEngine::execute`] — convenience wrapper that uses the engine's own
//!   built-in scorer.
//! * [`SearchEngine::execute_with_scorer`] — the primary path used by the Python
//!   layer; accepts a caller-supplied scorer so searches can run under a *read*
//!   lock without mutating any shared state.
//!
//! ## Two-round search strategy
//!
//! **Round 1 — candidate retrieval**
//! For each query field the tokeniser produces two token sets: *distinctive*
//! tokens (CEP codes, long numbers, name n-grams) and *all* tokens.  Within a
//! field the distinctive-token bitmaps are **intersected** (AND); across fields
//! they are **unioned** (OR).  This gives a tight candidate set in the common
//! case while still returning results when only some fields match.
//!
//! If no distinctive tokens survive, a fallback selects the *k* rarest tokens
//! by document frequency and unions their bitmaps instead.
//!
//! **Round 2 — BM25F scoring**
//! All candidates are scored with the full BM25F formula over every query
//! token.  The scorer reads postings via a single batched LMDB read transaction
//! and accumulates term contributions into a per-document score map.  Results
//! are sorted descending and truncated to `top_k`.

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

/// The central search engine, parameterised over a field type `F` and a
/// storage backend `S`.
///
/// `F` is typically [`RecordField`]; `S` is [`LmdbStorage`] in production and
/// [`InMemoryStorage`] in tests.
///
/// # Concurrency
///
/// `SearchEngine` itself carries no synchronisation primitives.  Thread safety
/// is provided by the `RwLock` in the Python layer (`GLOBAL_ENGINE`).
/// [`execute_with_scorer`] takes `&self` and performs only reads, so it is safe
/// to call from many threads simultaneously while the lock is held in read mode.
/// [`execute`] has the same property.
///
/// Mutable operations (indexing, flushing, loading metadata) require `&mut self`
/// and are therefore guarded by the write lock.
///
/// [`LmdbStorage`]: crate::storage::LmdbStorage
/// [`InMemoryStorage`]: crate::storage::InMemoryStorage
/// [`execute_with_scorer`]: SearchEngine::execute_with_scorer
pub struct SearchEngine<F, S>
where
    F: Hash + Eq + Clone + Ord + Copy,
    S: PostingsStorage<F>,
{
    /// The persistent inverted index mapping (field, term) pairs to posting
    /// lists.  Backed by LMDB in production; swappable for testing.
    pub index: InvertedIndex<F, S>,

    /// Per-field and per-document statistics required for BM25F scoring:
    /// document lengths, corpus-wide average lengths, total document count,
    /// and term document-frequencies.
    pub metadata: FieldMetadata<F>,

    /// Default BM25F scorer carrying field weights (`w_f`) and length-
    /// normalisation parameters (`b_f`).  This scorer is used by [`execute`]
    /// and is borrowed directly by [`execute_with_scorer`] when the Python
    /// caller has not configured custom weights.
    ///
    /// **Never mutate this field during a search.**  The Python layer stores
    /// custom weights on the `PySearchEngine` instance and constructs a
    /// throw-away scorer on the stack instead; see [`execute_with_scorer`].
    ///
    /// [`execute`]: SearchEngine::execute
    /// [`execute_with_scorer`]: SearchEngine::execute_with_scorer
    pub scorer: BM25FScorer<F>,
}

impl<S> SearchEngine<RecordField, S>
where
    S: PostingsStorage<RecordField>,
{
    /// Construct a `SearchEngine` for Brazilian address records with sensible
    /// BM25F defaults.
    ///
    /// The default weights are tuned for address matching where short,
    /// structured identifiers (house number, CEP) are more discriminating than
    /// free-text fields (street name, neighbourhood):
    ///
    /// | Field            | Weight | b    | Rationale                              |
    /// |------------------|--------|------|----------------------------------------|
    /// | `Numero`         | 10.0   | 0.0  | Most selective; fixed-length token     |
    /// | `Cep`            |  8.0   | 0.0  | Highly selective; fixed-length         |
    /// | `Rua`            |  5.0   | 0.75 | Important but variable length          |
    /// | `Municipio`      |  3.0   | 0.5  | Moderate; many docs share municipality |
    /// | `Bairro`         |  2.0   | 0.75 | Useful disambiguation                  |
    /// | `Complemento`    |  1.5   | 0.5  | Supplemental info                      |
    /// | `Estado`         |  1.0   | 0.0  | Low selectivity; short abbreviation    |
    /// | `Nome`           |  1.0   | 0.75 | Person / entity name                   |
    /// | `TipoLogradouro` |  0.5   | 0.0  | Very low selectivity                   |
    ///
    /// `k1 = 1.2` (standard BM25 saturation constant).
    ///
    /// Weights and b-values can be overridden at query time via
    /// [`PySearchEngine::set_field_weights`] and
    /// [`PySearchEngine::set_field_b_values`] without touching this struct.
    pub fn with_storage(storage: S) -> Self {
        let mut field_weights = HashMap::new();

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
    /// Execute a search using the engine's own built-in scorer.
    ///
    /// This is a convenience wrapper around [`execute_with_scorer`] that passes
    /// `&self.scorer`.  Use it from Rust code that does not need per-call weight
    /// overrides.  The Python layer calls [`execute_with_scorer`] directly so it
    /// can supply a stack-local scorer without touching shared state.
    ///
    /// # Parameters
    ///
    /// * `query` — structured query carrying per-field text and `top_k` limit.
    /// * `blocking_k` — passed through to `execute_with_scorer`; controls the
    ///   maximum candidate set size.  Currently reserved for future pruning;
    ///   the actual cut-off is applied inside the scorer.
    ///
    /// # Returns
    ///
    /// Up to `query.top_k` [`SearchHit`] values sorted by score descending.
    ///
    /// [`execute_with_scorer`]: SearchEngine::execute_with_scorer
    pub fn execute(&self, query: StructuredQuery<F>, blocking_k: usize) -> Vec<SearchHit> {
        self.execute_with_scorer(query, blocking_k, &self.scorer)
    }

    /// Execute a search with an explicitly supplied scorer.
    ///
    /// This is the **primary search entry-point** used by the Python layer.
    /// Accepting the scorer as a parameter decouples scoring configuration from
    /// the engine's own `scorer` field, which means:
    ///
    /// * The caller never needs to mutate `self` (or the global engine) during a
    ///   search — a `&self` borrow is sufficient.
    /// * The global `RwLock` can be held in **read mode** for the entire duration
    ///   of the search, allowing many threads to search concurrently.
    /// * Different Python `PySearchEngine` objects can carry different weights
    ///   and run parallel searches without interfering with each other.
    ///
    /// # Algorithm
    ///
    /// See the [module-level documentation](self) for a full description of the
    /// two-round candidate-retrieval / BM25F-scoring strategy.
    ///
    /// # Parameters
    ///
    /// * `query` — structured query.  `query.fields` drives both rounds;
    ///   `query.top_k` caps the returned result list.
    /// * `_blocking_k` — reserved for future candidate-set pruning between
    ///   rounds; currently unused inside this function.
    /// * `scorer` — the BM25F scorer to use for Round 2.  May be `&self.scorer`
    ///   (the default path) or a stack-allocated override constructed by the
    ///   caller.
    ///
    /// # Returns
    ///
    /// Up to `query.top_k` [`SearchHit`] values sorted by score descending.
    /// Returns an empty `Vec` if no candidates survive Round 1 (or the fallback).
    pub fn execute_with_scorer(
        &self,
        query: StructuredQuery<F>,
        _blocking_k: usize,
        scorer: &BM25FScorer<F>,
    ) -> Vec<SearchHit> {
        info!("[SEARCH] Starting search execution");
        let search_timer = Timer::new("SearchEngine::execute");

        info!("[SEARCH] ROUND 1: Building candidates (per-field intersect, cross-field union)");
        let round1_timer = Timer::new("Round1::FindCandidates");

        // `per_field_candidates[field]` accumulates the bitmap of documents
        // that contain at least one distinctive token for that field.
        // `all_query_tokens` collects every token (distinctive + weak) for
        // the BM25F scoring pass in Round 2.
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

            // Within-field strategy: start with the full document universe
            // (`None`) and narrow it down by intersecting each distinctive
            // token's posting bitmap.  Missing tokens are skipped rather than
            // zeroing the field bitmap — a single unindexed token (e.g. a typo
            // or an out-of-vocabulary abbreviation) would otherwise eliminate
            // all candidates for that field.
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
                        debug!(
                            "[SEARCH]     Token '{}' not found in field {:?}, skipping",
                            token, field
                        );
                    }
                }
            }

            // Cross-field strategy: union across fields so a document qualifies
            // as a candidate if it matches *any* queried field (OR semantics).
            if let Some(bitmap) = field_bitmap {
                if !bitmap.is_empty() {
                    info!(
                        "[SEARCH]   Field {:?} contributed {} candidates",
                        field,
                        bitmap.len()
                    );
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

            // Collect ALL tokens (including weak n-grams) for Round 2 scoring
            // regardless of whether they contributed any candidates.
            for token in token_set.all {
                all_query_tokens.push((*field, token));
            }
        }

        // Merge all per-field bitmaps into one flat candidate set for scoring.
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
            // Fallback: no distinctive token matched anything in the index.
            // This happens for very short queries, heavily abbreviated inputs,
            // or queries whose terms all landed in the stop-word / too-short
            // filter.  Rather than returning nothing we fall back to the
            // *k* rarest tokens by document frequency — rarer tokens produce
            // smaller, more relevant candidate sets.
            info!("[SEARCH] FALLBACK: no distinctive-token candidates, using rarest tokens");

            let mut token_rareness: Vec<(&F, &String, usize)> = Vec::new();
            for (field, token) in &all_query_tokens {
                if let Some(&df) = self.metadata.term_df.get(&(*field, token.clone())) {
                    token_rareness.push((field, token, df));
                }
            }

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

        info!(
            "[SEARCH] ROUND 2: Scoring {} candidates with {} query tokens",
            candidates.len(),
            all_query_tokens.len()
        );

        let round2_timer = Timer::new("Round2::ScoreCandidates");
        // Score all candidates using the *caller-supplied* scorer.
        // Using `scorer` (parameter) rather than `self.scorer` is what allows
        // this function to hold only a read lock: no field of `self` is written.
        let scored_results =
            scorer.score(candidates, &all_query_tokens, &self.index, &self.metadata);
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