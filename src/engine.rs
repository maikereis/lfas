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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::InvertedIndex;
    use crate::metadata::FieldMetadata;
    use crate::scorer::BM25FScorer;
    use crate::storage::InMemoryStorage;
    use crate::tokenizer::tokenize;
    use crate::{Record, RecordField, StructuredQuery};
    use std::collections::HashMap;

    #[test]
    fn test_structured_address_search() {
        let _ = env_logger::builder().is_test(true).try_init();

        // 1. Initialize Components
        let storage = InMemoryStorage::new();
        let mut index = InvertedIndex::new(storage);
        let mut metadata = FieldMetadata::new();

        // Define sample addresses with more distinctive content
        let address_1 = Record {
            id: "101".into(),
            estado: "PA".into(),
            municipio: "Belem".into(),
            bairro: "Marco".into(),
            cep: "66095-000".into(),
            tipo_logradouro: "Passagem".into(),
            rua: "Mauriti".into(),
            numero: "31".into(),
            complemento: "".into(),
            nome: "Edificio Metropolitan".into(),
        };

        let address_2 = Record {
            id: "102".into(),
            estado: "PA".into(),
            municipio: "Ananindeua".into(),
            bairro: "Centro".into(),
            cep: "67000-000".into(),
            tipo_logradouro: "Rua".into(),
            rua: "Mauriti".into(),
            numero: "500".into(),
            complemento: "Lote B".into(),
            nome: "Mercado Municipal".into(),
        };

        // 2. Populate the Index using tokenize (which returns all tokens)
        let dataset = vec![address_1, address_2];
        for (internal_id, record) in dataset.iter().enumerate() {
            metadata.total_docs += 1;
            let doc_meta = metadata.lengths.entry(internal_id).or_default();

            for (field, text) in record.fields() {
                let tokens = tokenize(text);
                doc_meta.insert(field, tokens.len());
                *metadata.total_field_lengths.entry(field).or_insert(0) += tokens.len();

                for token in tokens {
                    index.add_term(internal_id, field, token.clone());

                    // Track document frequency for fallback
                    let key = (field, token);
                    *metadata.term_df.entry(key).or_insert(0) += 1;
                }
            }
        }

        // 3. Configure Scorer with Field Weights
        let mut field_weights = HashMap::new();
        field_weights.insert(RecordField::Rua, 2.0);
        field_weights.insert(RecordField::Municipio, 1.0);
        field_weights.insert(RecordField::Cep, 5.0);

        let engine = SearchEngine {
            index,
            metadata,
            scorer: BM25FScorer {
                k1: 1.2,
                field_weights,
                field_b: HashMap::new(),
            },
        };

        // 4. Test with CEP (highly distinctive token)
        println!("\n=== Test 1: CEP Search (Distinctive) ===");
        let query_cep = StructuredQuery {
            fields: vec![(RecordField::Cep, "66095-000".to_string())],
            top_k: 5,
        };

        let results_cep = engine.execute(query_cep, 10);
        println!("CEP Search Results:");
        for (i, hit) in results_cep.iter().enumerate() {
            println!("{}. Document {} (Score: {})", i + 1, hit.doc_id, hit.score);
        }
        assert!(!results_cep.is_empty(), "CEP search should return results");
        assert_eq!(
            results_cep[0].doc_id, 0,
            "Should find address with matching CEP"
        );

        // 5. Test with Municipio only (should use fallback)
        println!("\n=== Test 2: Municipio Only (Fallback) ===");
        let query_municipio_only = StructuredQuery {
            fields: vec![(RecordField::Municipio, "Belem".to_string())],
            top_k: 5,
        };

        let results_municipio_only = engine.execute(query_municipio_only, 10);
        println!("Municipio Only Search Results:");
        for (i, hit) in results_municipio_only.iter().enumerate() {
            println!("{}. Document {} (Score: {})", i + 1, hit.doc_id, hit.score);
        }
        assert!(
            !results_municipio_only.is_empty(),
            "Municipio search should return results via fallback"
        );
        assert_eq!(
            results_municipio_only[0].doc_id, 0,
            "Should find Belem address"
        );

        // 6. Test with Municipio + Number (creates distinctive token)
        println!("\n=== Test 3: Municipio + Number Search ===");
        let query_municipio = StructuredQuery {
            fields: vec![
                (RecordField::Municipio, "Belem".to_string()),
                (RecordField::Numero, "31".to_string()),
            ],
            top_k: 5,
        };

        let results_municipio = engine.execute(query_municipio, 10);
        println!("Municipio + Number Search Results:");
        for (i, hit) in results_municipio.iter().enumerate() {
            println!("{}. Document {} (Score: {})", i + 1, hit.doc_id, hit.score);
        }
        assert!(
            !results_municipio.is_empty(),
            "Municipio + Number search should return results"
        );
        assert_eq!(
            results_municipio[0].doc_id, 0,
            "Should find address with Belem and 31"
        );

        // 7. Test with combined query including a distinctive token (number)
        println!("\n=== Test 4: Combined Search (Rua + Municipio + Number) ===");
        let query_combined = StructuredQuery {
            fields: vec![
                (RecordField::Rua, "Mauriti".to_string()),
                (RecordField::Municipio, "Belem".to_string()),
                (RecordField::Numero, "31".to_string()),
            ],
            top_k: 5,
        };

        let results_combined = engine.execute(query_combined, 10);
        println!("Combined Search Results:");
        for (i, hit) in results_combined.iter().enumerate() {
            println!("{}. Document {} (Score: {})", i + 1, hit.doc_id, hit.score);
        }

        assert!(
            !results_combined.is_empty(),
            "Combined search should return results"
        );
        assert_eq!(
            results_combined[0].doc_id, 0,
            "Address 1 should be top result"
        );

        if results_combined.len() > 1 {
            println!(
                "Top Result: doc {} score {}",
                results_combined[0].doc_id, results_combined[0].score
            );
            println!(
                "Second Result: doc {} score {}",
                results_combined[1].doc_id, results_combined[1].score
            );
            assert!(
                results_combined[0].score > results_combined[1].score,
                "Full match should score higher than partial match"
            );
        }
    }
}
