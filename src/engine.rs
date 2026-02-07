use crate::tokenizer::{tokenize_structured};
use crate::{RecordField, SearchHit, StructuredQuery};
use crate::index::InvertedIndex;
use crate::metadata::FieldMetadata;
use crate::scorer::BM25FScorer;
use roaring::RoaringBitmap;
use log::{info, debug};
use crate::timing::Timer;

pub struct SearchEngine {
    pub index: InvertedIndex<RecordField>,
    pub metadata: FieldMetadata<RecordField>,
    pub scorer: BM25FScorer<RecordField>,
}

impl SearchEngine {
    pub fn execute(&self, query: StructuredQuery, _blocking_k: usize) -> Vec<SearchHit> {
        info!("[SEARCH] Starting search execution");
        let search_timer = Timer::new("SearchEngine::execute");
        
        // ROUND 1: Use DISTINCTIVE tokens to find candidates
        info!("[SEARCH] ROUND 1: Finding candidates using distinctive tokens");
        let round1_timer = Timer::new("Round1::FindCandidates");
        
        let mut candidates = RoaringBitmap::new();
        let mut all_query_tokens: Vec<(RecordField, String)> = Vec::new();

        for (field, text) in &query.fields {
            debug!("[SEARCH] Processing field {:?}: '{}'", field, text);
            let token_set = tokenize_structured(text);
            
            info!("[SEARCH]   Field {:?} - Distinctive tokens: {}, All tokens: {}", 
                  field, token_set.distinctive.len(), token_set.all.len());
            
            // Round 1: Union of distinctive tokens (any match qualifies)
            for token in &token_set.distinctive {
                if let Some(postings) = self.index.get_postings(*field, token) {
                    let before = candidates.len();
                    candidates |= &postings.bitmap;
                    let after = candidates.len();
                    debug!("[SEARCH]     Token '{}' added {} candidates (total: {} -> {})", 
                           token, after - before, before, after);
                }
            }
            
            // Collect ALL tokens for Round 2 scoring
            for token in token_set.all {
                all_query_tokens.push((*field, token));
            }
        }

        drop(round1_timer);
        info!("[SEARCH] ROUND 1 Complete: {} candidates found", candidates.len());

        if candidates.is_empty() { 
            info!("[SEARCH] No candidates found, returning empty results");
            return vec![]; 
        }

        // ROUND 2: Score candidates using ALL tokens (including weak n-grams)
        info!("[SEARCH] ROUND 2: Scoring {} candidates with {} query tokens", 
              candidates.len(), all_query_tokens.len());
        
        let round2_timer = Timer::new("Round2::ScoreCandidates");
        let scored_results = self.scorer
            .score(candidates, &all_query_tokens, &self.index, &self.metadata);
        drop(round2_timer);
        
        info!("[SEARCH] Scored {} documents", scored_results.len());
        
        // Take top-k results
        let final_results: Vec<SearchHit> = scored_results
            .into_iter()
            .take(query.top_k)
            .map(|(doc_id, score)| {
                debug!("[SEARCH] Result: doc_id={}, score={}", doc_id, score);
                SearchHit {doc_id, score}
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
    use crate::tokenizer::tokenize;
    use crate::index::InvertedIndex;
    use crate::metadata::FieldMetadata;
    use crate::scorer::BM25FScorer;
    use crate::{Record, RecordField, StructuredQuery};
    use std::collections::HashMap;

    #[test]
    fn test_structured_address_search() {
        // 1. Initialize Components
        let mut index = InvertedIndex::new();
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
                    index.add_term(internal_id, field, token);
                }
            }
        }

        // 3. Configure Scorer with Field Weights
        let mut field_weights = HashMap::new();
        field_weights.insert(RecordField::Rua, 2.0);
        field_weights.insert(RecordField::Municipio, 1.0);
        field_weights.insert(RecordField::Cep, 5.0); // CEP is highly distinctive

        let engine = SearchEngine {
            index,
            metadata,
            scorer: BM25FScorer {
                k1: 1.2,
                field_weights,
                field_b: HashMap::new(), // Default b=0.75
            },
        };

        // 4. Test with CEP (highly distinctive token)
        println!("\n=== Test 1: CEP Search (Distinctive) ===");
        let query_cep = StructuredQuery {
            fields: vec![
                (RecordField::Cep, "66095-000".to_string()),
            ],
            top_k: 5,
        };

        let results_cep = engine.execute(query_cep, 10);
        println!("CEP Search Results:");
        for (i, hit) in results_cep.iter().enumerate() {
            println!("{}. Document {} (Score: {})", i + 1, hit.doc_id, hit.score);
        }
        assert!(!results_cep.is_empty(), "CEP search should return results");
        assert_eq!(results_cep[0].doc_id, 0, "Should find address with matching CEP");

        // 5. Test with Municipio + Number (creates distinctive token)
        // Note: Pure city name searches won't work in the two-round architecture
        // because city names are not distinctive tokens. We need to combine with
        // something distinctive like a house number.
        println!("\n=== Test 2: Municipio + Number Search ===");
        let query_municipio = StructuredQuery {
            fields: vec![
                (RecordField::Municipio, "Belem".to_string()),
                (RecordField::Numero, "31".to_string()), // 31 is a distinctive token
            ],
            top_k: 5,
        };

        let results_municipio = engine.execute(query_municipio, 10);
        println!("Municipio + Number Search Results:");
        for (i, hit) in results_municipio.iter().enumerate() {
            println!("{}. Document {} (Score: {})", i + 1, hit.doc_id, hit.score);
        }
        assert!(!results_municipio.is_empty(), "Municipio + Number search should return results");
        assert_eq!(results_municipio[0].doc_id, 0, "Should find address with Belem and 31");

        // 6. Test with combined query including a distinctive token (number)
        println!("\n=== Test 3: Combined Search (Rua + Municipio + Number) ===");
        let query_combined = StructuredQuery {
            fields: vec![
                (RecordField::Rua, "Mauriti".to_string()),      // Common (not distinctive)
                (RecordField::Municipio, "Belem".to_string()),  // Common (not distinctive) 
                (RecordField::Numero, "31".to_string()),        // Distinctive!
            ],
            top_k: 5,
        };

        let results_combined = engine.execute(query_combined, 10);
        println!("Combined Search Results:");
        for (i, hit) in results_combined.iter().enumerate() {
            println!("{}. Document {} (Score: {})", i + 1, hit.doc_id, hit.score);
        }
        
        assert!(!results_combined.is_empty(), "Combined search should return results");
        assert_eq!(results_combined[0].doc_id, 0, "Address 1 (has all 3: Belem + Mauriti + 31) should be top result");
        
        if results_combined.len() > 1 {
            println!("Top Result: doc {} score {}", results_combined[0].doc_id, results_combined[0].score);
            println!("Second Result: doc {} score {}", results_combined[1].doc_id, results_combined[1].score);
            // Address 1 matches all 3 fields, Address 2 only matches Mauriti
            assert!(results_combined[0].score > results_combined[1].score, 
                    "Full match (3 fields) should score higher than partial match (1 field)");
        }
    }
}