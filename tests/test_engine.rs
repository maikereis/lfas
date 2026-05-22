use lfas::engine::SearchEngine;
use lfas::scorer::BM25FScorer;
use lfas::storage::InMemoryStorage;
use lfas::tokenizer::tokenize;
use lfas::{Record, RecordField, StructuredQuery};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Helper: index one record into the engine, mirroring the Python index_dict
// logic exactly.
//
// Key subtleties:
//   * `term_df` is document frequency — each unique (field, token) pair is
//     counted *once per document*, not once per occurrence.  We collect all
//     unique pairs first and then increment the counter, matching the
//     Python implementation.
//   * `total_docs` is the highest `internal_id + 1` seen, not a simple
//     accumulator; this matches the Python `if doc_id >= total_docs` guard.
// ---------------------------------------------------------------------------
fn index_record(
    engine: &mut SearchEngine<RecordField, InMemoryStorage<RecordField>>,
    internal_id: usize,
    record: &Record,
) {
    let mut doc_terms: HashMap<(RecordField, String), bool> = HashMap::new();

    for (field, text) in record.fields() {
        let tokens = tokenize(text);

        engine
            .metadata
            .lengths
            .entry(internal_id)
            .or_default()
            .insert(field, tokens.len());
        *engine
            .metadata
            .total_field_lengths
            .entry(field)
            .or_insert(0) += tokens.len();

        for token in tokens {
            engine.index.add_term(internal_id, field, token.clone());
            doc_terms.insert((field, token), true);
        }
    }

    // Increment df once per unique (field, token) pair, not per occurrence.
    for (key, _) in doc_terms {
        *engine.metadata.term_df.entry(key).or_insert(0) += 1;
    }

    if internal_id >= engine.metadata.total_docs {
        engine.metadata.total_docs = internal_id + 1;
    }
}

// ---------------------------------------------------------------------------
// Shared test data
// ---------------------------------------------------------------------------
fn make_dataset() -> Vec<Record> {
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

    vec![address_1, address_2]
}

fn build_engine() -> SearchEngine<RecordField, InMemoryStorage<RecordField>> {
    let storage = InMemoryStorage::new();
    // Use the canonical constructor, which sets default BM25F weights/b-values:
    //   Numero: 10.0 (b=0.0), Cep: 8.0 (b=0.0), Rua: 5.0 (b=0.75),
    //   Municipio: 3.0 (b=0.5), Bairro: 2.0, Complemento: 1.5, Estado: 1.0,
    //   Nome: 1.0, TipoLogradouro: 0.5
    let mut engine = SearchEngine::with_storage(storage);
    for (internal_id, record) in make_dataset().iter().enumerate() {
        index_record(&mut engine, internal_id, record);
    }
    engine
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn test_structured_address_search() {
    let _ = env_logger::builder().is_test(true).try_init();
    let engine = build_engine();

    // ── Test 1: CEP search (highly distinctive) ──────────────────────────────
    println!("\n=== Test 1: CEP Search (Distinctive) ===");
    let query_cep = StructuredQuery {
        fields: vec![(RecordField::Cep, "66095-000".to_string())],
        top_k: 5,
        blocking_k: 10_000,
    };

    let results_cep = engine.execute(query_cep, 10_000);
    println!("CEP Search Results:");
    for (i, hit) in results_cep.iter().enumerate() {
        println!("{}. Document {} (Score: {})", i + 1, hit.doc_id, hit.score);
    }
    assert!(!results_cep.is_empty(), "CEP search should return results");
    assert_eq!(
        results_cep[0].doc_id, 0,
        "Should find address with matching CEP (doc 0)"
    );

    // ── Test 2: Municipio only (exercises fallback path) ─────────────────────
    println!("\n=== Test 2: Municipio Only (Fallback) ===");
    let query_municipio_only = StructuredQuery {
        fields: vec![(RecordField::Municipio, "Belem".to_string())],
        top_k: 5,
        blocking_k: 10_000,
    };

    let results_municipio_only = engine.execute(query_municipio_only, 10_000);
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
        "Should find the Belem address (doc 0)"
    );

    // ── Test 3: Municipio + Numero ────────────────────────────────────────────
    println!("\n=== Test 3: Municipio + Number Search ===");
    let query_municipio_numero = StructuredQuery {
        fields: vec![
            (RecordField::Municipio, "Belem".to_string()),
            (RecordField::Numero, "31".to_string()),
        ],
        top_k: 5,
        blocking_k: 10_000,
    };

    let results_municipio_numero = engine.execute(query_municipio_numero, 10_000);
    println!("Municipio + Number Search Results:");
    for (i, hit) in results_municipio_numero.iter().enumerate() {
        println!("{}. Document {} (Score: {})", i + 1, hit.doc_id, hit.score);
    }
    assert!(
        !results_municipio_numero.is_empty(),
        "Municipio + Numero search should return results"
    );
    assert_eq!(
        results_municipio_numero[0].doc_id, 0,
        "Should find address with Belem and numero 31 (doc 0)"
    );

    // ── Test 4: Combined (Rua + Municipio + Numero) ───────────────────────────
    println!("\n=== Test 4: Combined Search (Rua + Municipio + Number) ===");
    let query_combined = StructuredQuery {
        fields: vec![
            (RecordField::Rua, "Mauriti".to_string()),
            (RecordField::Municipio, "Belem".to_string()),
            (RecordField::Numero, "31".to_string()),
        ],
        top_k: 5,
        blocking_k: 10_000,
    };

    let results_combined = engine.execute(query_combined, 10_000);
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
        "Full-match address (doc 0) should be top result"
    );

    if results_combined.len() > 1 {
        println!(
            "Top Result:    doc {} score {}",
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

// ---------------------------------------------------------------------------
// Test execute_with_scorer: verify that a caller-supplied scorer is used and
// that changing weights produces different ranking behaviour.
// ---------------------------------------------------------------------------
#[test]
fn test_execute_with_scorer_custom_weights() {
    let _ = env_logger::builder().is_test(true).try_init();
    let engine = build_engine();

    // Build a scorer that heavily favours Municipio over everything else.
    // With these weights Belem (doc 0) should outscore Ananindeua (doc 1) on
    // any query that includes the street name shared by both addresses.
    let mut field_weights = HashMap::new();
    field_weights.insert(RecordField::Municipio, 50.0_f32);
    field_weights.insert(RecordField::Rua, 1.0_f32);

    let custom_scorer = BM25FScorer {
        k1: 1.2_f32,
        field_weights,
        field_b: HashMap::new(),
    };

    let query = StructuredQuery {
        fields: vec![
            (RecordField::Rua, "Mauriti".to_string()),
            (RecordField::Municipio, "Belem".to_string()),
        ],
        top_k: 5,
        blocking_k: 10_000,
    };

    let results = engine.execute_with_scorer(query, 10_000, &custom_scorer);

    println!("\n=== execute_with_scorer: Municipio-heavy weights ===");
    for (i, hit) in results.iter().enumerate() {
        println!("{}. Document {} (Score: {})", i + 1, hit.doc_id, hit.score);
    }

    assert!(
        !results.is_empty(),
        "Should return results with custom scorer"
    );
    assert_eq!(
        results[0].doc_id, 0,
        "Municipio-heavy scorer should still rank the Belem address first"
    );
}

// ---------------------------------------------------------------------------
// Test execute_with_scorer does not mutate the engine's own scorer.
// Run a search with a wildly different weight map; the built-in scorer should
// remain unchanged for the next call.
// ---------------------------------------------------------------------------
#[test]
fn test_execute_with_scorer_does_not_mutate_engine_scorer() {
    let _ = env_logger::builder().is_test(true).try_init();
    let engine = build_engine();

    // Record the engine's default Cep weight before the call.
    let cep_weight_before = *engine
        .scorer
        .field_weights
        .get(&RecordField::Cep)
        .expect("Cep weight should be set by with_storage");

    // Deliberately override Cep weight to something very different.
    let mut field_weights = engine.scorer.field_weights.clone();
    field_weights.insert(RecordField::Cep, 0.001_f32);
    let ephemeral_scorer = BM25FScorer {
        k1: engine.scorer.k1,
        field_weights,
        field_b: engine.scorer.field_b.clone(),
    };

    let query = StructuredQuery {
        fields: vec![(RecordField::Cep, "66095-000".to_string())],
        top_k: 5,
        blocking_k: 10_000,
    };

    let _ = engine.execute_with_scorer(query, 10_000, &ephemeral_scorer);

    // The engine's own scorer must be untouched.
    let cep_weight_after = *engine
        .scorer
        .field_weights
        .get(&RecordField::Cep)
        .expect("Cep weight should still be set after the call");

    assert_eq!(
        cep_weight_before, cep_weight_after,
        "execute_with_scorer must not mutate the engine's own scorer"
    );
}

// ---------------------------------------------------------------------------
// Test empty-results path: a term that doesn't exist in the index should
// return an empty Vec without panicking.
// ---------------------------------------------------------------------------
#[test]
fn test_search_unknown_term_returns_empty() {
    let _ = env_logger::builder().is_test(true).try_init();
    let engine = build_engine();

    let query = StructuredQuery {
        fields: vec![(RecordField::Rua, "xyznonexistentstreet".to_string())],
        top_k: 5,
        blocking_k: 10_000,
    };

    let results = engine.execute(query, 10_000);
    assert!(
        results.is_empty(),
        "A completely unknown term should return no results"
    );
}

// ---------------------------------------------------------------------------
// Test with_storage default weights match documented values.
// ---------------------------------------------------------------------------
#[test]
fn test_with_storage_default_weights() {
    let storage = InMemoryStorage::new();
    let engine = SearchEngine::with_storage(storage);

    let weights = &engine.scorer.field_weights;

    // Spot-check the documented defaults from engine.rs.
    assert_eq!(weights.get(&RecordField::Numero).copied(), Some(10.0_f32));
    assert_eq!(weights.get(&RecordField::Cep).copied(), Some(8.0_f32));
    assert_eq!(weights.get(&RecordField::Rua).copied(), Some(5.0_f32));
    assert_eq!(weights.get(&RecordField::Municipio).copied(), Some(3.0_f32));
    assert_eq!(weights.get(&RecordField::Bairro).copied(), Some(2.0_f32));

    let b_values = &engine.scorer.field_b;
    assert_eq!(b_values.get(&RecordField::Numero).copied(), Some(0.0_f32));
    assert_eq!(b_values.get(&RecordField::Cep).copied(), Some(0.0_f32));
    assert_eq!(b_values.get(&RecordField::Rua).copied(), Some(0.75_f32));
    assert_eq!(
        b_values.get(&RecordField::Municipio).copied(),
        Some(0.5_f32)
    );
}
