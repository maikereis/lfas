use lfas::engine::SearchEngine;
use lfas::index::InvertedIndex;
use lfas::metadata::FieldMetadata;
use lfas::scorer::BM25FScorer;
use lfas::storage::InMemoryStorage;
use lfas::tokenizer::tokenize;
use lfas::{Record, RecordField, StructuredQuery};
use std::collections::HashMap;

#[test]
fn test_structured_address_search() {
    let _ = env_logger::builder().is_test(true).try_init();

    // Initialize Components
    let storage = InMemoryStorage::new();
    let mut index = InvertedIndex::new(storage);
    let mut metadata = FieldMetadata::new();

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
                let key = (field, token);
                *metadata.term_df.entry(key).or_insert(0) += 1;
            }
        }
    }

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

    // Test 1: CEP Search (Distinctive)
    println!("\n=== Test 1: CEP Search (Distinctive) ===");
    let query_cep = StructuredQuery {
        fields: vec![(RecordField::Cep, "66095-000".to_string())],
        top_k: 5,
        blocking_k: 10_000,
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

    // Test 2: Municipio Only (Fallback)
    println!("\n=== Test 2: Municipio Only (Fallback) ===");
    let query_municipio_only = StructuredQuery {
        fields: vec![(RecordField::Municipio, "Belem".to_string())],
        top_k: 5,
        blocking_k: 10_000,
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

    // Test 3: Municipio + Number Search
    println!("\n=== Test 3: Municipio + Number Search ===");
    let query_municipio = StructuredQuery {
        fields: vec![
            (RecordField::Municipio, "Belem".to_string()),
            (RecordField::Numero, "31".to_string()),
        ],
        top_k: 5,
        blocking_k: 10_000,
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

    // Test 4: Combined Search
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
