use criterion::{criterion_group, criterion_main, Criterion};
use std::hint::black_box;
use std::collections::HashMap;
use rand::{SeedableRng, rngs::StdRng};

// Fake data imports
use fake::{Fake};
use fake::faker::address::raw::*;
use fake::locales::*;

use lfas::engine::SearchEngine;
use lfas::index::InvertedIndex;
use lfas::metadata::FieldMetadata;
use lfas::scorer::BM25FScorer;
use lfas::tokenizer::tokenize;
use lfas::{RecordField, StructuredQuery};

fn build_fake_engine(size: usize) -> SearchEngine {
    let mut index = InvertedIndex::new();
    let mut metadata = FieldMetadata::new();
    let mut rng = StdRng::seed_from_u64(42);
    
    for i in 0..size {
        let municipio: String = CityName(EN).fake_with_rng(&mut rng);
        let bairro: String = StateName(EN).fake_with_rng(&mut rng);
        let rua: String = StreetName(EN).fake_with_rng(&mut rng);
        let cep: String = PostCode(EN).fake_with_rng(&mut rng);

        metadata.total_docs += 1;
        let doc_entry = metadata.lengths.entry(i).or_default();

        let fields = [
            (RecordField::Municipio, municipio),
            (RecordField::Bairro, bairro),
            (RecordField::Rua, rua),
            (RecordField::Cep, cep),
        ];

        for (field, text) in fields {
            // Use tokenize() to get ALL tokens including n-grams
            let all_tokens: Vec<String> = tokenize(&text).into_iter().collect();
            let token_count = all_tokens.len();
            
            doc_entry.insert(field, token_count);
            *metadata.total_field_lengths.entry(field).or_insert(0) += token_count;
            
            for token in all_tokens {
                index.add_term(i, field, token);
            }
        }
    }

    // --- Injecting the "Uncommon Term" ---
    // We manually add a unique token to the very last document
    let unique_token = "zyxwvut_unique".to_string();
    let target_doc_id = size - 1;
    
    // Index the unique token with all its n-grams
    for token in tokenize(&unique_token) {
        index.add_term(target_doc_id, RecordField::Rua, token);
    }
    
    // Update metadata for that field to account for the extra tokens
    if let Some(doc_fields) = metadata.lengths.get_mut(&target_doc_id) {
        let unique_token_count = tokenize(&unique_token).len();
        *doc_fields.entry(RecordField::Rua).or_insert(0) += unique_token_count;
        *metadata.total_field_lengths.entry(RecordField::Rua).or_insert(0) += unique_token_count;
    }

    let mut field_weights = HashMap::new();
    field_weights.insert(RecordField::Rua, 2.0);
    field_weights.insert(RecordField::Municipio, 1.0);

    SearchEngine {
        index,
        metadata,
        scorer: BM25FScorer {
            k1: 1.2,
            field_weights,
            field_b: HashMap::new(),
        },
    }
}

fn bench_fake_load(c: &mut Criterion) {
    // Build once for 100k for faster iterative testing, or 1M for final stress test
    let size = 100_000; 
    let engine = build_fake_engine(size);
    let mut group = c.benchmark_group("Search_Density_Analysis");
    
    group.sample_size(10);

    // 1. Uncommon Term: Low Bitmap Density (The "Needle")
    group.bench_function("uncommon_term_search", |b| {
        b.iter(|| {
            let query = StructuredQuery {
                fields: vec![(RecordField::Rua, black_box("zyxwvut_unique".to_string()))],
                top_k: 10,
            };
            // Even with a strict blocking_k, this should be lightning fast
            engine.execute(query, 10) 
        })
    });

    // 2. Common Term: High Bitmap Density
    // Note: "street" is a common English word that should appear in many fake street names
    group.bench_function("common_term_search", |b| {
        b.iter(|| {
            let query = StructuredQuery {
                fields: vec![(RecordField::Rua, black_box("street".to_string()))],
                top_k: 10,
            };
            engine.execute(query, size / 10) 
        })
    });

    group.finish();
}

criterion_group!(benches, bench_fake_load);
criterion_main!(benches);