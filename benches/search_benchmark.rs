use criterion::{criterion_group, criterion_main, Criterion};
use std::hint::black_box;
use rand::{SeedableRng, rngs::StdRng};
use fake::{Fake, faker::address::raw::*, locales::*};

use lfas::engine::SearchEngine;
use lfas::storage::InMemoryStorage;
use lfas::tokenizer::tokenize;
use lfas::{RecordField, StructuredQuery};

fn build_bench_engine(size: usize) -> SearchEngine<RecordField, InMemoryStorage<RecordField>> {
    let storage = InMemoryStorage::new();
    let mut engine = SearchEngine::with_storage(storage);
    let mut rng = StdRng::seed_from_u64(42);
    
    // Default weight configuration for benchmark
    engine.scorer.field_weights.insert(RecordField::Rua, 1.0);
    engine.scorer.field_weights.insert(RecordField::Municipio, 0.5);

    for i in 0..size {
        let municipio: String = CityName(EN).fake_with_rng(&mut rng);
        let rua: String = StreetName(EN).fake_with_rng(&mut rng);

        engine.metadata.total_docs += 1;
        
        let fields = [(RecordField::Municipio, municipio), (RecordField::Rua, rua)];
        let doc_entry = engine.metadata.lengths.entry(i).or_default();

        for (field, text) in fields {
            let tokens = tokenize(&text);
            doc_entry.insert(field, tokens.len());
            *engine.metadata.total_field_lengths.entry(field).or_insert(0) += tokens.len();
            
            for token in tokens {
                engine.index.add_term(i, field, token.clone());
                *engine.metadata.term_df.entry((field, token)).or_insert(0) += 1;
            }
        }
    }
    engine
}

fn bench_search_scenarios(c: &mut Criterion) {
    let engine = build_bench_engine(50_000);
    let mut group = c.benchmark_group("Search Engine Scenarios");
    
    group.sample_size(50);

    group.bench_function("single_field_rare_term", |b| {
        let query = StructuredQuery {
            fields: vec![(RecordField::Rua, "unique_path_777".to_string())],
            top_k: 10,
        };
        b.iter(|| engine.execute(black_box(query.clone()), 100))
    });

    group.bench_function("multi_field_common_terms", |b| {
        let query = StructuredQuery {
            fields: vec![
                (RecordField::Rua, "Street".to_string()),
                (RecordField::Municipio, "London".to_string()),
            ],
            top_k: 10,
        };
        b.iter(|| engine.execute(black_box(query.clone()), 100))
    });

    group.finish();
}

criterion_group!(benches, bench_search_scenarios);
criterion_main!(benches);