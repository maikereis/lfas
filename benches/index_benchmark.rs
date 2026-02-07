use criterion::{criterion_group, criterion_main, Criterion};
use std::hint::black_box;

use lfas::{Record, RecordField}; 
use lfas::index::InvertedIndex;
use lfas::tokenizer::tokenize;

fn setup_benchmark_data() -> (InvertedIndex<RecordField>, Vec<String>) {
    let mut idx = InvertedIndex::new();
    // Simulate 1000 documents to get dense enough bitmaps
    // Use tokenize() to ensure all tokens (including n-grams) are indexed
    for i in 0..1000 {
        // Index all tokens from "belem" including n-grams
        for token in tokenize("belem") {
            idx.add_term(i, RecordField::Municipio, token);
        }
        if i % 2 == 0 {
            // Index all tokens from "mauriti" including n-grams
            for token in tokenize("mauriti") {
                idx.add_term(i, RecordField::Rua, token);
            }
        }
    }
    // Return base tokens for bitmap retrieval
    (idx, vec!["belem".to_string(), "mauriti".to_string()])
}

fn bench_set_operations(c: &mut Criterion) {
    let (idx, terms) = setup_benchmark_data();
    let mut group = c.benchmark_group("Bitwise Operations");

    // Retrieve bitmaps once to avoid BTreeMap lookup overhead during the bench
    let bm1 = idx.term_bitmap(RecordField::Municipio, &terms[0]);
    let bm2 = idx.term_bitmap(RecordField::Rua, &terms[1]);
    let bitmaps = vec![bm1, bm2];

    group.bench_function("intersection", |b| {
        b.iter(|| {
            black_box(InvertedIndex::<RecordField>::intersect(black_box(&bitmaps)))
        })
    });

    group.bench_function("union", |b| {
        b.iter(|| {
            black_box(InvertedIndex::<RecordField>::union(black_box(&bitmaps)))
        })
    });

    group.finish();
}

fn bench_indexing(c: &mut Criterion) {
    let records = (0..100).map(|i| Record {
        id: i.to_string(),
        estado: "Para".to_string(),
        municipio: "Belem".to_string(),
        bairro: "Marco".to_string(),
        cep: "66095-000".to_string(),
        tipo_logradouro: "Travessa".to_string(),
        rua: "Mauriti".to_string(),
        numero: "31".to_string(),
        complemento: "Sala A".to_string(),
        nome: "Empresa X".to_string(),
    }).collect::<Vec<_>>();

    c.bench_function("index_add_term_100_records", |b| {
        b.iter(|| {
            let mut idx: InvertedIndex<RecordField> = InvertedIndex::new();
            for (i, record) in records.iter().enumerate() {
                for (field, text) in record.fields() {
                    // Use tokenize() which returns ALL tokens including n-grams
                    for token in tokenize(text) {
                        // black_box ensures the compiler doesn't optimize away the work
                        idx.add_term(black_box(i), black_box(field), black_box(token));
                    }
                }
            }
            idx
        })
    });
}

criterion_group!(benches, bench_indexing, bench_set_operations);
criterion_main!(benches);