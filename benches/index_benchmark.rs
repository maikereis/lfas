use criterion::{criterion_group, criterion_main, Criterion, BatchSize};
use std::hint::black_box;
use lfas::RecordField; 
use lfas::index::InvertedIndex;
use lfas::storage::InMemoryStorage;

fn setup_dense_index(size: usize) -> InvertedIndex<RecordField, InMemoryStorage<RecordField>> {
    let storage = InMemoryStorage::new();
    let mut idx = InvertedIndex::new(storage);
    for i in 0..size {
        // Simulate common and rare terms
        idx.add_term(i, RecordField::Municipio, "belem".to_string());
        if i % 10 == 0 {
            idx.add_term(i, RecordField::Rua, format!("rua_{}", i));
        }
    }
    idx
}

fn bench_indexing_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("Index Operations");

    group.bench_function("add_term_single", |b| {
        b.iter_batched(
            || InvertedIndex::new(InMemoryStorage::new()),
            |mut idx| idx.add_term(black_box(1), RecordField::Rua, black_box("mauriti".to_string())),
            BatchSize::SmallInput,
        )
    });

    let idx = setup_dense_index(1000);
    let bm1 = idx.term_bitmap(RecordField::Municipio, "belem");
    let bm2 = idx.term_bitmap(RecordField::Rua, "rua_0");

    group.bench_function("bitmap_intersection", |b| {
        b.iter(|| {
            InvertedIndex::<RecordField, InMemoryStorage<RecordField>>::intersect(black_box(&[bm1.clone(), bm2.clone()]))
        })
    });

    group.finish();
}

criterion_group!(benches, bench_indexing_operations);
criterion_main!(benches);