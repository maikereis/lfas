use criterion::{criterion_group, criterion_main, Criterion};
use std::sync::Arc;
use std::thread;
use lfas::engine::SearchEngine;
use lfas::storage::InMemoryStorage;
use lfas::{RecordField, StructuredQuery};

fn setup_engine_for_concurrency(size: usize) -> SearchEngine<RecordField, InMemoryStorage<RecordField>> {
    let storage = InMemoryStorage::new();
    let mut engine = SearchEngine::with_storage(storage);
    for i in 0..size {
        engine.index.add_term(i, RecordField::Rua, "street".to_string());
        engine.metadata.total_docs += 1;
    }
    engine
}

fn bench_parallel_queries(c: &mut Criterion) {
    let engine = Arc::new(setup_engine_for_concurrency(10_000));
    let mut group = c.benchmark_group("Concurrency");

    for n_threads in [2, 4].iter() {
        group.bench_with_input(format!("threads_{}", n_threads), n_threads, |b, &t| {
            b.iter(|| {
                let mut handles = vec![];
                for _ in 0..t {
                    let engine_share = Arc::clone(&engine);
                    handles.push(thread::spawn(move || {
                        let query = StructuredQuery {
                            fields: vec![(RecordField::Rua, "street".to_string())],
                            top_k: 5,
                        };
                        engine_share.execute(query, 50)
                    }));
                }
                for handle in handles {
                    let _ = handle.join();
                }
            })
        });
    }
    group.finish();
}

criterion_group!(benches, bench_parallel_queries);
criterion_main!(benches);