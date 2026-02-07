use criterion::{criterion_group, criterion_main, Criterion};
use lfas::storage::{LmdbStorage, InMemoryStorage, PostingsStorage};
use lfas::postings::Postings;
use lfas::RecordField;
use tempfile::tempdir;

fn bench_storage_io(c: &mut Criterion) {
    let mut group = c.benchmark_group("Storage Backends");
    let dir = tempdir().unwrap();
    // 100MB map size, batch_size = 1 for immediate flushes in benchmark
    let mut lmdb = LmdbStorage::<RecordField>::open_with_batch_size(
        dir.path(), 
        1  // batch_size
    ).unwrap();
    let mut mem = InMemoryStorage::new();

    let mut postings = Postings::new();
    for i in 0..100 { 
        postings.add_doc(i); 
    }

    group.bench_function("lmdb_put_flush", |b| {
        b.iter(|| {
            lmdb.put(RecordField::Rua, "termo".into(), postings.clone()).unwrap();
        })
    });

    group.bench_function("in_memory_put", |b| {
        b.iter(|| {
            mem.put(RecordField::Rua, "termo".into(), postings.clone()).unwrap()
        })
    });

    group.finish();
}

criterion_group!(benches, bench_storage_io);
criterion_main!(benches);