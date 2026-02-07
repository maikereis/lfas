use criterion::{Criterion, criterion_group, criterion_main};
use lfas::tokenizer::{tokenize, tokenize_structured};
use std::hint::black_box;

fn bench_tokenizer_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("Tokenizer Scaling");

    let small_text = "Rua Mauriti, 31. Belem, PA.";
    let large_text = "Rua Mauriti ".repeat(100);

    group.bench_function("tokenize_small", |b| {
        b.iter(|| tokenize(black_box(small_text)))
    });

    group.bench_function("tokenize_large", |b| {
        b.iter(|| tokenize(black_box(&large_text)))
    });

    group.bench_function("structured_overhead", |b| {
        b.iter(|| tokenize_structured(black_box(small_text)))
    });

    group.finish();
}

criterion_group!(benches, bench_tokenizer_scaling);
criterion_main!(benches);
