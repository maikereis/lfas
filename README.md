# LFAS — Lightning Fast Address Search

[![Rust](https://img.shields.io/badge/rust-1.70+-orange.svg)](https://www.rust-lang.org/)
[![Python](https://img.shields.io/badge/python-3.13-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Build](https://img.shields.io/badge/build-passing-brightgreen.svg)]()

High-performance address search engine designed for fuzzy matching and partial queries. Built with Rust for speed and efficiency, with Python bindings for easy integration.

## Overview

LFAS implements a two-stage retrieval architecture inspired by ["Efficient Query Evaluation using a Two-Level Retrieval Process"](https://arxiv.org/abs/1708.01402) (Crane et al., 2017). The system uses distinctive tokens for candidate selection and comprehensive token matching for ranking.

For a detailed explanation of the implementation and design decisions (in Portuguese), see: ["Como tornar buscas de endereços rápidas e precisas"](https://maikerar.substack.com/p/como-tornar-buscas-de-enderecos-rapidas).

## Features

- **Fuzzy Search** — Handles typos and partial address descriptions
- **Field-Aware Indexing** — Optimized for structured address data (street, city, state, ZIP, etc.)
- **BM25F Scoring** — Advanced relevance ranking with field-specific weights
- **Persistent Storage** — LMDB-backed index for fast disk I/O
- **High Throughput** — Batch indexing at 100K+ docs/sec
- **Low Latency** — Sub-200ms query response times
- **Token Strategies** — Distinctive tokens for candidate filtering, weak tokens (n-grams) for scoring

## Architecture
```
                +------------------+
                |   Streamlit UI   |
                +--------+---------+
                         |
                    +----+-----+
                    |  Python  |
                    | Bindings |
                    +----+-----+
                         |
    +--------------------+------------------+
    |         Rust Core Engine              |
    +---------------------------------------+
    |  Tokenizer --> Index --> Scorer       |
    |  - N-grams & Phrases                  |
    |  - Inverted Index (LMDB)              |
    |  - BM25F Ranking                      |
    +---------------------------------------+
```

## Prerequisites

- Rust 1.93+
- Python 3.13+
- Make

## Installation
```bash
git clone <repository-url>
cd lfas
make develop
uv sync
```

## Building
```bash
source .venv/bin/activate
make develop   # Development mode
make release   # Optimized release
```

## Running the Application
```bash
streamlit run app.py
```

Access the web interface at `http://localhost:8501`.

## Usage

### Indexing Documents

Upload a CSV file with address data. Required columns:

| Column            | Description        |
|-------------------|--------------------|
| `rua`             | Street name        |
| `municipio`       | City               |
| `estado`          | State              |
| `cep`             | ZIP code           |
| `bairro`          | Neighborhood       |
| `tipo_logradouro` | Street type        |
| `numero`          | House number       |
| `complemento`     | Address complement |
| `nome`            | Name / identifier  |

### Searching Addresses
```python
from lfas import SearchEngine

engine = SearchEngine()
engine.load_metadata("./lmdb_data/metadata.bin")

results = engine.search(
    query={"rua": "Mauriti", "municipio": "Belem", "numero": "31"},
    top_k=10,
    blocking_k=1000
)

for doc_id, score in results:
    print(f"Document {doc_id}: {score:.2f}")
```

## Tokenization Strategy

### Distinctive Tokens (Candidate Filtering)

Used in Round 1 to narrow the candidate set via union operations:

- CEP patterns: `66095-000`
- House numbers: `31`, `500`
- State abbreviations: `PA`, `MA`
- N-grams with address types: `rua 123`, `br 010`

### Weak Tokens (Scoring Only)

3-character n-grams from all tokens. Improves recall for partial matches and feeds the BM25F scorer in Round 2.

### Example

Input: `"Travessa Mauriti 31 Belem PA"`

- **Distinctive**: `["31", "pa", "travessa 31"]`
- **All**: `["travessa", "mauriti", "31", "belem", "pa", "mau", "uri", "iti", ...]`

## Performance

Indexing throughput: ~9,000+ docs/sec at ~500 bytes/doc (compressed). Search latency: under 200ms for most queries.

### Benchmark Results
```
cargo bench --bench search_benchmark

single_field_rare_term     time: [~145 us]
multi_field_common_terms   time: [~295 us]
```

## Two-Stage Search

1. **Round 1 — Candidate Retrieval**: Union of posting lists for distinctive tokens produces the candidate set.
2. **Round 2 — Ranking**: BM25F scores all candidates using the full token set, including weak n-grams.

## Configuration

### BM25F Field Weights

Default weights (configurable in `src/engine.rs`):

| Field             | Weight |
|-------------------|--------|
| `numero`          | 10.0   |
| `cep`             | 8.0    |
| `rua`             | 5.0    |
| `municipio`       | 3.0    |
| `bairro`          | 2.0    |
| `complemento`     | 1.5    |
| `estado`          | 1.0    |
| `nome`            | 1.0    |
| `tipo_logradouro` | 0.5    |

### Length Normalization (b values)

Fixed-length identifier fields (`cep`, `estado`, `numero`) default to `b=0.0`. Free-text fields use `b=0.75`. Configurable in `src/engine.rs`.

### LMDB Settings

Configurable in `src/storage/lmdb.rs`:
```rust
pub const BATCH_SIZE: usize = 100_000;                  // Write buffer size
pub const MAP_SIZE: usize = 10 * 1024 * 1024 * 1024;   // 10 GB
```

## Development
```bash
make test       # Run all unit and integration tests
make check      # Lint with Clippy and verify formatting
make bench      # Run default benchmark
make doc        # Generate and open documentation
make clean      # Remove build artifacts
```

### Benchmark Suite
```bash
cargo bench --bench index_benchmark        # Indexing performance
cargo bench --bench search_benchmark       # Search performance
cargo bench --bench tokenizer_benchmark    # Tokenizer performance
cargo bench --bench persistance_benchmark  # Storage I/O
cargo bench --bench concurrency_benchmark  # Concurrent reads
```

## Project Structure
```
lfas/
├── src/
|   ├── engine.rs           Search engine core logic
|   ├── index.rs            Inverted index implementation
|   ├── lib.rs              Library root and type definitions
|   ├── metadata.rs         Document statistics and field lengths
|   ├── postings.rs         Posting lists (bitmaps + frequencies)
|   ├── python.rs           PyO3 Python bindings
|   ├── scorer.rs           BM25F ranking algorithm
|   ├── timing.rs           Performance instrumentation
|   ├── tokenizer.rs        Text processing and n-gram generation
|   ├── storage/
|   |   ├── lmdb.rs         LMDB persistent storage backend
|   |   ├── memory.rs       In-memory storage backend (tests)
|   |   └── mod.rs          Storage trait definition
|   └── bin
|       └── stub_gen.rs     Python stub generator
├── benches/                Criterion benchmark suites
├── tests/                  Integration tests
├── app/                    Streamlit web interface
├── python/lfas/            Python package and SearchEngine wrapper
├── Cargo.toml
└── pyproject.toml
```

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/my-improvement`
3. Commit your changes: `git commit -am 'Add my improvement'`
4. Push: `git push origin feature/my-improvement`
5. Open a Pull Request

## License

MIT License — see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Built with [Rust](https://www.rust-lang.org/) and [PyO3](https://pyo3.rs/)
- Persistent storage via [LMDB](http://www.lmdb.tech/) through the [heed](https://github.com/meilisearch/heed) crate
- Web UI powered by [Streamlit](https://streamlit.io/)
- Benchmarking with [Criterion.rs](https://github.com/bheisler/criterion.rs)

## References

Crane, M., Trotman, A., & O'Keefe, R. (2017). *Efficient Query Evaluation using a Two-Level Retrieval Process*. arXiv:1708.01402. <https://arxiv.org/abs/1708.01402>

Reis, M. (2025). *Como tornar buscas de endereços rápidas e precisas*. Substack. <https://maikerar.substack.com/p/como-tornar-buscas-de-enderecos-rapidas>

---

> This project is optimized for Brazilian address data. To adapt it for other address formats, modify the tokenizer rules in `src/tokenizer.rs`.