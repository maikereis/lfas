# LFAS — Lightning Fast Address Search

[![Rust](https://img.shields.io/badge/rust-1.93+-orange.svg)](https://www.rust-lang.org/)
[![Python](https://img.shields.io/badge/python-3.13-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Build](https://img.shields.io/badge/build-passing-brightgreen.svg)]()

High-performance address search engine designed for fuzzy matching and partial queries. Built with Rust for speed and efficiency, with Python bindings for easy integration.

## Overview

LFAS implements a two-stage retrieval architecture inspired by ["Efficient Query Evaluation using a Two-Level Retrieval Process"](https://arxiv.org/abs/1708.01402) (Crane et al., 2017). The system uses distinctive tokens for candidate selection and BM25F scoring for ranking.

For a detailed explanation of the implementation and design decisions (in Portuguese), see: ["Como tornar buscas de endereços rápidas e precisas"](https://maikerar.substack.com/p/como-tornar-buscas-de-enderecos-rapidas).

## Features

- **Fuzzy Search** — Handles typos and partial address descriptions
- **Field-Aware Indexing** — Optimized for structured address data (street, city, state, ZIP, etc.)
- **BM25F Scoring** — Advanced relevance ranking with per-field weights and length normalisation
- **Persistent Storage** — LMDB-backed inverted index for fast disk I/O
- **High Throughput** — Batch indexing at 100K–200K docs/sec
- **Low Latency** — Sub-200ms query response times
- **Concurrent Search** — Multiple threads can search in parallel via `RwLock` (read-only lock during search)
- **Per-Instance Scoring** — Custom field weights per `SearchEngine` instance, no global state mutation
- **Token Strategies** — Distinctive tokens for candidate filtering, weak n-grams for scoring

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

## Python API

The main entry point is the `SearchEngine` class, exported from the `lfas` package. It wraps the Rust `PySearchEngine` with a Pythonic API, input validation, and context-manager support.

### Quick Start

```python
from lfas import SearchEngine

engine = SearchEngine()                   # default ./lmdb_data
engine = SearchEngine("./my_index")       # custom path

engine.index({"rua": "Avenida Paulista", "numero": "1578"}, doc_id=0)
engine.flush()

results = engine.search({"rua": "Paulista"}, top_k=10)
for doc_id, score in results:
    print(doc_id, score)
```

### Context Manager

```python
with SearchEngine("./my_index") as engine:
    engine.index_batch(records)
    engine.flush()
    results = engine.search(query)
```

Calling `flush()` on `__exit__` guarantees durability even if no explicit call was made.

### SearchEngine Methods

| Method | Lock | Description |
|--------|------|-------------|
| `index(document, doc_id)` | write | Index a single document |
| `index_batch(documents)` | write | Bulk index; 10–20× faster than single indexing |
| `flush()` | write | Commit pending LMDB writes |
| `search(query, top_k, blocking_k)` | **read** | Field-aware BM25F search (thread-safe) |
| `set_weights(weights)` | — | Set per-instance BM25F field weights |
| `set_b_values(b_values)` | — | Set per-instance length normalisation parameters |
| `reset_weights()` | — | Revert to engine defaults |
| `get_weights()` | read | Return effective weights for this instance |
| `save_metadata(path)` | read | Persist index metadata to disk |
| `load_metadata(path)` | write | Load saved metadata (required before searching a cold index) |
| `init_logging()` *(static)* | — | Bridge Rust log output into Python `logging` |

**Properties:** `db_path`, `total_docs`, `stats`

### Concurrency Model

`search()` acquires only a **read** lock on the global engine, so multiple Python threads can run searches truly in parallel. Write operations (`index`, `index_batch`, `flush`, `load_metadata`) acquire an exclusive write lock.

```python
from concurrent.futures import ThreadPoolExecutor

with ThreadPoolExecutor(max_workers=8) as pool:
    futures = [pool.submit(engine.search, q, top_k=5) for q in queries]
    results = [f.result() for f in futures]
```

### Custom Weights

Field weights and b-values are stored **per instance** and never written to the global engine, so different `SearchEngine` objects can use different configurations concurrently.

```python
engine = (
    SearchEngine("./my_index")
    .set_weights({"cep": 15.0, "numero": 12.0, "rua": 5.0})
    .set_b_values({"rua": 0.75, "cep": 0.0})
)
```

### Logging

```python
import logging
logging.basicConfig(level=logging.INFO)
SearchEngine.init_logging()   # bridge Rust log → Python logging
```

## Usage Examples

### Indexing a Dataset

```python
import time
import pandas as pd
from pathlib import Path
from lfas import SearchEngine

index_folder = Path("./my_index")
metadata_file = index_folder / "metadata.bin"

engine = SearchEngine(db_path=index_folder)

df = pd.read_csv("addresses.csv")
chunk_size = 500_000

for i in range(0, len(df), chunk_size):
    chunk = df.iloc[i : i + chunk_size]
    batch_data = [
        (int(row["id"]), {k: str(v) for k, v in row.items() if pd.notna(v) and k != "id"})
        for _, row in chunk.iterrows()
    ]
    engine.index_batch(batch_data)
    print(f"Indexed {i + len(chunk):,}/{len(df):,} docs")

engine.flush()
engine.save_metadata(metadata_file)
```

### Searching

```python
from pathlib import Path
from lfas import SearchEngine

engine = SearchEngine(db_path="./my_index")
engine.load_metadata("./my_index/metadata.bin")

results = engine.search(
    query={"rua": "Mauriti", "municipio": "Belem", "numero": "31"},
    top_k=10,
    blocking_k=1000,
)

for doc_id, score in results:
    print(f"doc_id={doc_id}  score={score:.4f}")
```

> **Note:** `load_metadata()` must be called before `search()` when working with a pre-built index after a process restart.

## Data Format

Upload a CSV file with address data. Required columns:

> You can obtain address data from the [National Address Registry for Statistical Purposes](https://github.com/maikereis/CNEFE-data), a comprehensive database of georeferenced residential addresses across Brazil.

| Column            | Description        |
|-------------------|--------------------|
| `id`              | Address identifier |
| `rua`             | Street name        |
| `municipio`       | City               |
| `estado`          | State              |
| `cep`             | ZIP code           |
| `bairro`          | Neighborhood       |
| `tipo_logradouro` | Street type        |
| `numero`          | House number       |
| `complemento`     | Address complement |
| `nome`            | Name / identifier  |

## Tokenization Strategy

The tokenizer performs Unicode normalisation (NFD, lowercase) and produces two disjoint token sets per field.

### Distinctive Tokens (Candidate Filtering)

Used in Round 1 to narrow the candidate set:

- CEP patterns: `66095-000`
- House numbers: `31`, `500`
- State abbreviations: `PA`, `MA`
- N-grams with address types: `rua 123`, `br 010`

Stopwords (Portuguese NLTK + custom address prepositions) are stripped before classification.

### Weak Tokens / All Tokens (Scoring)

3-character byte n-grams from all remaining tokens, plus the tokens themselves. Feeds the BM25F scorer in Round 2 and improves recall for partial or abbreviated inputs.

### Example

Input: `"Travessa Mauriti 31 Belem PA"`

- **Distinctive**: `["31", "pa", "travessa 31"]`
- **All**: `["travessa", "mauriti", "31", "belem", "pa", "mau", "uri", "iti", ...]`

> The token `"000"` (trailing CEP suffix) is explicitly suppressed from candidate retrieval to avoid massive candidate sets.

## Two-Stage Search

**Round 1 — Candidate Retrieval**

For each query field, distinctive token bitmaps are **intersected** (AND) within the field and **unioned** (OR) across fields. If no distinctive tokens match, the engine falls back to the *k* rarest tokens by document frequency to avoid returning empty results.

**Round 2 — BM25F Scoring**

All candidates are scored using the full BM25F formula. Postings are loaded in a **single batched LMDB read transaction**, and term contributions are accumulated into a per-document score map. Results are sorted descending and truncated to `top_k`.

## Configuration

### BM25F Field Weights

Default weights (tuned for Brazilian address matching; overridable via `set_weights()`):

| Field             | Weight | b    | Rationale                         |
|-------------------|--------|------|-----------------------------------|
| `numero`          | 10.0   | 0.0  | Most selective; fixed-length      |
| `cep`             | 8.0    | 0.0  | Highly selective; fixed-length    |
| `rua`             | 5.0    | 0.75 | Important but variable length     |
| `municipio`       | 3.0    | 0.5  | Moderate selectivity              |
| `bairro`          | 2.0    | 0.75 | Useful disambiguation             |
| `complemento`     | 1.5    | 0.5  | Supplemental info                 |
| `estado`          | 1.0    | 0.0  | Low selectivity; short token      |
| `nome`            | 1.0    | 0.75 | Person / entity name              |
| `tipo_logradouro` | 0.5    | 0.0  | Very low selectivity              |

`k1 = 1.2` (BM25 term frequency saturation constant).

### LMDB Settings

Configurable in `src/storage/lmdb.rs`:

```rust
pub const BATCH_SIZE: usize = 100_000;                  // Write buffer size
pub const MAP_SIZE: usize = 10 * 1024 * 1024 * 1024;   // 10 GB
```

## Performance

- **Indexing throughput:** ~100K–200K docs/sec (batch mode, recommended batch size 100K–500K)
- **Search latency:** under 200ms for most queries

### Benchmark Results

```
cargo bench --bench search_benchmark

single_field_rare_term     time: [~145 us]
multi_field_common_terms   time: [~295 us]
```

## Project Structure

```
lfas/
├── src/
│   ├── engine.rs           Search engine core: candidate retrieval + BM25F scoring
│   ├── index.rs            Inverted index (add_term, add_batch, bitmap operations)
│   ├── lib.rs              Library root, RecordField enum, type definitions
│   ├── metadata.rs         FieldMetadata: doc lengths, avg lengths, term DFs
│   ├── postings.rs         Posting lists (RoaringBitmap + frequency map)
│   ├── python.rs           PyO3 Python bindings (PySearchEngine, GLOBAL_ENGINE)
│   ├── scorer.rs           BM25F scorer (TAAT with batched LMDB reads)
│   ├── timing.rs           Timer / TimingStats for performance instrumentation
│   ├── tokenizer.rs        Text processing, n-gram generation, stopword removal
│   └── storage/
│       ├── lmdb.rs         LMDB persistent storage backend
│       ├── memory.rs       In-memory storage backend (tests)
│       └── mod.rs          PostingsStorage trait
├── benches/                Criterion benchmark suites
├── tests/                  Integration tests
├── app/                    Streamlit web interface
├── python/lfas/            Python package (__init__.py, SearchEngine wrapper)
├── Cargo.toml
└── pyproject.toml
```

## Development

```bash
make test       # Run all unit and integration tests
make check      # Lint with Clippy and verify formatting
make bench      # Run default benchmark
make doc        # Generate and open Rust documentation
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

### Generating Python Stubs

Type stubs (`lfas.pyi`) are generated from `#[gen_stub_pyclass]` / `#[gen_stub_pymethods]` annotations:

```bash
cargo run --bin stub_gen
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
