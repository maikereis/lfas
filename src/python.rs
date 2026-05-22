//! Python bindings for the LFAS search engine (PyO3 layer).
//!
//! This module exposes [`PySearchEngine`], a PyO3 class that wraps the Rust
//! [`SearchEngine`] behind a process-wide singleton (`GLOBAL_ENGINE`).
//!
//! ## Singleton pattern
//!
//! A single `SearchEngine<RecordField, LmdbStorage>` lives inside
//! `GLOBAL_ENGINE`.  All `PySearchEngine` Python objects share it.  This
//! mirrors how Python code typically works: one process, one database path,
//! many references.  Switching paths (rare) creates a new engine and discards
//! the old one.
//!
//! ## Locking strategy
//!
//! | Operation              | Lock mode | Reason                                      |
//! |------------------------|-----------|---------------------------------------------|
//! | `new` (path change)    | write     | Replaces the global engine                  |
//! | `index_dict`           | write     | Mutates index and metadata                  |
//! | `index_batch`          | write     | Mutates index and metadata                  |
//! | `flush`                | write     | Commits LMDB write buffer                   |
//! | `load_metadata`        | write     | Replaces `engine.metadata`                  |
//! | `search_complex`       | **read**  | Read-only; scorer is per-call stack value   |
//! | `get_weights`          | read      | Read-only                                   |
//! | `get_total_docs`       | read      | Read-only                                   |
//! | `get_stats`            | read      | Read-only                                   |
//! | `save_metadata`        | read      | Serialises metadata without modifying it    |
//!
//! Because `search_complex` holds only a read lock, any number of Python
//! threads can search simultaneously.  The GIL does **not** provide the
//! serialisation here — the `RwLock` does, and reads are not exclusive.
//!
//! ## Custom weights
//!
//! Weights and b-values are stored on each `PySearchEngine` *instance*, not on
//! the global engine.  `search_complex` builds a throw-away [`BM25FScorer`] on
//! the stack from these per-instance values and passes it to
//! [`SearchEngine::execute_with_scorer`].  The global scorer is never written
//! during a search.

use crate::engine;
use crate::scorer::BM25FScorer;
use crate::storage::PostingsStorage;
use crate::timing::Timer;
use crate::tokenizer::tokenize;
use crate::{RecordField, StructuredQuery, engine::SearchEngine, storage::LmdbStorage};
use bincode::{deserialize_from, serialize_into};
use log::{debug, info};
use once_cell::sync::Lazy;
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::sync::{Arc, RwLock};

/// Process-wide singleton holding the active `SearchEngine` and the LMDB path
/// it was opened with.
///
/// Wrapped in `Arc<RwLock<…>>` so that:
/// * multiple Python threads can hold read locks simultaneously (concurrent
///   searches),
/// * write operations (indexing, flush, metadata load) are exclusive.
///
/// `Lazy` ensures the `RwLock` is initialised exactly once, the first time any
/// `PySearchEngine` is constructed.
static GLOBAL_ENGINE: Lazy<
    Arc<RwLock<Option<(SearchEngine<RecordField, LmdbStorage<RecordField>>, String)>>>,
> = Lazy::new(|| Arc::new(RwLock::new(None)));

/// High-performance BM25F search engine for Brazilian address data.
///
/// `PySearchEngine` provides a Python interface to a Rust-based inverted index
/// with LMDB storage, optimised for field-aware address searching.
///
/// ## Singleton model
///
/// All `PySearchEngine` instances share a single global
/// `SearchEngine<RecordField, LmdbStorage>` (see `GLOBAL_ENGINE`).  Creating a
/// second instance pointing to the **same** path reuses the existing engine at
/// zero cost.  Creating one pointing to a **different** path replaces the
/// global engine (rare; intended for testing or path migration).
///
/// ## Concurrency model
///
/// `search_complex` holds only a *read* lock on the global engine, so multiple
/// Python threads can run searches truly in parallel.  Write operations
/// (`index_dict`, `index_batch`, `flush`, `load_metadata`) hold an exclusive
/// write lock and will block — and be blocked by — any concurrent reader.
///
/// ## Custom scoring
///
/// Field weights and b-values are stored **per instance** in `custom_weights`
/// and `custom_b_values`.  They are applied at search time by constructing a
/// throw-away [`BM25FScorer`] on the stack; the global engine's scorer is never
/// modified.  This means two `PySearchEngine` objects with different weights can
/// search the same index concurrently without interference.
///
/// ## Examples
///
/// ```python
/// from lfas import PySearchEngine
/// engine = PySearchEngine()                          # default ./lmdb_data
/// engine = PySearchEngine(db_path="./my_index")      # custom path
/// engine.index_dict(0, {
///     'rua': 'Avenida Paulista',
///     'numero': '1578',
///     'municipio': 'São Paulo',
/// })
/// engine.flush()
/// results = engine.search_complex({'rua': 'Paulista'}, top_k=10, blocking_k=1000)
/// ```
#[gen_stub_pyclass]
#[pyclass]
pub struct PySearchEngine {
    /// Per-instance custom field importance weights (`w_f` in BM25F).
    ///
    /// `None` means "use the global engine's defaults".  When `Some`, these
    /// values are merged with the engine defaults (missing fields fall back to
    /// defaults) at the start of each `search_complex` call.  The global
    /// engine's `scorer.field_weights` is **never** written.
    custom_weights: Option<HashMap<RecordField, f32>>,

    /// Per-instance BM25F length-normalisation parameters (`b_f`).
    ///
    /// `None` means "use the global engine's defaults".  Semantics mirror
    /// `custom_weights`.  Setting `b = 0.0` disables length normalisation for
    /// that field (appropriate for fixed-length fields like CEP or estado).
    custom_b_values: Option<HashMap<RecordField, f32>>,

    /// The LMDB path this instance was created with.
    ///
    /// Stored for informational purposes (repr, Python wrapper defaults for
    /// metadata paths).  The authoritative path lives inside `GLOBAL_ENGINE`.
    #[allow(dead_code)]
    db_path: String,
}

#[gen_stub_pymethods]
#[pymethods]
impl PySearchEngine {
    /// Initialize Rust logging integration with Python.
    ///
    /// This static method should be called once at application startup to enable
    /// Rust log messages to appear in Python logging output.
    ///
    /// Examples
    /// --------
    /// >>> PySearchEngine.init_logging()
    #[staticmethod]
    #[pyo3(text_signature = "()")]
    fn init_logging() {
        let _ = pyo3_log::try_init();
    }

    /// Create a new `PySearchEngine` instance.
    ///
    /// On the first call (or when `db_path` differs from the currently open
    /// database), opens an LMDB environment at `db_path` and stores it in
    /// `GLOBAL_ENGINE`.  Subsequent calls with the **same** path reuse the
    /// existing environment without any I/O.
    ///
    /// The write lock on `GLOBAL_ENGINE` is held only for the duration of
    /// engine initialisation and is released before the constructor returns.
    /// Concurrent searches in other threads are therefore blocked only briefly
    /// during the first construction.
    ///
    /// Parameters
    /// ----------
    /// db_path : str, optional
    ///     Path to the LMDB database directory.  The directory is created by
    ///     the Python wrapper (`SearchEngine.__init__`) before this constructor
    ///     is called, so it is expected to exist by the time Rust sees it.
    ///     Default: `"./lmdb_data"`.
    ///
    /// Panics
    /// ------
    /// Panics if LMDB fails to open the environment (e.g. path is not a
    /// directory, insufficient file-descriptor limit, or corrupt database).
    ///
    /// Examples
    /// --------
    /// >>> engine = PySearchEngine()
    /// >>> engine = PySearchEngine(db_path="./my_custom_index")
    #[new]
    #[pyo3(signature = (db_path = "./lmdb_data"))]
    fn new(db_path: &str) -> Self {
        info!("[RUST] PySearchEngine::new() called with db_path: {}", db_path);
        let timer = Timer::new("PySearchEngine::new");

        // Write lock only during initialization; released before any search.
        let mut global = GLOBAL_ENGINE.write().unwrap();

        match global.as_ref() {
            Some((_, existing_path)) if existing_path == db_path => {
                info!("[RUST] Reusing existing LMDB storage at {}", db_path);
            }
            Some((_, existing_path)) => {
                info!(
                    "[RUST] Different path requested. Old: {}, New: {}",
                    existing_path, db_path
                );
                let storage = LmdbStorage::<RecordField>::open(std::path::Path::new(db_path))
                    .expect("Failed to open LMDB storage");
                *global = Some((engine::SearchEngine::with_storage(storage), db_path.to_string()));
            }
            None => {
                info!("[RUST] Creating new LMDB storage (first time) at {}", db_path);
                let storage = LmdbStorage::<RecordField>::open(std::path::Path::new(db_path))
                    .expect("Failed to open LMDB storage");
                *global = Some((engine::SearchEngine::with_storage(storage), db_path.to_string()));
            }
        }
        drop(global); // Release write lock immediately.

        drop(timer);
        info!("[RUST] PySearchEngine created successfully");

        PySearchEngine {
            custom_weights: None,
            custom_b_values: None,
            db_path: db_path.to_string(),
        }
    }

    /// Set custom field importance weights for BM25F scoring.
    ///
    /// Weights are stored on this `PySearchEngine` instance only — the global
    /// engine is never touched — so different Python objects can use different
    /// weights concurrently without any locking.
    ///
    /// Parameters
    /// ----------
    /// weights : dict[str, float]
    ///     Field names → weight values.
    ///     Valid fields: 'estado', 'municipio', 'bairro', 'cep',
    ///     'tipo_logradouro', 'rua', 'numero', 'complemento', 'nome'
    ///
    /// Notes
    /// -----
    /// Default weights:
    ///   numero: 10.0, cep: 8.0, rua: 5.0, municipio: 3.0, bairro: 2.0,
    ///   complemento: 1.5, estado: 1.0, nome: 1.0, tipo_logradouro: 0.5
    #[pyo3(text_signature = "(self, weights)")]
    fn set_field_weights(&mut self, weights: HashMap<String, f32>) {
        let mut field_weights = HashMap::new();
        for (field_name, weight) in weights {
            if let Some(field) = self.map_field(&field_name) {
                field_weights.insert(field, weight);
                info!("[RUST] Set weight for {:?}: {}", field, weight);
            } else {
                info!("[RUST] Warning: Unknown field '{}'", field_name);
            }
        }
        self.custom_weights = Some(field_weights);
        info!(
            "[RUST] Custom weights configured for {} fields",
            self.custom_weights.as_ref().unwrap().len()
        );
    }

    /// Set length normalization (b) parameters for BM25F scoring.
    ///
    /// Like `set_field_weights`, these are stored per-instance and never
    /// written to the global engine, enabling concurrent searches with
    /// different configurations.
    ///
    /// Parameters
    /// ----------
    /// b_values : dict[str, float]
    ///     Field names → b values (0.0 = no normalisation, 1.0 = full).
    ///
    /// Notes
    /// -----
    /// Default b-values:
    ///   numero, cep, estado, tipo_logradouro: 0.0
    ///   municipio, complemento: 0.5
    ///   rua, bairro, nome: 0.75
    #[pyo3(text_signature = "(self, b_values)")]
    fn set_field_b_values(&mut self, b_values: HashMap<String, f32>) {
        let mut field_b = HashMap::new();
        for (field_name, b_value) in b_values {
            if let Some(field) = self.map_field(&field_name) {
                field_b.insert(field, b_value);
                info!("[RUST] Set b-value for {:?}: {}", field, b_value);
            } else {
                info!("[RUST] Warning: Unknown field '{}'", field_name);
            }
        }
        self.custom_b_values = Some(field_b);
        info!(
            "[RUST] Custom b-values configured for {} fields",
            self.custom_b_values.as_ref().unwrap().len()
        );
    }

    /// Reset field weights and b-values to engine defaults.
    ///
    /// Examples
    /// --------
    /// >>> engine.reset_weights()
    #[pyo3(text_signature = "(self)")]
    fn reset_weights(&mut self) {
        self.custom_weights = None;
        self.custom_b_values = None;
        info!("[RUST] Reset to default weights");
    }

    /// Return the effective field weight configuration for this instance.
    ///
    /// If custom weights have been set via `set_field_weights`, those are
    /// returned.  Otherwise the global engine's default weights are returned.
    /// In both cases the map is keyed by lowercase field-name strings
    /// (e.g. `"rua"`, `"cep"`).
    ///
    /// This method acquires a **read** lock and is safe to call concurrently.
    ///
    /// Returns
    /// -------
    /// dict[str, float]
    ///     Field name → effective weight value.
    ///
    /// Examples
    /// --------
    /// >>> weights = engine.get_weights()
    /// >>> print(weights['cep'])
    /// 8.0
    #[pyo3(text_signature = "(self)")]
    fn get_weights(&self) -> HashMap<String, f32> {
        let global = GLOBAL_ENGINE.read().unwrap();
        let (engine, _) = global.as_ref().expect("Engine not initialized");

        let weights = if let Some(ref custom) = self.custom_weights {
            custom.clone()
        } else {
            engine.scorer.field_weights.clone()
        };

        weights
            .into_iter()
            .map(|(field, weight)| (format!("{:?}", field).to_lowercase(), weight))
            .collect()
    }

    /// Index multiple documents in a single batch operation.
    ///
    /// Preferred over `index_dict` for bulk ingestion; it is 10–20× faster
    /// because it aggregates all (field, term) → doc_id mappings in memory
    /// first and then performs exactly **one LMDB read + one write per unique
    /// term** in the batch, instead of a read-modify-write for every token of
    /// every document.
    ///
    /// This method acquires a **write** lock for its entire duration.
    ///
    /// Parameters
    /// ----------
    /// records : list[tuple[int, dict[str, str]]]
    ///     Each element is `(doc_id, field_dict)` where:
    ///     - `doc_id` is a non-negative integer unique identifier.
    ///     - `field_dict` maps field names to string values.  Unknown field
    ///       names are silently ignored by `map_field`.
    ///
    /// Notes
    /// -----
    /// - Call `flush()` after each batch to commit the LMDB write buffer.
    /// - Metadata (`lengths`, `total_field_lengths`, `term_df`) is updated
    ///   within the same write lock, so readers see a consistent snapshot.
    /// - Recommended batch size: 100 000–500 000 documents for optimal
    ///   throughput (≈ 100 000–200 000 docs/sec on typical hardware).
    ///
    /// Examples
    /// --------
    /// >>> batch = [
    /// ...     (0, {'rua': 'Rua A', 'municipio': 'Belém'}),
    /// ...     (1, {'rua': 'Rua B', 'municipio': 'Belém'}),
    /// ... ]
    /// >>> engine.index_batch(batch)
    /// >>> engine.flush()
    #[pyo3(text_signature = "(self, records)")]
    fn index_batch(&mut self, records: Vec<(usize, HashMap<String, String>)>) {
        let mut global = GLOBAL_ENGINE.write().unwrap();
        let (engine, _) = global.as_mut().expect("Engine not initialized");

        let mut batch_accumulator: HashMap<(RecordField, String), Vec<usize>> = HashMap::new();

        for (doc_id, record_dict) in records {
            for (field_name, value) in record_dict {
                if let Some(field) = self.map_field(&field_name) {
                    let tokens = tokenize(&value);
                    let token_count = tokens.len();
                    for term in tokens {
                        batch_accumulator.entry((field, term)).or_default().push(doc_id);
                    }
                    engine
                        .metadata
                        .lengths
                        .entry(doc_id)
                        .or_default()
                        .insert(field, token_count);
                    *engine
                        .metadata
                        .total_field_lengths
                        .entry(field)
                        .or_insert(0) += token_count;
                }
            }
            engine.metadata.total_docs += 1;
        }

        for ((field, term), mut doc_ids) in batch_accumulator {
            doc_ids.sort_unstable();
            doc_ids.dedup();

            let mut postings = engine
                .index
                .storage
                .get(field, &term)
                .unwrap_or_default()
                .unwrap_or_else(crate::postings::Postings::new);

            for id in doc_ids {
                postings.add_occurrence(id);
            }

            let key = (field, term.clone());
            engine.metadata.term_df.insert(key, postings.len());
            engine.index.storage.put(field, term, postings).unwrap();
        }
    }

    /// Index a single document with field-value pairs.
    ///
    /// Tokenises each field value with the address-aware tokeniser, updates the
    /// inverted index, and records per-document field lengths and term document
    /// frequencies used by the BM25F scorer.
    ///
    /// For bulk ingestion use `index_batch()` instead — it performs the same
    /// work 10–20× faster by batching LMDB transactions.
    ///
    /// This method acquires a **write** lock for its entire duration.
    ///
    /// Parameters
    /// ----------
    /// doc_id : int
    ///     Non-negative unique document identifier.  If `doc_id` is larger than
    ///     the current `total_docs` counter, the counter is advanced to
    ///     `doc_id + 1`.
    /// record_dict : dict[str, str]
    ///     Field names → raw string values.  Unknown field names are skipped.
    ///     Empty values produce zero tokens and are effectively no-ops for that
    ///     field.
    ///
    /// Notes
    /// -----
    /// - All values are tokenised and Unicode-normalised (NFD, lowercase).
    /// - Calling this with the same `doc_id` twice does **not** remove the
    ///   previous entry; it merges tokens into the existing postings.
    #[pyo3(text_signature = "(self, doc_id, record_dict)")]
    fn index_dict(&mut self, doc_id: usize, record_dict: HashMap<String, String>) {
        let mut global = GLOBAL_ENGINE.write().unwrap();
        let (engine, _) = global.as_mut().expect("Engine not initialized");

        if doc_id % 10000 == 0 {
            info!(
                "[RUST] Indexing doc_id: {} (Total docs: {})",
                doc_id, engine.metadata.total_docs
            );
        }

        let mut field_count = 0;
        let mut token_count = 0;
        let mut doc_terms: HashMap<(RecordField, String), bool> = HashMap::new();

        for (key, text) in record_dict {
            let field = match self.map_field(&key) {
                Some(f) => f,
                None => continue,
            };

            let tokens = tokenize(&text);
            let this_field_tokens = tokens.len();
            token_count += this_field_tokens;
            field_count += 1;

            for token in tokens {
                engine.index.add_term(doc_id, field, token.clone());
                doc_terms.insert((field, token), true);
            }

            engine
                .metadata
                .lengths
                .entry(doc_id)
                .or_default()
                .insert(field, this_field_tokens);
            *engine
                .metadata
                .total_field_lengths
                .entry(field)
                .or_insert(0) += this_field_tokens;
        }

        for (key, _) in doc_terms {
            *engine.metadata.term_df.entry(key).or_insert(0) += 1;
        }

        if doc_id >= engine.metadata.total_docs {
            engine.metadata.total_docs = doc_id + 1;
        }

        if doc_id == 0 {
            info!(
                "[INDEX] First doc indexed: {} fields, {} tokens",
                field_count, token_count
            );
        }
    }

    /// Flush the LMDB write buffer to persistent storage.
    ///
    /// `LmdbStorage` accumulates writes in a memory buffer and flushes them in
    /// sorted-key batches to minimise LMDB transaction overhead.  This method
    /// forces an immediate commit of any remaining buffered entries.
    ///
    /// Call `flush()` after each batch of `index_dict` / `index_batch` calls to
    /// ensure data survives a process restart.  It is also called automatically
    /// when `LmdbStorage` is dropped.
    ///
    /// This method acquires a **write** lock for its entire duration.
    ///
    /// Returns
    /// -------
    /// None
    ///
    /// Raises
    /// ------
    /// RuntimeError
    ///     If the underlying LMDB `write_txn` or `commit` fails (e.g. disk
    ///     full, LMDB map size exceeded).
    #[pyo3(text_signature = "(self)")]
    fn flush(&mut self) -> PyResult<()> {
        info!("[RUST] Flushing buffered writes to disk...");
        let timer = Timer::new("flush");

        let mut global = GLOBAL_ENGINE.write().unwrap();
        let (engine, _) = global.as_mut().expect("Engine not initialized");

        engine.index.storage.flush().map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("Flush failed: {}", e))
        })?;

        drop(timer);
        info!("[RUST] Flush complete");
        Ok(())
    }

    /// Perform a field-aware BM25F search query.
    ///
    /// This method acquires only a **read** lock on the global engine, so
    /// multiple Python threads can run searches truly in parallel.  Custom
    /// weights/b-values are applied via a lightweight, per-call
    /// `BM25FScorer` allocated on the stack — the global scorer is never
    /// touched.
    ///
    /// Parameters
    /// ----------
    /// query_dict : dict[str, str]
    ///     Field-value pairs for the search query.
    /// top_k : int
    ///     Maximum number of results to return.
    /// blocking_k : int
    ///     Maximum candidate documents to score (performance/recall trade-off).
    ///
    /// Returns
    /// -------
    /// list[tuple[int, float]]
    ///     (doc_id, score) pairs sorted by score descending.
    ///
    /// Notes
    /// -----
    /// Concurrency: safe to call from multiple threads simultaneously.
    /// blocking_k guidance:
    ///   1 000  → fastest, may miss some results
    ///   10 000 → balanced
    ///   100 000 → highest recall, slower
    #[pyo3(text_signature = "(self, query_dict, top_k, blocking_k)")]
    fn search_complex(
        &self,
        query_dict: HashMap<String, String>,
        top_k: usize,
        blocking_k: usize,
    ) -> Vec<(usize, f32)> {
        info!("[RUST] search_complex called");
        info!("[RUST] Query dict size: {}", query_dict.len());

        let total_timer = Timer::new("search_complex::total");

        // ── Parse query fields ──────────────────────────────────────────────
        let parse_timer = Timer::new("search_complex::parse_query");
        let mut query_fields = Vec::new();

        for (key, text) in query_dict {
            if text.trim().is_empty() {
                continue;
            }
            info!("[RUST] Processing field: {} = '{}'", key, text);
            let field = match self.map_field(&key) {
                Some(f) => f,
                None => continue,
            };
            query_fields.push((field, text));
        }
        drop(parse_timer);

        if query_fields.is_empty() {
            info!("[RUST] No valid query fields, returning empty results");
            return Vec::new();
        }

        let query = StructuredQuery {
            fields: query_fields,
            top_k,
            blocking_k,
        };

        // ── Acquire READ lock — allows concurrent searches ──────────────────
        let exec_timer = Timer::new("search_complex::execute");
        let global = GLOBAL_ENGINE.read().unwrap(); // ← read lock, not write
        let (engine, _) = global.as_ref().expect("Engine not initialized");

        // Build a per-call scorer if custom parameters are configured.
        // This is a cheap stack allocation; no global state is touched.
        //
        // When neither custom_weights nor custom_b_values are set we borrow
        // the engine's own scorer directly, avoiding any allocation.
        let local_scorer: BM25FScorer<RecordField>;
        let scorer_ref: &BM25FScorer<RecordField> =
            if self.custom_weights.is_some() || self.custom_b_values.is_some() {
                local_scorer = BM25FScorer {
                    k1: engine.scorer.k1,
                    field_weights: self
                        .custom_weights
                        .clone()
                        .unwrap_or_else(|| engine.scorer.field_weights.clone()),
                    field_b: self
                        .custom_b_values
                        .clone()
                        .unwrap_or_else(|| engine.scorer.field_b.clone()),
                };
                &local_scorer
            } else {
                &engine.scorer
            };

        let results: Vec<(usize, f32)> = engine
            .execute_with_scorer(query, blocking_k, scorer_ref)
            .into_iter()
            .map(|hit| (hit.doc_id, hit.score))
            .collect();

        drop(exec_timer);

        info!("[RUST] Search returned {} results", results.len());
        for (i, (doc_id, score)) in results.iter().take(10).enumerate() {
            debug!(
                "[RUST] Result #{}: doc_id={}, score={}",
                i + 1,
                doc_id,
                score
            );
        }

        drop(total_timer);
        results
    }

    /// Return the total number of indexed documents.
    ///
    /// Reads `engine.metadata.total_docs`, which is incremented during
    /// `index_dict` and `index_batch`.  The value reflects the highest
    /// `doc_id + 1` seen, not the count of unique doc IDs.
    ///
    /// This method acquires a **read** lock and is safe to call concurrently.
    ///
    /// Returns
    /// -------
    /// int
    ///     Total indexed document count.
    #[pyo3(text_signature = "(self)")]
    fn get_total_docs(&self) -> usize {
        let global = GLOBAL_ENGINE.read().unwrap();
        let (engine, _) = global.as_ref().expect("Engine not initialized");
        engine.metadata.total_docs
    }

    /// Return a human-readable statistics string for the current index.
    ///
    /// Currently reports the total document count.  The format may be extended
    /// in future versions to include term counts, storage size, etc.
    ///
    /// This method acquires a **read** lock and is safe to call concurrently.
    ///
    /// Returns
    /// -------
    /// str
    ///     Formatted statistics, e.g. `"Total docs indexed: 1234567"`.
    #[pyo3(text_signature = "(self)")]
    fn get_stats(&self) -> String {
        let global = GLOBAL_ENGINE.read().unwrap();
        let (engine, _) = global.as_ref().expect("Engine not initialized");
        format!("Total docs indexed: {}", engine.metadata.total_docs)
    }

    /// Serialise index metadata to a binary file.
    ///
    /// Writes `engine.metadata` (document lengths, corpus-wide field lengths,
    /// total doc count, and term document frequencies) to `path` using
    /// `bincode` serialisation.  The file can be loaded back with
    /// `load_metadata` to avoid recomputing metadata after a process restart.
    ///
    /// This method acquires a **read** lock; metadata is not modified.
    ///
    /// Parameters
    /// ----------
    /// path : str
    ///     Destination file path.  The Python wrapper defaults this to
    ///     `{db_path}/metadata.bin` when `None` is passed.
    ///
    /// Raises
    /// ------
    /// IOError
    ///     If the file cannot be created or the serialisation fails.
    ///
    /// Notes
    /// -----
    /// The metadata file is engine-version-specific; do not share it across
    /// different versions of LFAS that change the `FieldMetadata` schema.
    #[pyo3(text_signature = "(self, path)")]
    fn save_metadata(&self, path: &str) -> PyResult<()> {
        let global = GLOBAL_ENGINE.read().unwrap();
        let (engine, _) = global.as_ref().expect("Engine not initialized");

        let file = File::create(path)?;
        let writer = BufWriter::new(file);
        serialize_into(writer, &engine.metadata)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))
    }

    /// Deserialise and replace index metadata from a binary file.
    ///
    /// Loads a `FieldMetadata` snapshot previously written by `save_metadata`
    /// and replaces `engine.metadata` in place.  Must be called before
    /// `search_complex` when working with a pre-built LMDB index that was
    /// loaded into a freshly constructed engine (i.e. after a process restart).
    ///
    /// This method acquires a **write** lock because it replaces `engine.metadata`.
    ///
    /// Parameters
    /// ----------
    /// path : str
    ///     Source file path.  The Python wrapper defaults this to
    ///     `{db_path}/metadata.bin` and raises `FileNotFoundError` before
    ///     calling into Rust if the file is absent.
    ///
    /// Raises
    /// ------
    /// IOError
    ///     If the file cannot be opened or deserialisation fails (e.g. corrupt
    ///     file or schema mismatch from a different LFAS version).
    #[pyo3(text_signature = "(self, path)")]
    fn load_metadata(&mut self, path: &str) -> PyResult<()> {
        let mut global = GLOBAL_ENGINE.write().unwrap();
        let (engine, _) = global.as_mut().expect("Engine not initialized");

        let file = File::open(path)?;
        let reader = BufReader::new(file);
        engine.metadata = deserialize_from(reader)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;
        Ok(())
    }
}

// Internal helpers — kept outside #[gen_stub_pymethods] to avoid stub-gen
// needing `RecordField: PyStubType`.
impl PySearchEngine {
    /// Map a Python-side field name string to the corresponding [`RecordField`]
    /// enum variant.
    ///
    /// The comparison is case-insensitive (`to_lowercase()` is applied before
    /// matching).  Returns `None` for any unrecognised name; callers log a
    /// warning and skip the field rather than returning an error, so partial
    /// queries with unknown fields degrade gracefully.
    ///
    /// Recognised names: `estado`, `municipio`, `bairro`, `cep`,
    /// `tipo_logradouro`, `rua`, `numero`, `complemento`, `nome`.
    fn map_field(&self, field_name: &str) -> Option<RecordField> {
        match field_name.to_lowercase().as_str() {
            "estado" => Some(RecordField::Estado),
            "municipio" => Some(RecordField::Municipio),
            "bairro" => Some(RecordField::Bairro),
            "cep" => Some(RecordField::Cep),
            "tipo_logradouro" => Some(RecordField::TipoLogradouro),
            "rua" => Some(RecordField::Rua),
            "numero" => Some(RecordField::Numero),
            "complemento" => Some(RecordField::Complemento),
            "nome" => Some(RecordField::Nome),
            _ => None,
        }
    }
}

/// LFAS `_core` Python extension module.
///
/// This is the compiled Rust extension imported as `lfas._core`.  It exposes
/// [`PySearchEngine`] to Python.  Users should import from the `lfas` package
/// directly (via `__init__.py`) rather than from `_core`, which is a private
/// implementation detail.
///
/// The module name `_core` is declared in `pyproject.toml` and must match the
/// `#[pymodule]` function name here.
#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    info!("[RUST] PySearchEngine class registered");
    m.add_class::<PySearchEngine>()?;
    Ok(())
}

/// Entry-point for the `stub_gen` binary.
///
/// Reads `pyproject.toml` to discover the module name and output path, then
/// walks all `#[gen_stub_pyclass]` / `#[gen_stub_pymethods]` annotations to
/// produce a `lfas.pyi` stub file.  Run with:
///
/// ```shell
/// cargo run --bin stub_gen
/// ```
pub fn stub_info() -> pyo3_stub_gen::StubInfo {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    pyo3_stub_gen::StubInfo::from_pyproject_toml(format!("{}/pyproject.toml", manifest_dir))
        .expect("Failed to read pyproject.toml for stub generation")
}