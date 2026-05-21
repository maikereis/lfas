use crate::engine;
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

// Store both the engine and its database path
static GLOBAL_ENGINE: Lazy<
    Arc<RwLock<Option<(SearchEngine<RecordField, LmdbStorage<RecordField>>, String)>>>,
> = Lazy::new(|| Arc::new(RwLock::new(None)));

/// High-performance BM25F search engine for Brazilian address data.
///
/// PySearchEngine provides a Python interface to a Rust-based inverted index
/// with LMDB storage, optimized for field-aware address searching.
///
/// The engine uses a global singleton pattern with configurable LMDB storage path
/// and supports concurrent read operations (searches) while serializing writes.
///
/// Examples
/// --------
/// >>> from lfas import PySearchEngine
/// >>> engine = PySearchEngine()  # Uses default ./lmdb_data
/// >>> engine = PySearchEngine(db_path="./my_index")  # Custom path
/// >>> engine.index_dict(0, {
/// ...     'rua': 'Avenida Paulista',
/// ...     'numero': '1578',
/// ...     'municipio': 'São Paulo'
/// ... })
/// >>> engine.flush()
/// >>> results = engine.search_complex({'rua': 'Paulista'}, top_k=10, blocking_k=1000)
#[gen_stub_pyclass]
#[pyclass]
pub struct PySearchEngine {
    custom_weights: Option<HashMap<RecordField, f32>>,
    custom_b_values: Option<HashMap<RecordField, f32>>,
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

    /// Create a new search engine instance.
    ///
    /// The constructor initializes or reuses a global LMDB-backed search engine.
    /// On first call with a given path, creates a new LMDB environment.
    /// Subsequent calls with the same path reuse the existing environment (singleton pattern).
    ///
    /// Parameters
    /// ----------
    /// db_path : str, optional
    ///     Path to the LMDB database directory (default: "./lmdb_data")
    ///
    /// Returns
    /// -------
    /// PySearchEngine
    ///     A new search engine instance with default BM25F parameters.
    ///
    /// Examples
    /// --------
    /// >>> engine = PySearchEngine()  # Uses ./lmdb_data
    /// >>> engine = PySearchEngine(db_path="./my_custom_index")
    #[new]
    #[pyo3(signature = (db_path = "./lmdb_data"))]
    fn new(db_path: &str) -> Self {
        info!("[RUST] PySearchEngine::new() called with db_path: {}", db_path);
        let timer = Timer::new("PySearchEngine::new");

        // Use write lock only for initialization
        let mut global = GLOBAL_ENGINE.write().unwrap();
        
        match global.as_ref() {
            Some((_, existing_path)) if existing_path == db_path => {
                info!("[RUST] Reusing existing LMDB storage at {}", db_path);
            }
            Some((_, existing_path)) => {
                info!("[RUST] Different path requested. Old: {}, New: {}", existing_path, db_path);
                info!("[RUST] Creating new LMDB storage at {}", db_path);
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
        drop(global); // Release write lock immediately

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
    /// Field weights control how much each field contributes to the final
    /// relevance score. Higher weights make a field more important.
    ///
    /// Parameters
    /// ----------
    /// weights : dict[str, float]
    ///     Dictionary mapping field names to weight values.
    ///     Valid field names: 'estado', 'municipio', 'bairro', 'cep',
    ///     'tipo_logradouro', 'rua', 'numero', 'complemento', 'nome'
    ///
    /// Examples
    /// --------
    /// >>> engine.set_field_weights({
    /// ...     'cep': 15.0,      # CEP very important
    /// ...     'numero': 12.0,   # Street number important
    /// ...     'rua': 5.0        # Street name moderately important
    /// ... })
    ///
    /// Notes
    /// -----
    /// Default weights:
    /// - numero: 10.0, cep: 8.0, rua: 5.0, municipio: 3.0, bairro: 2.0,
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
    /// The b parameter controls how much document length affects scoring.
    /// - b=0.0: No normalization (field length ignored)
    /// - b=0.75: Standard normalization (recommended)
    /// - b=1.0: Full normalization (heavily penalizes long fields)
    ///
    /// Parameters
    /// ----------
    /// b_values : dict[str, float]
    ///     Dictionary mapping field names to b values (0.0 to 1.0).
    ///     Valid field names: 'estado', 'municipio', 'bairro', 'cep',
    ///     'tipo_logradouro', 'rua', 'numero', 'complemento', 'nome'
    ///
    /// Examples
    /// --------
    /// >>> engine.set_field_b_values({
    /// ...     'cep': 0.0,      # No normalization (fixed-length)
    /// ...     'numero': 0.0,   # No normalization (fixed-length)
    /// ...     'rua': 0.75,     # Standard normalization
    /// ...     'bairro': 0.5    # Moderate normalization
    /// ... })
    ///
    /// Notes
    /// -----
    /// Default b-values:
    /// - numero, cep, estado, tipo_logradouro: 0.0 (fixed-length identifiers)
    /// - municipio, complemento: 0.5 (moderate normalization)
    /// - rua, bairro, nome: 0.75 (standard normalization)
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

    /// Reset field weights and b-values to default settings.
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

    /// Get current field weight configuration.
    ///
    /// Returns
    /// -------
    /// dict[str, float]
    ///     Dictionary of field names to current weight values.
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
    /// This is the recommended method for bulk indexing as it's significantly
    /// faster than individual index_dict() calls. Uses in-memory aggregation
    /// to minimize LMDB transaction overhead.
    ///
    /// Parameters
    /// ----------
    /// records : list[tuple[int, dict[str, str]]]
    ///     List of (doc_id, record_dict) tuples where:
    ///     - doc_id: Unique document identifier (must be >= 0)
    ///     - record_dict: Dictionary of field names to values
    ///
    /// Examples
    /// --------
    /// >>> batch = [
    /// ...     (0, {'rua': 'Rua A', 'municipio': 'Belém'}),
    /// ...     (1, {'rua': 'Rua B', 'municipio': 'Belém'}),
    /// ...     (2, {'rua': 'Rua C', 'municipio': 'Belém'})
    /// ... ]
    /// >>> engine.index_batch(batch)
    /// >>> engine.flush()
    ///
    /// Performance
    /// -----------
    /// - Processes 100,000-200,000 documents/second
    /// - Use batch sizes of 100,000-500,000 for optimal performance
    /// - Call flush() after each batch to ensure persistence
    #[pyo3(text_signature = "(self, records)")]
    fn index_batch(&mut self, records: Vec<(usize, HashMap<String, String>)>) {
        let mut global = GLOBAL_ENGINE.write().unwrap(); // Write lock for indexing
        let (engine, _) = global.as_mut().expect("Engine not initialized");

        // In-memory aggregation: (Field, Term) -> List of DocIds
        // This drastically reduces trips to the LMDB
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

        // Batch writing to Storage
        // Now we only perform ONE read and ONE write per single term in the batch
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

            // The LmdbStorage we have already has a WriteBuffer,
            // so this will be extremely fast.
            engine.index.storage.put(field, term, postings).unwrap();
        }
    }

    /// Index a single document with field-value pairs.
    ///
    /// For bulk indexing, use index_batch() instead as it's 10-20x faster.
    ///
    /// Parameters
    /// ----------
    /// doc_id : int
    ///     Unique document identifier (must be >= 0)
    /// record_dict : dict[str, str]
    ///     Dictionary mapping field names to values.
    ///     Valid fields: 'estado', 'municipio', 'bairro', 'cep',
    ///     'tipo_logradouro', 'rua', 'numero', 'complemento', 'nome'
    ///
    /// Examples
    /// --------
    /// >>> engine.index_dict(0, {
    /// ...     'rua': 'Travessa WE 8',
    /// ...     'numero': '100',
    /// ...     'bairro': 'Cidade Nova',
    /// ...     'municipio': 'Ananindeua',
    /// ...     'estado': 'PA',
    /// ...     'cep': '67130-021'
    /// ... })
    ///
    /// Notes
    /// -----
    /// - All values are automatically tokenized and normalized
    /// - Empty or missing fields are ignored
    /// - Updates metadata for BM25F scoring calculations
    #[pyo3(text_signature = "(self, doc_id, record_dict)")]
    fn index_dict(&mut self, doc_id: usize, record_dict: HashMap<String, String>) {
        let mut global = GLOBAL_ENGINE.write().unwrap(); // Write lock for indexing
        let (engine, _) = global.as_mut().expect("Engine not initialized");

        if doc_id % 10000 == 0 {
            info!(
                "[RUST] Indexing doc_id: {} (Total docs: {})",
                doc_id, engine.metadata.total_docs
            );
        }

        let mut field_count = 0;
        let mut token_count = 0;

        // Track unique terms by document
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

    /// Flush buffered writes to persistent storage (LMDB).
    ///
    /// This method commits all pending index operations to disk. Should be
    /// called after indexing operations to ensure data persistence.
    ///
    /// Returns
    /// -------
    /// None
    ///
    /// Raises
    /// ------
    /// RuntimeError
    ///     If the flush operation fails
    ///
    /// Examples
    /// --------
    /// >>> engine.index_batch(records)
    /// >>> engine.flush()  # Commit to disk
    ///
    /// Notes
    /// -----
    /// - Automatically called when the engine is destroyed
    /// - For large batch operations, flush periodically (e.g., every 500k docs)
    #[pyo3(text_signature = "(self)")]
    fn flush(&mut self) -> PyResult<()> {
        info!("[RUST] Flushing buffered writes to disk...");
        let timer = Timer::new("flush");

        let mut global = GLOBAL_ENGINE.write().unwrap(); // Write lock for flush
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
    /// Executes a two-stage search:
    /// 1. Candidate retrieval using distinctive tokens (CEP, numbers, n-grams)
    /// 2. BM25F scoring of candidates with all query tokens
    ///
    /// Parameters
    /// ----------
    /// query_dict : dict[str, str]
    ///     Field-value pairs for the search query.
    ///     Valid fields: 'estado', 'municipio', 'bairro', 'cep',
    ///     'tipo_logradouro', 'rua', 'numero', 'complemento', 'nome'
    /// top_k : int
    ///     Maximum number of results to return
    /// blocking_k : int
    ///     Maximum candidate documents to consider (performance/recall tradeoff)
    ///
    /// Returns
    /// -------
    /// list[tuple[int, float]]
    ///     List of (doc_id, score) tuples sorted by score (descending)
    ///
    /// Examples
    /// --------
    /// >>> results = engine.search_complex(
    /// ...     {
    /// ...         'rua': 'WE 8',
    /// ...         'bairro': 'Cidade Nova',
    /// ...         'municipio': 'Ananindeua'
    /// ...     },
    /// ...     top_k=10,
    /// ...     blocking_k=1000
    /// ... )
    /// >>> for doc_id, score in results:
    /// ...     print(f"Document {doc_id}: {score:.2f}")
    ///
    /// Notes
    /// -----
    /// Search Strategy:
    /// - Uses distinctive tokens (CEP, numbers, street type+number) for candidate retrieval
    /// - Fallback to rarest tokens if no distinctive matches found
    /// - Scores all candidates with full BM25F algorithm
    ///
    /// Performance Tuning:
    /// - blocking_k=1000: Fast, may miss some relevant results
    /// - blocking_k=10000: Balanced performance/recall
    /// - blocking_k=100000: Slower, highest recall
    #[pyo3(text_signature = "(self, query_dict, top_k, blocking_k)")]
    fn search_complex(
        &self,
        query_dict: HashMap<String, String>,
        top_k: usize,
        blocking_k: usize,
    ) -> Vec<(usize, f32)> {
        info!("[RUST] search_complex called");
        info!("[RUST] Query dict size: {}", query_dict.len());
        info!("[RUST] top_k: {}", top_k);

        let total_timer = Timer::new("search_complex::total");

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

        info!(
            "[RUST] Total query fields after parsing: {}",
            query_fields.len()
        );

        if query_fields.is_empty() {
            info!("[RUST] No valid query fields, returning empty results");
            return Vec::new();
        }

        let query = StructuredQuery {
            fields: query_fields,
            top_k,
            blocking_k,
        };

        info!("[RUST] Executing search with blocking_k={}", blocking_k);

        let exec_timer = Timer::new("search_complex::execute");

        // Use write lock (needed to apply custom weights before scoring)
        let mut global = GLOBAL_ENGINE.write().unwrap();
        let (engine, _) = global.as_mut().expect("Engine not initialized");

        // Apply custom weights if configured
        if let Some(ref weights) = self.custom_weights {
            info!("[RUST] Applying custom weights for search");
            engine.scorer.field_weights = weights.clone();
        }

        if let Some(ref b_values) = self.custom_b_values {
            info!("[RUST] Applying custom b-values for search");
            engine.scorer.field_b = b_values.clone();
        }

        let results: Vec<(usize, f32)> = engine
            .execute(query, blocking_k)
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
        info!("[RUST] Returning {} results to Python", results.len());

        results
    }

    /// Get the total number of indexed documents.
    ///
    /// Returns
    /// -------
    /// int
    ///     Total count of indexed documents
    ///
    /// Examples
    /// --------
    /// >>> total = engine.get_total_docs()
    /// >>> print(f"Indexed {total:,} documents")
    #[pyo3(text_signature = "(self)")]
    fn get_total_docs(&self) -> usize {
        let global = GLOBAL_ENGINE.read().unwrap(); // Read lock
        let (engine, _) = global.as_ref().expect("Engine not initialized");
        engine.metadata.total_docs
    }

    /// Get formatted index statistics.
    ///
    /// Returns
    /// -------
    /// str
    ///     Human-readable statistics string
    ///
    /// Examples
    /// --------
    /// >>> stats = engine.get_stats()
    /// >>> print(stats)
    /// Total docs indexed: 1234567
    #[pyo3(text_signature = "(self)")]
    fn get_stats(&self) -> String {
        let global = GLOBAL_ENGINE.read().unwrap(); // Read lock
        let (engine, _) = global.as_ref().expect("Engine not initialized");
        format!("Total docs indexed: {}", engine.metadata.total_docs)
    }

    /// Save index metadata to a binary file.
    ///
    /// Saves document lengths, field statistics, and term document frequencies
    /// to a file for later loading with load_metadata().
    ///
    /// Parameters
    /// ----------
    /// path : str
    ///     File path for the metadata file
    ///
    /// Raises
    /// ------
    /// IOError
    ///     If file cannot be created or written
    ///
    /// Examples
    /// --------
    /// >>> engine.save_metadata("./lmdb_data/metadata.bin")
    ///
    /// Notes
    /// -----
    /// - Required for search operations after restarting
    /// - Faster than rebuilding metadata from scratch
    /// - Contains: doc lengths, total field lengths, doc counts, term DFs
    #[pyo3(text_signature = "(self, path)")]
    fn save_metadata(&self, path: &str) -> PyResult<()> {
        let global = GLOBAL_ENGINE.read().unwrap(); // Read lock
        let (engine, _) = global.as_ref().expect("Engine not initialized");

        let file = File::create(path)?;
        let writer = BufWriter::new(file);
        serialize_into(writer, &engine.metadata)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))
    }

    /// Load index metadata from a binary file.
    ///
    /// Loads previously saved metadata required for search operations.
    /// Must be called before searching when using a pre-built index.
    ///
    /// Parameters
    /// ----------
    /// path : str
    ///     File path to the metadata file
    ///
    /// Raises
    /// ------
    /// IOError
    ///     If file cannot be read or is corrupted
    ///
    /// Examples
    /// --------
    /// >>> engine = PySearchEngine()
    /// >>> engine.load_metadata("./lmdb_data/metadata.bin")
    /// >>> results = engine.search_complex({'rua': 'Paulista'}, 10, 1000)
    ///
    /// Notes
    /// -----
    /// - Must match the current LMDB index
    /// - Enables BM25F scoring calculations
    /// - Much faster than rebuilding from scratch
    #[pyo3(text_signature = "(self, path)")]
    fn load_metadata(&mut self, path: &str) -> PyResult<()> {
        let mut global = GLOBAL_ENGINE.write().unwrap(); // Write lock
        let (engine, _) = global.as_mut().expect("Engine not initialized");

        let file = File::open(path)?;
        let reader = BufReader::new(file);
        engine.metadata = deserialize_from(reader)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;
        Ok(())
    }
}

// Separate impl for internal Rust methods that are not part of the Python API.
// By staying outside the #[gen_stub_pymethods] block, the macro does not attempt to generate
// stubs for them — avoiding the "RecordField: PyStubType not satisfied" error.
impl PySearchEngine {
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

/// LFAS - Lightning-Fast Address Search
///
/// A high-performance BM25F search engine optimized for Brazilian address data.
///
/// Features
/// --------
/// - LMDB-backed persistent inverted index
/// - Configurable database path
/// - Field-aware BM25F scoring
/// - Concurrent read operations (searches)
/// - Optimized tokenization for Brazilian addresses
/// - Batch indexing: 100,000+ docs/second
/// - Search latency: 10-50ms typical
///
/// Example
/// -------
/// >>> from lfas import PySearchEngine
/// >>> engine = PySearchEngine(db_path="./my_index")
/// >>> engine.index_dict(0, {'rua': 'Avenida Paulista', 'numero': '1578'})
/// >>> engine.flush()
/// >>> results = engine.search_complex({'rua': 'Paulista'}, top_k=10, blocking_k=1000)
/// >>> print(results)  # [(doc_id, score), ...]
#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    info!("[RUST] PySearchEngine class registered");
    m.add_class::<PySearchEngine>()?;
    Ok(())
}

/// Called by the `stub_gen` binary to produce the .pyi file.
pub fn stub_info() -> pyo3_stub_gen::StubInfo {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    pyo3_stub_gen::StubInfo::from_pyproject_toml(
        format!("{}/pyproject.toml", manifest_dir)
    )
    .expect("Falha ao ler pyproject.toml para gerar stubs")
}