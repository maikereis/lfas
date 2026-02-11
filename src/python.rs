use crate::engine;
use crate::storage::PostingsStorage;
use crate::timing::Timer;
use crate::tokenizer::tokenize;
use crate::{RecordField, StructuredQuery, engine::SearchEngine, storage::LmdbStorage};
use bincode::{deserialize_from, serialize_into};
use log::{debug, info};
use once_cell::sync::Lazy;
use pyo3::prelude::*;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::sync::{Arc, RwLock};

// Use RwLock for concurrent reads (searches)
static GLOBAL_ENGINE: Lazy<
    Arc<RwLock<Option<SearchEngine<RecordField, LmdbStorage<RecordField>>>>>,
> = Lazy::new(|| Arc::new(RwLock::new(None)));

#[pyclass]
pub struct PySearchEngine {
    custom_weights: Option<HashMap<RecordField, f32>>,
    custom_b_values: Option<HashMap<RecordField, f32>>,
}

#[pymethods]
impl PySearchEngine {
    #[staticmethod]
    fn init_logging() {
        let _ = pyo3_log::try_init();
    }

    #[new]
    fn new() -> Self {
        info!("[RUST] PySearchEngine::new() called");
        let timer = Timer::new("PySearchEngine::new");

        // Use write lock only for initialization
        let mut global = GLOBAL_ENGINE.write().unwrap();
        if global.is_none() {
            info!("[RUST] Creating new LMDB storage (first time)");
            let storage = LmdbStorage::<RecordField>::open(std::path::Path::new("./lmdb_data"))
                .expect("Failed to open LMDB storage");
            *global = Some(engine::SearchEngine::with_storage(storage));
        } else {
            info!("[RUST] Reusing existing LMDB storage");
        }
        drop(global); // Release write lock immediately

        drop(timer);
        info!("[RUST] PySearchEngine created successfully");

        PySearchEngine {
            custom_weights: None,
            custom_b_values: None,
        }
    }

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

    /// Reset to default weights
    fn reset_weights(&mut self) {
        self.custom_weights = None;
        self.custom_b_values = None;
        info!("[RUST] Reset to default weights");
    }

    /// Get current weights configuration
    fn get_weights(&self) -> HashMap<String, f32> {
        let global = GLOBAL_ENGINE.read().unwrap();
        let engine = global.as_ref().expect("Engine not initialized");

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

    fn index_batch(&mut self, records: Vec<(usize, HashMap<String, String>)>) {
        let mut global = GLOBAL_ENGINE.write().unwrap(); // Write lock for indexing
        let engine = global.as_mut().expect("Engine not initialized");

        // In-memory aggregation: (Field, Term) -> List of DocIds
        // This drastically reduces trips to the LMDB
        let mut batch_accumulator: HashMap<(RecordField, String), Vec<usize>> = HashMap::new();

        for (doc_id, record_dict) in records {
            for (field_name, value) in record_dict {
                if let Some(field) = self.map_field(&field_name) {
                    for term in tokenize(&value) {
                        batch_accumulator
                            .entry((field, term))
                            .or_default()
                            .push(doc_id);
                    }
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

    fn index_dict(&mut self, doc_id: usize, record_dict: HashMap<String, String>) {
        let mut global = GLOBAL_ENGINE.write().unwrap(); // Write lock for indexing
        let engine = global.as_mut().expect("Engine not initialized");

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

    fn flush(&mut self) -> PyResult<()> {
        info!("[RUST] Flushing buffered writes to disk...");
        let timer = Timer::new("flush");

        let mut global = GLOBAL_ENGINE.write().unwrap(); // Write lock for flush
        let engine = global.as_mut().expect("Engine not initialized");

        engine.index.storage.flush().map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("Flush failed: {}", e))
        })?;

        drop(timer);
        info!("[RUST] Flush complete");
        Ok(())
    }

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

        // Use READ lock for searching (allows concurrent searches)
        let mut global = GLOBAL_ENGINE.write().unwrap();
        let engine = global.as_mut().expect("Engine not initialized");

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

    fn get_total_docs(&self) -> usize {
        let global = GLOBAL_ENGINE.read().unwrap(); // Read lock
        let engine = global.as_ref().expect("Engine not initialized");
        engine.metadata.total_docs
    }

    fn get_stats(&self) -> String {
        let global = GLOBAL_ENGINE.read().unwrap(); // Read lock
        let engine = global.as_ref().expect("Engine not initialized");
        format!("Total docs indexed: {}", engine.metadata.total_docs)
    }

    fn save_metadata(&self, path: &str) -> PyResult<()> {
        let global = GLOBAL_ENGINE.read().unwrap(); // Read lock
        let engine = global.as_ref().expect("Engine not initialized");

        let file = File::create(path)?;
        let writer = BufWriter::new(file);
        serialize_into(writer, &engine.metadata)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))
    }

    fn load_metadata(&mut self, path: &str) -> PyResult<()> {
        let mut global = GLOBAL_ENGINE.write().unwrap(); // Write lock
        let engine = global.as_mut().expect("Engine not initialized");

        let file = File::open(path)?;
        let reader = BufReader::new(file);
        engine.metadata = deserialize_from(reader)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;
        Ok(())
    }
}

#[pymodule]
fn lfas(m: &Bound<'_, PyModule>) -> PyResult<()> {
    info!("[RUST] PySearchEngine class registered");
    m.add_class::<PySearchEngine>()?;
    Ok(())
}
