use crate::engine;
use crate::storage::PostingsStorage;
use crate::timing::Timer;
use crate::tokenizer::tokenize;
use crate::{RecordField, StructuredQuery, engine::SearchEngine, storage::LmdbStorage};
use bincode::{deserialize_from, serialize_into};
use log::{debug, info};
use pyo3::prelude::*;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter};

#[pyclass]
pub struct PySearchEngine {
    inner: SearchEngine<RecordField, LmdbStorage<RecordField>>,
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

        let storage = LmdbStorage::<RecordField>::open(std::path::Path::new("./lmdb_data"))
            .expect("Failed to open LMDB storage");

        drop(timer);
        info!("[RUST] PySearchEngine created successfully");

        PySearchEngine {
            inner: engine::SearchEngine::with_storage(storage),
        }
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
        use std::collections::HashMap;
        
        // In-memory aggregation: (Field, Term) -> List of DocIds
        // This drastically reduces trips to the LMDB
        let mut batch_accumulator: HashMap<(RecordField, String), Vec<usize>> = HashMap::new();

        for (doc_id, record_dict) in records {
            for (field_name, value) in record_dict {
                if let Some(field) = self.map_field(&field_name) {
                    for term in tokenize(&value) {
                        // Acumula o ID do documento para este termo específico
                        batch_accumulator
                            .entry((field, term))
                            .or_default()
                            .push(doc_id);
                    }
                }
            }
            self.inner.metadata.total_docs += 1;
        }

        // Batch writing to Storage
        // Now we only perform ONE read and ONE write per single term in the batch
        for ((field, term), doc_ids) in batch_accumulator {
            let mut postings = self.inner.index.storage.get(field, &term)
                .unwrap_or_default()
                .unwrap_or_else(crate::postings::Postings::new);
                
            for id in doc_ids {
                postings.add_doc(id);
            }
            
            // The LmdbStorage you have already has a WriteBuffer,
            // so this will be extremely fast.
            self.inner.index.storage.put(field, term, postings).unwrap();
        }
    }

    fn index_dict(&mut self, doc_id: usize, record_dict: HashMap<String, String>) {
        if doc_id % 10000 == 0 {
            info!(
                "[RUST] Indexing doc_id: {} (Total docs: {})",
                doc_id, self.inner.metadata.total_docs
            );
        }

        let mut field_count = 0;
        let mut token_count = 0;

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
                self.inner.index.add_term(doc_id, field, token.clone());

                let key = (field, token);
                *self.inner.metadata.term_df.entry(key).or_insert(0) += 1;
            }

            self.inner
                .metadata
                .lengths
                .entry(doc_id)
                .or_default()
                .insert(field, this_field_tokens);
            *self
                .inner
                .metadata
                .total_field_lengths
                .entry(field)
                .or_insert(0) += this_field_tokens;
        }

        if doc_id >= self.inner.metadata.total_docs {
            self.inner.metadata.total_docs = doc_id + 1;
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
        self.inner.index.storage.flush().map_err(|e| {
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
        let results: Vec<(usize, f32)> = self
            .inner
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
        self.inner.metadata.total_docs
    }

    fn get_stats(&self) -> String {
        format!("Total docs indexed: {}", self.inner.metadata.total_docs)
    }

    fn save_metadata(&self, path: &str) -> PyResult<()> {
        let file = File::create(path)?;
        let writer = BufWriter::new(file);
        serialize_into(writer, &self.inner.metadata)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))
    }
    
    fn load_metadata(&mut self, path: &str) -> PyResult<()> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        self.inner.metadata = deserialize_from(reader)
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
