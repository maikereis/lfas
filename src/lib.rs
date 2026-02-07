pub mod index;
pub mod metadata;
pub mod postings;
pub mod tokenizer;
pub mod scorer;
pub mod engine;
pub mod timing;

pub type DocId = usize;

#[derive(Hash, Eq, PartialEq, Clone, Copy, Ord, PartialOrd, Debug)]
pub enum RecordField {
    Estado,
    Municipio,
    Bairro,
    Cep,
    TipoLogradouro,
    Rua,
    Numero,
    Complemento,
    Nome,
}

#[allow(dead_code)]
#[derive(Hash, Eq, PartialEq, Clone, Ord, PartialOrd, Debug, serde::Deserialize)]
pub struct Record {
    pub id: String,
    pub estado: String,
    pub municipio: String,
    pub bairro: String,
    pub cep: String,
    pub tipo_logradouro: String,
    pub rua: String,
    pub numero: String,
    pub complemento: String,
    pub nome: String,
}

impl Record {
    pub fn fields(&self) -> Vec<(RecordField, &str)> {
        vec![
            (RecordField::Estado, &self.estado),
            (RecordField::Municipio, &self.municipio),
            (RecordField::Bairro, &self.bairro),
            (RecordField::Cep, &self.cep),
            (RecordField::TipoLogradouro, &self.tipo_logradouro),
            (RecordField::Rua, &self.rua),
            (RecordField::Numero, &self.numero),
            (RecordField::Complemento, &self.complemento),
            (RecordField::Nome, &self.nome),
        ]
    }
}

pub struct StructuredQuery {
    pub fields: Vec<(RecordField, String)>,
    pub top_k: usize,
}

#[derive(Debug)]
pub struct SearchHit {
    pub doc_id: usize,
    pub score: f32,
}

pub trait AddressSearcher {
    fn search(&self, query: StructuredQuery) -> Vec<SearchHit>;
}


use pyo3::prelude::*;
use crate::engine::SearchEngine;
use crate::index::InvertedIndex;
use crate::metadata::FieldMetadata;
use crate::scorer::BM25FScorer;
use crate::tokenizer::tokenize;
use crate::timing::Timer;
use std::collections::HashMap;
use log::{info, debug};

#[pyclass]
pub struct PySearchEngine {
    inner: SearchEngine,
}

#[pymethods]
impl PySearchEngine {
    #[staticmethod]
    fn init_logging() {
        // Use try_init() and discard the result to avoid panicking 
        // if Streamlit re-runs this script.
        let _ = pyo3_log::try_init();
    }

    #[new]
    fn new() -> Self {
        info!("[RUST] PySearchEngine::new() called");
        let timer = Timer::new("PySearchEngine::new");
        
        let mut field_weights = HashMap::new();
        field_weights.insert(RecordField::Rua, 2.0);
        field_weights.insert(RecordField::Municipio, 1.0);
        field_weights.insert(RecordField::Bairro, 1.0);
        field_weights.insert(RecordField::Cep, 0.5);

        let engine = SearchEngine {
            index: InvertedIndex::new(),
            metadata: FieldMetadata::new(),
            scorer: BM25FScorer {
                k1: 1.2,
                field_weights,
                field_b: HashMap::new(),
            },
        };
        
        drop(timer);
        info!("[RUST] PySearchEngine created successfully");
        
        PySearchEngine { 
            inner: engine,
        }
    }

    fn index_dict(&mut self, doc_id: usize, record_dict: HashMap<String, String>) {
        
        // Log every 10000 documents with timing
        if doc_id % 10000 == 0 {
            info!("[RUST] Indexing doc_id: {} (Total docs: {})", 
                  doc_id, self.inner.metadata.total_docs);
        }
        
        let mut field_count = 0;
        let mut token_count = 0;
        
        for (key, text) in record_dict {
            let field = match key.to_lowercase().as_str() {
                "id" => RecordField::Nome,
                "estado" => RecordField::Estado,
                "municipio" => RecordField::Municipio,
                "bairro" => RecordField::Bairro,
                "cep" => RecordField::Cep,
                "tipo_logradouro" => RecordField::TipoLogradouro,
                "rua" => RecordField::Rua,
                "numero" => RecordField::Numero,
                "complemento" => RecordField::Complemento,
                "nome" => RecordField::Nome,
                _ => continue,
            };

            // Use ALL tokens (including weak ones) for indexing
            let tokens = tokenize(&text);
            let this_field_tokens = tokens.len();
            token_count += this_field_tokens;
            field_count += 1;

            for token in tokens {
                self.inner.index.add_term(doc_id, field, token);
            }

            self.inner.metadata.lengths.entry(doc_id).or_default().insert(field, this_field_tokens);
            *self.inner.metadata.total_field_lengths.entry(field).or_insert(0) += this_field_tokens;
        }

        if doc_id >= self.inner.metadata.total_docs {
            self.inner.metadata.total_docs = doc_id + 1;
        }
        
        // Log details for first document
        if doc_id == 0 {
            info!("[INDEX] First doc indexed: {} fields, {} tokens", field_count, token_count);
        }
    }

    fn search_complex(&self, query_dict: HashMap<String, String>, top_k: usize) -> Vec<(usize, f32)> {
        info!("[RUST] search_complex called");
        info!("[RUST] Query dict size: {}", query_dict.len());
        info!("[RUST] top_k: {}", top_k);
        
        let total_timer = Timer::new("search_complex::total");
        
        // Parse query
        let parse_timer = Timer::new("search_complex::parse_query");
        let mut query_fields = Vec::new();

        for (key, text) in query_dict {
            if text.trim().is_empty() { continue; }
            
            info!("[RUST] Processing field: {} = '{}'", key, text);
            
            let field = match key.to_lowercase().as_str() {
                "estado" => RecordField::Estado,
                "municipio" => RecordField::Municipio,
                "bairro" => RecordField::Bairro,
                "cep" => RecordField::Cep,
                "tipo_logradouro" => RecordField::TipoLogradouro,
                "rua" => RecordField::Rua,
                "numero" => RecordField::Numero,
                "complemento" => RecordField::Complemento,
                "nome" => RecordField::Nome,
                _ => continue,
            };
            query_fields.push((field, text));
        }
        drop(parse_timer);
        
        info!("[RUST] Total query fields after parsing: {}", query_fields.len());

        if query_fields.is_empty() {
            info!("[RUST] No valid query fields, returning empty results");
            return Vec::new();
        }

        let query = StructuredQuery {
            fields: query_fields,
            top_k,
        };
        
        // Execute search with high blocking_k for better recall
        let blocking_k = 100_000;
        info!("[RUST] Executing search with blocking_k={}", blocking_k);
        
        let exec_timer = Timer::new("search_complex::execute");
        let results: Vec<(usize, f32)> = self.inner.execute(query, blocking_k)
            .into_iter()
            .map(|hit| (hit.doc_id, hit.score))
            .collect();
        drop(exec_timer);
        
        info!("[RUST] Search returned {} results", results.len());
        
        // Log top 10 results with details
        for (i, (doc_id, score)) in results.iter().take(10).enumerate() {
            debug!("[RUST] Result #{}: doc_id={}, score={}", i+1, doc_id, score);
        }
        
        drop(total_timer);
        info!("[RUST] Returning {} results to Python", results.len());
        
        results
    }

    /// Get the total number of documents indexed
    fn get_total_docs(&self) -> usize {
        self.inner.metadata.total_docs
    }
    
    /// Get basic statistics as a formatted string
    fn get_stats(&self) -> String {
        format!("Total docs indexed: {}", self.inner.metadata.total_docs)
    }
}

#[pymodule]
fn lfas(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Initialize pyo3-log to bridge Rust logging to Python
    //info!("[RUST] Registering lfas module");
    //pyo3_log::init();
    
    info!("[RUST] PySearchEngine class registered");
    m.add_class::<PySearchEngine>()?;

    Ok(())
}