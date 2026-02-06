pub mod index;
pub mod metadata;
pub mod postings;
pub mod tokenizer;
pub mod scorer;
pub mod engine;

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
use std::collections::HashMap;

#[pyclass]
pub struct PySearchEngine {
    inner: SearchEngine,
}

#[pymethods]
impl PySearchEngine {
    #[new]
    fn new() -> Self {
        // Domain-Driven Design: Setting up the 'Search Context' with Brazilian defaults
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
                field_b: HashMap::new(), // Defaults to 0.75 in scorer logic
            },
        };
        PySearchEngine { inner: engine }
    }

    /// Receives a dictionary of fields for a single DocId
    fn index_dict(&mut self, doc_id: usize, record_dict: HashMap<String, String>) {
        // Pre-allocate capacity if possible
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

            let tokens = tokenize(&text);
            let token_count = tokens.len();

            // Index terms
            for token in tokens {
                self.inner.index.add_term(doc_id, field, token);
            }

            // Update Metadata lengths
            self.inner.metadata.lengths.entry(doc_id).or_default().insert(field, token_count);
            *self.inner.metadata.total_field_lengths.entry(field).or_insert(0) += token_count;
        }

        // Global count
        if doc_id >= self.inner.metadata.total_docs {
            self.inner.metadata.total_docs = doc_id + 1;
        }
    }

    /// Performs a multi-field search using a dictionary of field names and query strings
    fn search_complex(&self, query_dict: HashMap<String, String>, top_k: usize) -> Vec<(usize, f32)> {
        let mut query_fields = Vec::new();

        for (key, text) in query_dict {
            if text.trim().is_empty() { continue; }
            
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

        if query_fields.is_empty() {
            return Vec::new();
        }

        let query = StructuredQuery {
            fields: query_fields,
            top_k,
        };
        
        // Execute with the 100k blocking_k we used before
        self.inner.execute(query, 100_000)
            .into_iter()
            .map(|hit| (hit.doc_id, hit.score))
            .collect()
    }
}

#[pymodule]
fn lfas(m: &Bound<'_, PyModule>) -> PyResult<()> { // Changed from lfas_python
    m.add_class::<PySearchEngine>()?;
    Ok(())
}