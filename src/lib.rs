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