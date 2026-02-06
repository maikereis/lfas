use std::{io};

use lfas::{index::InvertedIndex, tokenizer::tokenize};

#[derive(Hash, Eq, PartialEq, Clone, Copy, Ord, PartialOrd, Debug)]
enum RecordField {
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
struct Record {
    id: String,
    estado: String,
    municipio: String,
    bairro: String,
    cep: String,
    tipo_logradouro: String,
    rua: String,
    numero: String,
    complemento: String,
    nome: String,
}

impl Record {
    fn fields(&self) -> Vec<(RecordField, &str)> {
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

//fn read_csv(n_rows: usize)-> Result<(), Box<dyn Error>> {
//    let mut rdr = csv::Reader::from_reader(io::stdin());
//    for result in rdr.deserialize().take(n_rows) {
//        let record: Record = result?;
//        println!("{:?}", record);
//    }
//    Ok(())
//}

fn main() -> Result<(), Box<dyn std::error::Error>> {

    //if let Err(err) = read_csv(1000) {
    //    println!("error running example: {}", err);
    //}


    let mut rdr = csv::Reader::from_reader(io::stdin());
    
    // F is now RecordField, DocId is usize
    let mut idx: InvertedIndex<RecordField> = InvertedIndex::new();
    let mut id_map: Vec<String> = Vec::new();

    for result in rdr.deserialize::<Record>().take(10000) {
        let record: Record = result?; 
        
        let internal_id = id_map.len();
        id_map.push(record.id.clone());

        for (field_enum, text) in record.fields() {
            for token in tokenize(text) {
                // field_enum is RecordField, matching the index type
                idx.add_term(internal_id, field_enum, token);
            }
        }
 
    }

    if let Some(postings) = idx.get_postings(RecordField::Municipio, "belem") {
        println!("{:?}", postings.bitmap);
    }

    let intersection = idx.intersect_terms(RecordField::Municipio, &["belem"]);
    println!("{:?}", intersection);


    Ok(())
}
