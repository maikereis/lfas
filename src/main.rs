use lfas::index::InvertedIndex;
use lfas::storage::InMemoryStorage;
use lfas::tokenizer::tokenize;
use lfas::{Record, RecordField};
use std::io;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut rdr = csv::Reader::from_reader(io::stdin());

    let storage = InMemoryStorage::new();
    let mut idx: InvertedIndex<RecordField, InMemoryStorage<RecordField>> =
        InvertedIndex::new(storage);
    let mut id_map: Vec<String> = Vec::new();

    for result in rdr.deserialize::<Record>().take(100000) {
        let record: Record = result?;

        let internal_id = id_map.len();
        id_map.push(record.id.clone());

        for (field_enum, text) in record.fields() {
            for token in tokenize(text) {
                idx.add_term(internal_id, field_enum, token);
            }
        }
    }

    if let Some(postings) = idx.get_postings(RecordField::Municipio, "belem") {
        println!("{:?}", postings.bitmap);
    }

    let bm_sao = idx.term_bitmap(RecordField::Municipio, "sao");
    let bm_do = idx.term_bitmap(RecordField::Municipio, "capim");

    let intersection =
        InvertedIndex::<RecordField, InMemoryStorage<RecordField>>::intersect(&[bm_sao, bm_do]);
    println!("{:?}", intersection);

    Ok(())
}
