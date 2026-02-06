use std::io;

use lfas::index::InvertedIndex;
use lfas::tokenizer::tokenize;
use lfas::{Record, RecordField};

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

    for result in rdr.deserialize::<Record>().take(100000) {
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

    let bm_sao = idx.term_bitmap(RecordField::Municipio, "sao");
    let bm_do = idx.term_bitmap(RecordField::Municipio, "capim");

    let intersection = InvertedIndex::<RecordField>::intersect(&[bm_sao, bm_do]);
    println!("{:?}", intersection);

    Ok(())
}
