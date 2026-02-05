use std::{error::Error, io, process};

#[derive(Debug, serde::Deserialize)]
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

fn read_csv(n_rows: usize)-> Result<(), Box<dyn Error>> {
    let mut rdr = csv::Reader::from_reader(io::stdin());
    for result in rdr.deserialize().take(n_rows) {
        let record: Record = result?;
        println!("{:?}", record);
    }
    Ok(())
}


fn main() {
    if let Err(err) = read_csv(1000) {
        println!("error running example: {}", err);
        process::exit(1);
    }
}
