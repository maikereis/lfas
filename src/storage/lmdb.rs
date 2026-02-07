use super::PostingsStorage;
use crate::postings::Postings;
use heed::types::{Bytes, SerdeBincode};
use heed::{Database, Env, EnvOpenOptions};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::fs::create_dir_all;
use std::hash::Hash;
use std::iter::once;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Mutex;

pub struct LmdbStorage<F>
where
    F: Hash + Eq + Clone + Ord + Serialize + DeserializeOwned,
{
    env: Env,
    db: Database<SerdeBincode<(F, String)>, Bytes>,
    _phantom: PhantomData<F>,
    write_buffer: Mutex<Vec<((F, String), Postings)>>,
    batch_size: usize,
}

impl<F> LmdbStorage<F>
where
    F: Hash + Eq + Clone + Ord + Serialize + DeserializeOwned + 'static,
{
    pub fn open(path: &Path) -> Result<Self, heed::Error> {
        Self::open_with_batch_size(path, 1_000_000)
    }

    pub fn open_with_batch_size(path: &Path, batch_size: usize) -> Result<Self, heed::Error> {
        create_dir_all(path)?;

        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(15 * 1024 * 1024 * 1024) // 15GB
                .max_dbs(10)
                .open(path)?
        };

        let mut wtxn = env.write_txn()?;
        let db: Database<SerdeBincode<(F, String)>, Bytes> =
            env.create_database(&mut wtxn, Some("postings"))?;
        wtxn.commit()?;

        Ok(Self {
            env,
            db,
            _phantom: PhantomData,
            write_buffer: Mutex::new(Vec::with_capacity(batch_size)),
            batch_size,
        })
    }

    pub fn flush(&self) -> Result<(), LmdbError> {
        let mut buffer = self.write_buffer.lock().unwrap();
        if buffer.is_empty() {
            return Ok(());
        }

        let mut wtxn = self.env.write_txn().map_err(LmdbError::HeedError)?;

        for ((field, term), postings) in buffer.drain(..) {
            let bytes = bincode::serialize(&postings).map_err(LmdbError::SerializationError)?;
            self.db
                .put(&mut wtxn, &(field, term), &bytes)
                .map_err(LmdbError::HeedError)?;
        }

        wtxn.commit().map_err(LmdbError::HeedError)?;
        Ok(())
    }
}

#[derive(Debug)]
pub enum LmdbError {
    HeedError(heed::Error),
    SerializationError(bincode::Error),
}

impl std::fmt::Display for LmdbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LmdbError::HeedError(e) => write!(f, "LMDB error: {}", e),
            LmdbError::SerializationError(e) => write!(f, "Serialization error: {}", e),
        }
    }
}

impl std::error::Error for LmdbError {}

impl<F> PostingsStorage<F> for LmdbStorage<F>
where
    F: Hash + Eq + Clone + Ord + Serialize + DeserializeOwned + Copy + 'static,
{
    type Error = LmdbError;

    fn get(&self, field: F, term: &str) -> Result<Option<Postings>, Self::Error> {
        let rtxn = self.env.read_txn().map_err(LmdbError::HeedError)?;
        let key = (field, term.to_string());

        match self.db.get(&rtxn, &key).map_err(LmdbError::HeedError)? {
            Some(bytes) => {
                let postings: Postings =
                    bincode::deserialize(bytes).map_err(LmdbError::SerializationError)?;
                Ok(Some(postings))
            }
            None => Ok(None),
        }
    }

    fn put(&mut self, field: F, term: String, postings: Postings) -> Result<(), Self::Error> {
        {
            let mut buffer = self.write_buffer.lock().unwrap();
            buffer.push(((field, term), postings));

            if buffer.len() < self.batch_size {
                return Ok(());
            }
        }

        self.flush()
    }

    fn contains(&self, field: F, term: &str) -> Result<bool, Self::Error> {
        Ok(self.get(field, term)?.is_some())
    }

    fn iter(&self) -> Box<dyn Iterator<Item = Result<((F, String), Postings), Self::Error>> + '_> {
        let rtxn = match self.env.read_txn() {
            Ok(txn) => txn,
            Err(e) => return Box::new(once(Err(LmdbError::HeedError(e)))),
        };

        let iter = match self.db.iter(&rtxn) {
            Ok(iter) => iter,
            Err(e) => return Box::new(once(Err(LmdbError::HeedError(e)))),
        };

        let results: Vec<Result<((F, String), Postings), LmdbError>> = iter
            .map(|result| {
                let (key, bytes) = result.map_err(LmdbError::HeedError)?;
                let postings: Postings =
                    bincode::deserialize(bytes).map_err(LmdbError::SerializationError)?;
                Ok((key, postings))
            })
            .collect();

        Box::new(results.into_iter())
    }
}
