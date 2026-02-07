use crate::postings::Postings;
use heed::types::{Bytes, SerdeBincode};
use heed::{Database, Env, EnvOpenOptions};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::fs::create_dir_all;
use std::iter::once;
use std::marker::PhantomData;
use std::path::Path;
use std::{collections::BTreeMap, hash::Hash};

use std::sync::Mutex;

pub trait PostingsStorage<F>
where
    F: Hash + Eq + Clone + Ord + Copy,
{
    type Error: std::error::Error;

    /// Retrieve postings for a given field-term combination
    fn get(&self, field: F, term: &str) -> Result<Option<Postings>, Self::Error>;

    /// Store or update postings for a given field-term combination
    fn put(&mut self, field: F, term: String, postings: Postings) -> Result<(), Self::Error>;

    /// Check if a term exists in a field
    fn contains(&self, field: F, term: &str) -> Result<bool, Self::Error>;

    /// Iterate over all postings (useful for metadata computation)
    fn iter(&self) -> Box<dyn Iterator<Item = Result<((F, String), Postings), Self::Error>> + '_>;
}

pub struct InMemoryStorage<F>
where
    F: Hash + Eq + Clone + Ord + Copy,
{
    data: BTreeMap<(F, String), Postings>,
}

impl<F> InMemoryStorage<F>
where
    F: Hash + Eq + Clone + Ord + Copy,
{
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
        }
    }
}

impl<F> PostingsStorage<F> for InMemoryStorage<F>
where
    F: Hash + Eq + Clone + Ord + Copy,
{
    type Error = std::convert::Infallible; // In-memory operations don't fail

    fn get(&self, field: F, term: &str) -> Result<Option<Postings>, Self::Error> {
        Ok(self.data.get(&(field, term.to_string())).cloned())
    }

    fn put(&mut self, field: F, term: String, postings: Postings) -> Result<(), Self::Error> {
        self.data.insert((field, term), postings);
        Ok(())
    }

    fn contains(&self, field: F, term: &str) -> Result<bool, Self::Error> {
        Ok(self.data.contains_key(&(field, term.to_string())))
    }

    fn iter(&self) -> Box<dyn Iterator<Item = Result<((F, String), Postings), Self::Error>> + '_> {
        Box::new(
            self.data
                .iter()
                .map(|((f, t), p)| Ok(((*f, t.clone()), p.clone()))),
        )
    }
}

pub struct LmdbStorage<F>
where
    F: Hash + Eq + Clone + Ord + Serialize + DeserializeOwned,
{
    env: Env,
    db: Database<SerdeBincode<(F, String)>, Bytes>,
    _phantom: PhantomData<F>,
    // Batch buffer for write optimization
    write_buffer: Mutex<Vec<((F, String), Postings)>>,
    batch_size: usize,
}

impl<F> LmdbStorage<F>
where
    F: Hash + Eq + Clone + Ord + Serialize + DeserializeOwned + 'static,
{
    pub fn open(path: &Path) -> Result<Self, heed::Error> {
        Self::open_with_batch_size(path, 1_000_000) // Default batch size
    }

    pub fn open_with_batch_size(path: &Path, batch_size: usize) -> Result<Self, heed::Error> {
        create_dir_all(path)?;

        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(15 * 1024 * 1024 * 1024) // 15GB
                .max_dbs(10)
                .open(path)?
        };
        // 1. Create a write transaction
        let mut wtxn = env.write_txn()?;

        // 2. Wrap in unsafe and pass the transaction reference
        // heed requires unsafe here because opening a DB can invalidate
        // existing pointers if not handled correctly.
        let db: Database<SerdeBincode<(F, String)>, Bytes> =
            env.create_database(&mut wtxn, Some("postings"))?;

        // 3. Commit the transaction to persist the database creation
        wtxn.commit()?;

        Ok(Self {
            env,
            db,
            _phantom: PhantomData,
            write_buffer: Mutex::new(Vec::with_capacity(batch_size)),
            batch_size,
        })
    }

    /// Flush any pending writes to disk
    pub fn flush(&self) -> Result<(), LmdbError> {
        // Lock the mutex and get the guard
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
            // Scope the lock so it is released before we potentially call flush()
            let mut buffer = self.write_buffer.lock().unwrap();
            buffer.push(((field, term), postings));

            if buffer.len() < self.batch_size {
                return Ok(());
            }
        } // Lock drops here

        self.flush()
    }

    fn contains(&self, field: F, term: &str) -> Result<bool, Self::Error> {
        Ok(self.get(field, term)?.is_some())
    }

    fn iter(&self) -> Box<dyn Iterator<Item = Result<((F, String), Postings), Self::Error>> + '_> {
        // We need to collect all results because the transaction can't outlive this function
        let rtxn = match self.env.read_txn() {
            Ok(txn) => txn,
            Err(e) => return Box::new(once(Err(LmdbError::HeedError(e)))),
        };

        let iter = match self.db.iter(&rtxn) {
            Ok(iter) => iter,
            Err(e) => return Box::new(once(Err(LmdbError::HeedError(e)))),
        };

        // Collect all items before the transaction is dropped
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
