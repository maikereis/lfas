use super::PostingsStorage;
use crate::postings::Postings;
use heed::types::{Bytes, Str};
use heed::{Database, Env, EnvOpenOptions, RoTxn};
use once_cell::sync::Lazy;
use serde::{Serialize, de::DeserializeOwned};
use std::fs::create_dir_all;
use std::hash::Hash;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Mutex;

static OPEN_ENVS: Lazy<Mutex<std::collections::HashSet<std::path::PathBuf>>> =
    Lazy::new(|| Mutex::new(std::collections::HashSet::new()));

pub const BATCH_SIZE: usize = 100_000;
pub const MAP_SIZE: usize = 10 * 1024 * 1024 * 1024; // 10GB
pub const NUM_DBS: u32 = 10;

#[derive(Debug)]
pub enum LmdbError {
    HeedError(heed::Error),
    SerializationError(bincode::Error),
    CallbackError(String),
}

impl std::fmt::Display for LmdbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LmdbError::HeedError(e) => write!(f, "LMDB error: {}", e),
            LmdbError::SerializationError(e) => write!(f, "Serialization error: {}", e),
            LmdbError::CallbackError(e) => write!(f, "Callback error: {}", e),
        }
    }
}

impl std::error::Error for LmdbError {}

struct WriteBuffer {
    entries: Vec<(String, Vec<u8>)>,
}

impl WriteBuffer {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Vec::with_capacity(capacity),
        }
    }

    fn push(&mut self, key: String, value: Vec<u8>) {
        self.entries.push((key, value));
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn sort(&mut self) {
        self.entries.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    }

    fn drain(&mut self) -> std::vec::Drain<'_, (String, Vec<u8>)> {
        self.entries.drain(..)
    }
}

/// High-performance LMDB storage with transaction reuse
pub struct LmdbStorage<F>
where
    F: Hash + Eq + Clone + Ord + Copy + Serialize + DeserializeOwned,
{
    env: Env,
    db: Database<Str, Bytes>,
    _phantom: PhantomData<F>,
    write_buffer: Mutex<WriteBuffer>,
    batch_size: usize,
}

impl<F> LmdbStorage<F>
where
    F: Hash + Eq + Clone + Ord + Copy + Serialize + DeserializeOwned,
{
    pub fn flush(&self) -> Result<(), LmdbError> {
        let mut buffer = self.write_buffer.lock().unwrap();
        if buffer.is_empty() {
            return Ok(());
        }

        buffer.sort();

        let mut wtxn = self.env.write_txn().map_err(LmdbError::HeedError)?;

        for (key, value_bytes) in buffer.drain() {
            self.db
                .put(&mut wtxn, &key, &value_bytes)
                .map_err(LmdbError::HeedError)?;
        }

        wtxn.commit().map_err(LmdbError::HeedError)?;
        Ok(())
    }

    #[inline]
    fn encode_key(field: F, term: &str) -> Result<String, bincode::Error> {
        let field_bytes = bincode::serialize(&field)?;
        let mut key = String::with_capacity(field_bytes.len() * 2 + 1 + term.len());

        for &byte in &field_bytes {
            use std::fmt::Write;
            write!(&mut key, "{:02x}", byte).unwrap();
        }

        key.push(':');
        key.push_str(term);
        Ok(key)
    }

    #[inline]
    fn decode_key(key: &str) -> Result<(F, String), bincode::Error> {
        let colon_pos = key.find(':').ok_or_else(|| {
            bincode::Error::new(bincode::ErrorKind::Custom("Missing colon".into()))
        })?;

        let field_hex = &key[..colon_pos];
        let term = &key[colon_pos + 1..];

        let mut field_bytes = Vec::with_capacity(field_hex.len() / 2);
        for chunk in field_hex.as_bytes().chunks(2) {
            let hex_str = std::str::from_utf8(chunk)
                .map_err(|e| bincode::Error::new(bincode::ErrorKind::Custom(e.to_string())))?;
            let byte = u8::from_str_radix(hex_str, 16)
                .map_err(|e| bincode::Error::new(bincode::ErrorKind::Custom(e.to_string())))?;
            field_bytes.push(byte);
        }

        let field: F = bincode::deserialize(&field_bytes)?;
        Ok((field, term.to_string()))
    }

    // Get with existing transaction (for batch operations)
    fn get_with_txn(
        &self,
        txn: &RoTxn,
        field: F,
        term: &str,
    ) -> Result<Option<Postings>, LmdbError> {
        let key = Self::encode_key(field, term).map_err(LmdbError::SerializationError)?;

        match self.db.get(txn, &key).map_err(LmdbError::HeedError)? {
            Some(bytes) => {
                let postings: Postings =
                    bincode::deserialize(bytes).map_err(LmdbError::SerializationError)?;
                Ok(Some(postings))
            }
            None => Ok(None),
        }
    }

    // Batch get operation with single transaction
    pub fn get_batch(&self, queries: &[(F, String)]) -> Result<Vec<Option<Postings>>, LmdbError> {
        let rtxn = self.env.read_txn().map_err(LmdbError::HeedError)?;

        let mut results = Vec::with_capacity(queries.len());
        for (field, term) in queries {
            results.push(self.get_with_txn(&rtxn, *field, term)?);
        }

        Ok(results)
    }

    pub fn scan<E>(
        &self,
        mut callback: impl FnMut(F, &str, &[u8]) -> Result<(), E>,
    ) -> Result<(), LmdbError>
    where
        E: std::fmt::Display,
    {
        let rtxn = self.env.read_txn().map_err(LmdbError::HeedError)?;
        for result in self.db.iter(&rtxn).map_err(LmdbError::HeedError)? {
            let (key_str, value_bytes) = result.map_err(LmdbError::HeedError)?;
            let (field, term) = Self::decode_key(key_str).map_err(LmdbError::SerializationError)?;
            callback(field, &term, value_bytes)
                .map_err(|e| LmdbError::CallbackError(e.to_string()))?;
        }
        Ok(())
    }
}

impl<F> LmdbStorage<F>
where
    F: Hash + Eq + Clone + Ord + Copy + Serialize + DeserializeOwned + 'static + std::fmt::Debug,
{
    pub fn open(path: &Path) -> Result<Self, heed::Error> {
        Self::open_with_batch_size(path, BATCH_SIZE)
    }

    pub fn open_with_batch_size(path: &Path, batch_size: usize) -> Result<Self, heed::Error> {
        create_dir_all(path)?;

        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(MAP_SIZE)
                .max_dbs(NUM_DBS)
                .max_readers(126) // Increase max concurrent readers
                .open(path)?
        };

        let mut wtxn = env.write_txn()?;
        let db = env.create_database(&mut wtxn, Some("postings"))?;
        wtxn.commit()?;

        Ok(Self {
            env,
            db,
            _phantom: PhantomData,
            write_buffer: Mutex::new(WriteBuffer::with_capacity(batch_size)),
            batch_size,
        })
    }
}

impl<F> PostingsStorage<F> for LmdbStorage<F>
where
    F: Hash + Eq + Clone + Ord + Copy + Serialize + DeserializeOwned,
{
    type Error = LmdbError;

    fn get(&self, field: F, term: &str) -> Result<Option<Postings>, Self::Error> {
        // Create transaction only when needed, drops immediately
        let rtxn = self.env.read_txn().map_err(LmdbError::HeedError)?;
        self.get_with_txn(&rtxn, field, term)
    }

    fn put(&mut self, field: F, term: String, postings: Postings) -> Result<(), Self::Error> {
        let key = Self::encode_key(field, &term).map_err(LmdbError::SerializationError)?;
        let value_bytes = bincode::serialize(&postings).map_err(LmdbError::SerializationError)?;

        {
            let mut buffer = self.write_buffer.lock().unwrap();
            buffer.push(key, value_bytes);
            if buffer.len() < self.batch_size {
                return Ok(());
            }
        }

        self.flush()
    }

    fn contains(&self, field: F, term: &str) -> Result<bool, Self::Error> {
        let key = Self::encode_key(field, term).map_err(LmdbError::SerializationError)?;
        let rtxn = self.env.read_txn().map_err(LmdbError::HeedError)?;
        Ok(self
            .db
            .get(&rtxn, &key)
            .map_err(LmdbError::HeedError)?
            .is_some())
    }

    fn iter(&self) -> Box<dyn Iterator<Item = Result<((F, String), Postings), Self::Error>> + '_> {
        let mut results = Vec::new();
        if let Err(e) = self.scan(|field, term, bytes| {
            let postings: Postings = bincode::deserialize(bytes).map_err(|e| e.to_string())?;
            results.push(Ok(((field, term.to_string()), postings)));
            Ok::<_, String>(())
        }) {
            results.push(Err(e));
        }
        Box::new(results.into_iter())
    }

    fn scan<E>(
        &self,
        callback: impl FnMut(F, &str, &[u8]) -> Result<(), E>,
    ) -> Result<(), Self::Error>
    where
        E: std::fmt::Display,
    {
        self.scan(callback)
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        LmdbStorage::flush(self)
    }
}

impl<F> Drop for LmdbStorage<F>
where
    F: Hash + Eq + Clone + Ord + Copy + Serialize + DeserializeOwned,
{
    fn drop(&mut self) {
        let _ = self.flush();

        if let Ok(path) = self.env.path().canonicalize() {
            let mut envs = OPEN_ENVS.lock().unwrap();
            envs.remove(&path);
        }
    }
}
