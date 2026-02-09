mod lmdb;
mod memory;

pub use lmdb::{LmdbError, LmdbStorage};
pub use memory::InMemoryStorage;

use crate::postings::Postings;
use std::hash::Hash;

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

    /// Zero-copy streaming iteration via callback
    fn scan<E>(
        &self,
        callback: impl FnMut(F, &str, &[u8]) -> Result<(), E>,
    ) -> Result<(), Self::Error>
    where
        E: std::fmt::Display;

    /// Flush buffered writes to persistent storage (optional, no-op for in-memory)
    fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Batch get with single transaction
    fn get_batch(&self, queries: &[(F, String)]) -> Result<Vec<Option<Postings>>, Self::Error> {
        // Default: fallback to individual gets (for in-memory storage)
        let mut results = Vec::with_capacity(queries.len());
        for (field, term) in queries {
            results.push(self.get(*field, term)?);
        }
        Ok(results)
    }
}