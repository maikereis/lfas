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
}
