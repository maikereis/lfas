use super::PostingsStorage;
use crate::postings::Postings;
use std::collections::BTreeMap;
use std::hash::Hash;

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

impl<F> Default for InMemoryStorage<F>
where
    F: Hash + Eq + Clone + Ord + Copy,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<F> PostingsStorage<F> for InMemoryStorage<F>
where
    F: Hash + Eq + Clone + Ord + Copy,
{
    type Error = std::convert::Infallible;

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

    fn scan<E>(
        &self,
        mut callback: impl FnMut(F, &str, &[u8]) -> Result<(), E>,
    ) -> Result<(), Self::Error>
    where
        E: std::fmt::Display,
    {
        // For in-memory storage, we serialize each posting and call the callback
        for ((field, term), postings) in &self.data {
            // Serialize the postings to bytes
            let bytes = bincode::serialize(postings).unwrap();
            // Call the callback - if it fails, we ignore it since Error is Infallible
            let _ = callback(*field, term, &bytes);
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        // No-op for in-memory storage
        Ok(())
    }
}