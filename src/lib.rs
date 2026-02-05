use std::borrow::Cow;
pub mod tokenizer;
pub mod index;

#[cfg(test)]
mod tests {
    use super::*;
    // Explicitly import the items needed for the test
    use crate::index::InvertedIndex; 
    use std::collections::BTreeMap;

    #[test]
    fn test_indexing_logic() {
        // Now 'InvertedIndex' and 'BTreeMap' are recognized
        let mut idx = InvertedIndex { index: BTreeMap::new() };
        
        // Simulate adding tokens from Doc ID 1
        idx.add(1, Cow::Borrowed("belem"));
        idx.add(1, Cow::Borrowed("mauriti"));
        
        // Add same token for Doc ID 2
        idx.add(2, Cow::Borrowed("belem"));

        let belem_docs = idx.index.get("belem").unwrap();
        assert_eq!(belem_docs, &vec![1, 2]);
        
        println!("Index state: {:?}", idx.index);
    }
}