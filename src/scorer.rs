pub struct BM25FScorer<F> {
    pub k1: f32,
    pub field_weights: std::collections::HashMap<F, f32>,
    pub field_b: std::collections::HashMap<F, f32>,
}

impl<F> BM25FScorer<F>
where
    F: std::hash::Hash + Eq + Clone + Copy + Ord,
{
    pub fn score(
        &self,
        matches: RoaringBitmap,
        query_terms: &[String],
        index: &InvertedIndex<F>,
        metadata: &FieldMetadata<F>,
    ) -> Vec<(DocId, f32)> {
        let mut scores = Vec::new();
        let avg_lengths = self.calculate_avg_lengths(metadata);

        for doc_id_u32 in matches.iter() {
            let doc_id = doc_id_u32 as usize;
            let mut doc_score = 0.0;

            for term in query_terms {
                let mut weighted_tf = 0.0;

                // Aggregate signal across all fields for this specific document
                for (&field, &weight) in &self.field_weights {
                    if let Some(postings) = index.get_postings(field, term) {
                        if let Some(&tf) = postings.frequencies.get(&doc_id) {
                            let b = *self.field_b.get(&field).unwrap_or(&0.75);
                            let dl = *metadata
                                .lengths
                                .get(&doc_id)
                                .and_then(|m| m.get(&field))
                                .unwrap_or(&0) as f32;
                            let avgdl = *avg_lengths.get(&field).unwrap_or(&1.0);

                            // Field-specific length normalization
                            weighted_tf += (tf as f32 * weight) / (1.0 + b * (dl / avgdl - 1.0));
                        }
                    }
                }

                let idf = self.calculate_idf(term, index, metadata.total_docs);
                doc_score += idf * (weighted_tf / (self.k1 + weighted_tf));
            }
            scores.push((doc_id, doc_score));
        }

        scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scores
    }

    fn calculate_avg_lengths(
        &self,
        metadata: &FieldMetadata<F>,
    ) -> std::collections::HashMap<F, f32> {
        metadata
            .total_field_lengths
            .iter()
            .map(|(&f, &total)| (f, total as f32 / metadata.total_docs as f32))
            .collect()
    }

    fn calculate_idf(&self, term: &str, index: &InvertedIndex<F>, total_docs: usize) -> f32 {
        // Collect document frequency across all fields (union of bitmaps)
        let mut df_bitmap = RoaringBitmap::new();
        for ((_, t), postings) in &index.postings {
            if t == term {
                df_bitmap |= &postings.bitmap;
            }
        }
        let df = df_bitmap.len() as f32;
        ((total_docs as f32 - df + 0.5) / (df + 0.5) + 1.0).ln()
    }
}
