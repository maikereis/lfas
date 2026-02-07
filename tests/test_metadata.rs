use lfas::metadata::FieldMetadata;

#[derive(Hash, Eq, PartialEq, Clone, Copy, Ord, PartialOrd, Debug)]
enum AddressField {
    Street,
    Neighborhood,
}

#[test]
fn test_field_metadata_tracking() {
    let mut meta = FieldMetadata::<AddressField>::new();
    let doc_id = 101;

    let fields = vec![
        (AddressField::Street, vec!["rua", "augusta"]),
        (AddressField::Neighborhood, vec!["consolação"]),
    ];

    meta.total_docs += 1;
    let doc_entry = meta.lengths.entry(doc_id).or_default();

    for (field, tokens) in fields {
        let len = tokens.len();
        doc_entry.insert(field, len);

        let total_field_len = meta.total_field_lengths.entry(field).or_insert(0);
        *total_field_len += len;
    }

    assert_eq!(meta.total_docs, 1);
    assert_eq!(meta.lengths[&doc_id][&AddressField::Street], 2);
    assert_eq!(meta.total_field_lengths[&AddressField::Neighborhood], 1);
}
