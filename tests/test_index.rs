use lfas::index::InvertedIndex;
use lfas::storage::InMemoryStorage;
use lfas::tokenizer::tokenize;

#[derive(Hash, Eq, PartialEq, Clone, Copy, Ord, PartialOrd, Debug)]
enum AddressField {
    Street,
    Municipality,
}

#[test]
fn test_address_field_inverted_index() {
    let storage = InMemoryStorage::new();
    let mut idx = InvertedIndex::<AddressField, InMemoryStorage<AddressField>>::new(storage);

    let doc1_id = 1;
    let addr1 = [
        (AddressField::Street, "Travessa Mauriti"),
        (AddressField::Municipality, "Belém"),
    ];

    for (field, text) in addr1 {
        for token in tokenize(text) {
            idx.add_term(doc1_id, field, token);
        }
    }

    let street_postings = idx
        .get_postings(AddressField::Street, "mauriti")
        .expect("Term not found");
    assert!(street_postings.contains(1));
    assert_eq!(*street_postings.frequencies.get(&1).unwrap(), 1);
}

#[test]
fn test_generic_set_operations() {
    let storage = InMemoryStorage::new();
    let mut idx = InvertedIndex::<AddressField, InMemoryStorage<AddressField>>::new(storage);

    // Doc 1: Travessa Mauriti, Belém
    idx.add_term(1, AddressField::Street, "travessa".to_string());
    idx.add_term(1, AddressField::Street, "mauriti".to_string());
    idx.add_term(1, AddressField::Municipality, "belem".to_string());

    // Doc 2: Avenida Mauriti, Santarém
    idx.add_term(2, AddressField::Street, "avenida".to_string());
    idx.add_term(2, AddressField::Street, "mauriti".to_string());
    idx.add_term(2, AddressField::Municipality, "santarem".to_string());

    // Intra-field Intersection (Street: avenida AND mauriti)
    let bm1 = idx.term_bitmap(AddressField::Street, "avenida");
    let bm2 = idx.term_bitmap(AddressField::Street, "mauriti");
    let intersection =
        InvertedIndex::<AddressField, InMemoryStorage<AddressField>>::intersect(&[bm1, bm2]);

    assert!(intersection.contains(2));
    assert!(!intersection.contains(1));

    // Intra-field Union (Municipality: belem OR santarem)
    let bm3 = idx.term_bitmap(AddressField::Municipality, "belem");
    let bm4 = idx.term_bitmap(AddressField::Municipality, "santarem");
    let union =
        InvertedIndex::<AddressField, InMemoryStorage<AddressField>>::union(&[bm3, bm4]);

    assert_eq!(union.len(), 2);

    // Inter-field Intersection (Street: mauriti AND Municipality: belem)
    let bm_mauriti = idx.term_bitmap(AddressField::Street, "mauriti");
    let bm_belem = idx.term_bitmap(AddressField::Municipality, "belem");
    let inter_field = InvertedIndex::<AddressField, InMemoryStorage<AddressField>>::intersect(&[
        bm_mauriti, bm_belem,
    ]);

    assert!(inter_field.contains(1));
    assert!(!inter_field.contains(2));
    assert_eq!(inter_field.len(), 1);
}
