use lfas::tokenizer::{tokenize, tokenize_structured};

#[test]
fn test_tokenizer_include_state_name() {
    let input = "Pará, Belém, Travessa Mauriti, 31, 67000-000, PA, Rua 3, BR-010, km 8";
    let tokens = tokenize(input);

    assert!(
        tokens.contains(&"para".to_string()),
        "Should contain 'para'"
    );
}

#[test]
fn test_tokenizer_include_cep() {
    let input = "Pará, Belém, Travessa Mauriti, 31, 67000-000, PA, Rua 3, BR-010, km 8";
    let tokens = tokenize(input);

    assert!(
        tokens.contains(&"67000-000".to_string()),
        "Should contain CEP"
    );
}

#[test]
fn test_tokenizer_clean_address() {
    let input = "Pará, Belém, Travessa Mauriti, 31, 67000-000, PA, Rua 3, BR-010, km 8";
    let tokens = tokenize(input);

    assert!(tokens.contains(&"belem".to_string()));
    assert!(tokens.contains(&"mauriti".to_string()));
    assert!(tokens.contains(&"31".to_string()));
    assert!(tokens.contains(&"travessa".to_string()));
}

#[test]
fn test_tokenizer_handles_hyphenated_highways() {
    let input = "Rodovia BR-316";
    let tokens = tokenize(input);

    assert!(
        tokens.contains(&"br 316".to_string()),
        "Should contain 'br 316'"
    );
}

#[test]
fn test_tokenizer_deduplication() {
    let input = "Rua Rua Rua 10";
    let tokens = tokenize(input);

    let count = tokens.iter().filter(|t| *t == "rua").count();
    assert_eq!(count, 1, "Tokens should be unique (HashSet)");
}

#[test]
fn test_weak_tokens_minimum_length() {
    let input = "ABC";
    let tokens = tokenize(input);

    assert!(tokens.contains(&"abc".to_string()));
}

#[test]
fn test_distinctive_vs_all_tokens() {
    let token_set = tokenize_structured("Travessa 123 Belém");

    // Short numbers (1–3 digits) are NOT distinctive on their own: they appear
    // in virtually every address block and would make candidate sets too large
    // to be useful for filtering.  The tokenizer only promotes numbers with
    // 4+ digits to distinctive.
    assert!(
        !token_set.distinctive.contains(&"123".to_string()),
        "Short number (< 4 digits) should NOT be distinctive on its own"
    );

    // When a short number immediately follows an address-type prefix the pair
    // forms a distinctive n-gram (e.g. "travessa 123"), because the combination
    // is far more selective than either token alone.
    assert!(
        token_set.distinctive.contains(&"travessa 123".to_string()),
        "Address-type + number n-gram should be distinctive"
    );

    // Short numbers still land in `all` so they contribute to BM25F scoring.
    assert!(
        token_set.all.contains(&"123".to_string()),
        "Short number should still be in all tokens for scoring"
    );
    assert!(token_set.all.contains(&"belem".to_string()));
    assert!(token_set.all.contains(&"travessa".to_string()));
}

#[test]
fn test_long_number_is_distinctive() {
    // Numbers with 4+ digits (e.g. a CEP prefix, a lot number) are distinctive
    // on their own because they are selective enough for candidate filtering.
    let token_set = tokenize_structured("Lote 1234");

    assert!(
        token_set.distinctive.contains(&"1234".to_string()),
        "4-digit number should be distinctive"
    );
    assert!(
        token_set.all.contains(&"1234".to_string()),
        "4-digit number should also be in all tokens"
    );
}