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

    assert!(
        token_set.distinctive.contains(&"123".to_string()),
        "Number should be distinctive"
    );
    assert!(
        token_set.distinctive.contains(&"travessa 123".to_string()),
        "N-gram should be distinctive"
    );

    assert!(token_set.all.contains(&"123".to_string()));
    assert!(token_set.all.contains(&"belem".to_string()));
    assert!(token_set.all.contains(&"travessa".to_string()));
}
