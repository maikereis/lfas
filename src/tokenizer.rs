use lazy_static::lazy_static;
use regex::{Regex, RegexBuilder};
use std::collections::HashSet;
use stopwords::{Language, NLTK, Stopwords};
use unicode_normalization::UnicodeNormalization;

pub const FEDERATIVE_UNITS: &[&str] = &["PA", "MA", "PI", "AL", "RS", "GO"];

pub const HIGHWAY_PREFIX: &[&str] = &["km", "br"];

pub const CUSTOM_STOPWORDS: &[&str] = &[
    "de", "da", "do", "das", "dos", "em", "na", "no", "nas", "nos", "as", "os", "um", "uma", "uns",
    "umas", "pelo", "pela", "por", "para", "com", "sem", "sobre", "entre", "ate", "desde",
];

pub const ADDRESS_TYPE: &[&str] = &[
    "travessa",
    "rua",
    "beco",
    "avenida",
    "ramal",
    "rodovia",
    "passagem",
    "alameda",
    "vila",
    "estrada",
    "igarape",
    "aglomerado",
    "folha",
    "ponte",
    "ruela",
    "vicinal",
    "travessao",
    "assentamento",
    "quadra",
    "rio",
    "comunidade",
    "acesso",
    "praca",
    "condominio",
    "vilarejo",
    "via",
    "residencial",
    "aldeia",
    "sitio",
    "caminho",
    "furo",
    "beirada",
    "chacara",
    "grota",
    "passarela",
    "loteamento",
    "fazenda",
    "planalto",
    "linha",
    "divisa",
    "ilha",
    "quilometro",
    "povoado",
    "agrovila",
    "conjunto",
    "outros",
    "propriedade",
    "colonia",
    "lago",
    "canal",
    "trilha",
    "costa",
    "perimetro",
    "regiao",
    "retiro",
    "marginal",
    "entrada",
    "trevo",
    "quilombo",
    "afluente",
    "eixo",
    "praia",
    "baixa",
    "margens",
    "viela",
    "invasao",
    "porto",
    "aeroporto",
    "baia",
    "contorno",
    "terra",
    "baixadao",
    "margem",
    "nucleo",
    "paralela",
    "descida",
    "arraial",
    "alto",
    "setor",
    "beira",
    "area",
    "buraco",
    "corrego",
    "bairro",
    "varzea",
    "desvio",
    "cabeceira",
    "campo",
    "prolongamento",
    "parque",
    "vale",
    "transversal",
    "trecho",
    "areal",
    "barra",
    "estancia",
    "corredor",
    "lagoa",
    "jardim",
    "gleba",
    "cruzamento",
    "perimetral",
    "reta",
    "boulevard",
    "arteria",
    "lugarejo",
    "travessia",
    "sede",
    "variante",
    "centro",
    "colina",
    "maloca",
    "atalho",
    "rancho",
    "volta",
    "enseada",
    "3a travessa da rua",
    "extensao",
    "lote",
    "limite",
    "1a travessa da rua",
    "terreno",
    "zona",
    "largo",
    "vereda",
    "esquina",
    "circular",
    "rampa",
    "ladeira",
    "2a travessa da rua",
    "5a travessa da rua",
    "4a travessa da rua",
    "ponta",
    "garimpo",
    "riacho",
    "granja",
    "balneario",
    "acampamento",
    "serra",
    "bloco",
    "baixada",
    "estadio",
    "rotatoria",
    "alagado",
    "trilho",
    "seringal",
    "cerca",
    "baixo",
    "orla",
    "saida",
    "tapera",
    "continuacao",
    "seta",
    "adro",
    "barragem",
    "cachoeirinha",
    "fonte",
    "ribeirao",
    "estacionamento",
    "mata",
    "haras",
    "terrenos",
    "unidade",
    "2a travessa",
    "retorno",
    "riachao",
    "baixao",
    "viaduto",
    "acude",
    "oca",
    "trilhos",
    "galeria",
    "projetada",
    "lado",
    "parada",
    "final",
    "escadinha",
    "canteiro",
    "marina",
    "cohab",
    "ferrovia",
    "patio",
    "vertente",
    "projeto",
    "fundos",
    "faixa",
    "encosta",
    "entreposto",
    "terminal",
    "ligacao",
    "calcada",
    "gameleira",
    "entroncamento",
    "morro",
    "esplanada",
    "vala",
    "aleia",
    "posto",
    "capoeira",
    "subida",
    "feira",
    "distrito",
    "pedras",
    "palafita",
    "bosque",
    "cais",
    "1a travessa da avenida",
    "boqueirao",
    "edificio",
    "capao",
    "et",
    "so",
    "lt",
    "pq",
    "bl",
    "ps",
    "ad",
    "al",
    "qd",
    "pr",
    "gr",
    "av",
    "tv",
    "jd",
    "ac",
    "as",
    "ia",
    "fa",
    "st",
    "ld",
    "pv",
    "vl",
    "cd",
    "pa",
    "bv",
    "lg",
    "pj",
    "dt",
    "r",
    "fl",
    "cl",
    "pc",
    "il",
    "bc",
    "fe",
    "pt",
    "mr",
    "rm",
    "rd",
    "vc",
    "cj",
];

lazy_static! {
    static ref RE: Regex = RegexBuilder::new(r"\d{5}-\d{3}|S/N|\d+|[a-z]+").case_insensitive(true).build().unwrap();
    static ref RE_CEP: Regex = RegexBuilder::new(r"\d{5}-?\d{3}").case_insensitive(true).build().unwrap();
    static ref RE_NUMBER: Regex = RegexBuilder::new(r"\d+|sn|s/n").case_insensitive(true).build().unwrap();
    static ref STOP_WORDS_SET: HashSet<&'static str> = CUSTOM_STOPWORDS.iter().copied().collect();
    static ref RE_STREET_NUMBER: Regex = Regex::new(r"^\d+$").unwrap();
    static ref RE_SHORT_NUMBER: Regex = Regex::new(r"\d{1,3}").unwrap();
    static ref ADDRESS_TYPE_SET: HashSet<&'static str> = ADDRESS_TYPE.iter().copied().collect();
    static ref UFS_SET: HashSet<&'static str> = FEDERATIVE_UNITS.iter().copied().collect();
    static ref HIGHWAY_PREFIX_SET: HashSet<&'static str> = HIGHWAY_PREFIX.iter().copied().collect();


    static ref NLTK_STOPS: HashSet<String> = {
        let language = Language::Portuguese;
        // Use a fallback empty set if NLTK fails to load
        NLTK::stopwords(language).unwrap_or_default().iter().map(|s| s.to_string()).collect()
    };
}

pub fn extract_weak_tokens(tokens: &HashSet<String>, n: usize) -> HashSet<String> {
    let mut weak_tokens = HashSet::new();

    for token in tokens {
        let bytes = token.as_bytes();
        if bytes.len() >= n {
            let mut i = 0;
            while i + n <= bytes.len() {
                if let Ok(slice) = std::str::from_utf8(&bytes[i..i + n]) {
                    weak_tokens.insert(slice.to_string());
                }
                i += n;
            }
        }
    }
    weak_tokens
}

pub struct TokenSet {
    pub distinctive: HashSet<String>,  // For candidate filtering
    pub all: HashSet<String>,           // For scoring
}

pub fn tokenize_structured(text: &str) -> TokenSet {
    let normalized: String = text
        .nfd()
        .filter(|c| !unicode_normalization::char::is_combining_mark(*c))
        .collect::<String>()
        .to_lowercase();

    let mut tokens_list: Vec<String> = RE
        .find_iter(&normalized)
        .map(|m| m.as_str().to_string())
        .filter(|token| !STOP_WORDS_SET.contains(token.as_str()) && !NLTK_STOPS.contains(token))
        .collect();

    if text.to_lowercase().contains("pará") {
        tokens_list.push("para".to_string());
    }

    let mut distinctive_tokens = HashSet::new();
    let mut all_tokens = HashSet::new();

    // Process Strong/Distinctive Tokens (N-grams, phrases)
    for window in tokens_list.windows(2) {
        let first = &window[0];
        let second = &window[1];

        if ADDRESS_TYPE_SET.contains(first.as_str()) && RE_STREET_NUMBER.is_match(second) {
            distinctive_tokens.insert(format!("{} {}", first, second));
        }

        if HIGHWAY_PREFIX_SET.contains(first.as_str()) && RE_SHORT_NUMBER.is_match(second) {
            distinctive_tokens.insert(format!("{} {}", first, second));
        }
    }

    // Identity & Specialized Tokens (distinctive)
    for t in &tokens_list {
        if RE_CEP.is_match(t) || UFS_SET.contains(t.as_str()) {
            distinctive_tokens.insert(t.clone());
        }
        if RE_NUMBER.is_match(t) && t.len() >= 3 {
            // House numbers are distinctive
            distinctive_tokens.insert(t.clone());
        }
        all_tokens.insert(t.clone());
    }

    // Weak Tokens (for scoring only, not filtering)
    let weak_tokens = extract_weak_tokens(&all_tokens, 3);
    all_tokens.extend(weak_tokens);
    
    // Copy distinctive tokens to all_tokens
    all_tokens.extend(distinctive_tokens.clone());

    TokenSet {
        distinctive: distinctive_tokens,
        all: all_tokens,
    }
}

// Keep the old function for backward compatibility
pub fn tokenize(text: &str) -> HashSet<String> {
    tokenize_structured(text).all
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenizer_include_state_name() {
        let input = "Pará, Belém, Travessa Mauriti, 31, 67000-000, PA, Rua 3, BR-010, km 8";
        let tokens = tokenize(input);

        assert!(tokens.contains(&"para".to_string()));
    }

    #[test]
    fn test_tokenizer_include_cep() {
        let input = "Pará, Belém, Travessa Mauriti, 31, 67000-000, PA, Rua 3, BR-010, km 8";
        let tokens = tokenize(input);

        assert!(tokens.contains(&"67000-000".to_string()));
    }

    #[test]
    fn test_tokenizer_clean_address() {
        let input = "Pará, Belém, Travessa Mauriti, 31, 67000-000, PA, Rua 3, BR-010, km 8";
        let tokens = tokenize(input);

        // Should remove 'travessa', lowercase 'belem', and keep '31'
        assert!(tokens.contains(&"belem".to_string()));
        assert!(tokens.contains(&"mauriti".to_string()));
        assert!(tokens.contains(&"31".to_string()));
        assert!(tokens.contains(&"travessa".to_string()));
    }

    #[test]
    fn test_tokenizer_handles_hyphenated_highways() {
        let input = "Rodovia BR-316";
        let tokens = tokenize(input);

        // Ensure "br 316" is captured as a strong token even with the hyphen
        // Note: Your current regex \d+|[a-z]+ splits "BR-316" into ["br", "316"]
        assert!(tokens.contains(&"br 316".to_string()));
    }

    #[test]
    fn test_tokenizer_deduplication() {
        let input = "Rua Rua Rua 10";
        let tokens = tokenize(input);

        // Count occurrences of 'rua'
        let count = tokens.iter().filter(|t| *t == "rua").count();
        assert_eq!(count, 1, "Tokens should be unique (HashSet)");
    }

    #[test]
    fn test_weak_tokens_minimum_length() {
        let input = "ABC";
        let tokens = tokenize(input);

        // "abc" should exist, but if it's too short,
        // we might not want it to generate further weak tokens.
        assert!(tokens.contains(&"abc".to_string()));
    }
}
