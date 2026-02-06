from lfas import PySearchEngine

def main():
    engine = PySearchEngine()
    # Simulating CSV row ingestion
    engine.index_record(1, "rua", "Avenida Mauriti")
    engine.index_record(1, "municipio", "Belem")

    # Search returns (doc_id, score)
    results = engine.search("mauriti belem", top_k=5)
    print(f"Top Result ID: {results[0][0]} with Score: {results[0][1]}")

if __name__ == '__main__':
    main()