"""
LFAS - Lightning-Fast Address Search

A high-performance BM25F search engine optimized for Brazilian address data.

Features:
---------
- LMDB-backed persistent inverted index
- Field-aware BM25F scoring
- Concurrent read operations (searches)
- Optimized tokenization for Brazilian addresses
- Batch indexing: 100,000+ docs/second
- Search latency: 10-50ms typical

Example:
--------
>>> from lfas import PySearchEngine
>>> engine = PySearchEngine()
>>> engine.index_dict(0, {'rua': 'Avenida Paulista', 'numero': '1578'})
>>> engine.flush()
>>> results = engine.search_complex({'rua': 'Paulista'}, top_k=10, blocking_k=1000)
>>> print(results)  # [(doc_id, score), ...]
"""

from typing import Dict, List, Tuple

class PySearchEngine:
    """
    High-performance BM25F search engine for Brazilian address data.

    The engine uses a global singleton pattern with LMDB storage in ./lmdb_data
    and supports concurrent read operations (searches) while serializing writes.

    Example:
    --------
    >>> from lfas import PySearchEngine
    >>> engine = PySearchEngine()
    >>> engine.index_dict(0, {
    ...     'rua': 'Avenida Paulista',
    ...     'numero': '1578',
    ...     'municipio': 'São Paulo'
    ... })
    >>> engine.flush()
    >>> results = engine.search_complex({'rua': 'Paulista'}, top_k=10, blocking_k=1000)
    """

    def __init__(self) -> None:
        """
        Create a new search engine instance.

        The constructor initializes or reuses a global LMDB-backed search engine.
        On first call, creates a new LMDB environment in ./lmdb_data directory.
        Subsequent calls reuse the existing environment (singleton pattern).

        Example:
        --------
        >>> engine = PySearchEngine()
        """
        ...

    @staticmethod
    def init_logging() -> None:
        """
        Initialize Rust logging integration with Python.

        This static method should be called once at application startup to enable
        Rust log messages to appear in Python logging output.

        Example:
        --------
        >>> PySearchEngine.init_logging()
        """
        ...

    def index_dict(self, doc_id: int, record_dict: Dict[str, str]) -> None:
        """
        Index a single document with field-value pairs.

        For bulk indexing, use index_batch() instead as it's 10-20x faster.

        Parameters:
        -----------
        doc_id : int
            Unique document identifier (must be >= 0)
        record_dict : Dict[str, str]
            Dictionary mapping field names to values.
            Valid fields: 'estado', 'municipio', 'bairro', 'cep',
            'tipo_logradouro', 'rua', 'numero', 'complemento', 'nome'

        Example:
        --------
        >>> engine.index_dict(0, {
        ...     'rua': 'Travessa WE 8',
        ...     'numero': '100',
        ...     'bairro': 'Cidade Nova',
        ...     'municipio': 'Ananindeua',
        ...     'estado': 'PA',
        ...     'cep': '67130-021'
        ... })

        Notes:
        ------
        - All values are automatically tokenized and normalized
        - Empty or missing fields are ignored
        - Updates metadata for BM25F scoring calculations
        """
        ...

    def index_batch(self, records: List[Tuple[int, Dict[str, str]]]) -> None:
        """
        Index multiple documents in a single batch operation.

        This is the recommended method for bulk indexing as it's significantly
        faster than individual index_dict() calls. Uses in-memory aggregation
        to minimize LMDB transaction overhead.

        Parameters:
        -----------
        records : List[Tuple[int, Dict[str, str]]]
            List of (doc_id, record_dict) tuples where:
            - doc_id: Unique document identifier (must be >= 0)
            - record_dict: Dictionary of field names to values

        Example:
        --------
        >>> batch = [
        ...     (0, {'rua': 'Rua A', 'municipio': 'Belém'}),
        ...     (1, {'rua': 'Rua B', 'municipio': 'Belém'}),
        ...     (2, {'rua': 'Rua C', 'municipio': 'Belém'})
        ... ]
        >>> engine.index_batch(batch)
        >>> engine.flush()

        Performance:
        ------------
        - Processes 100,000-200,000 documents/second
        - Use batch sizes of 100,000-500,000 for optimal performance
        - Call flush() after each batch to ensure persistence
        """
        ...

    def flush(self) -> None:
        """
        Flush buffered writes to persistent storage (LMDB).

        This method commits all pending index operations to disk. Should be
        called after indexing operations to ensure data persistence.

        Raises:
        -------
        RuntimeError
            If the flush operation fails

        Example:
        --------
        >>> engine.index_batch(records)
        >>> engine.flush()  # Commit to disk

        Notes:
        ------
        - Automatically called when the engine is destroyed
        - For large batch operations, flush periodically (e.g., every 500k docs)
        """
        ...

    def search_complex(
        self,
        query_dict: Dict[str, str],
        top_k: int,
        blocking_k: int
    ) -> List[Tuple[int, float]]:
        """
        Perform a field-aware BM25F search query.

        Executes a two-stage search:
        1. Candidate retrieval using distinctive tokens (CEP, numbers, n-grams)
        2. BM25F scoring of candidates with all query tokens

        Parameters:
        -----------
        query_dict : Dict[str, str]
            Field-value pairs for the search query.
            Valid fields: 'estado', 'municipio', 'bairro', 'cep',
            'tipo_logradouro', 'rua', 'numero', 'complemento', 'nome'
        top_k : int
            Maximum number of results to return
        blocking_k : int
            Maximum candidate documents to consider (performance/recall tradeoff)

        Returns:
        --------
        List[Tuple[int, float]]
            List of (doc_id, score) tuples sorted by score (descending)

        Example:
        --------
        >>> results = engine.search_complex(
        ...     {
        ...         'rua': 'WE 8',
        ...         'bairro': 'Cidade Nova',
        ...         'municipio': 'Ananindeua'
        ...     },
        ...     top_k=10,
        ...     blocking_k=1000
        ... )
        >>> for doc_id, score in results:
        ...     print(f"Document {doc_id}: {score:.2f}")

        Notes:
        ------
        Search Strategy:
        - Uses distinctive tokens (CEP, numbers, street type+number) for candidate retrieval
        - Fallback to rarest tokens if no distinctive matches found
        - Scores all candidates with full BM25F algorithm

        Performance Tuning:
        - blocking_k=1000: Fast, may miss some relevant results
        - blocking_k=10000: Balanced performance/recall
        - blocking_k=100000: Slower, highest recall
        """
        ...

    def set_field_weights(self, weights: Dict[str, float]) -> None:
        """
        Set custom field importance weights for BM25F scoring.

        Field weights control how much each field contributes to the final
        relevance score. Higher weights make a field more important.

        Parameters:
        -----------
        weights : Dict[str, float]
            Dictionary mapping field names to weight values.
            Valid field names: 'estado', 'municipio', 'bairro', 'cep',
            'tipo_logradouro', 'rua', 'numero', 'complemento', 'nome'

        Example:
        --------
        >>> engine.set_field_weights({
        ...     'cep': 15.0,      # CEP very important
        ...     'numero': 12.0,   # Street number important
        ...     'rua': 5.0        # Street name moderately important
        ... })

        Notes:
        ------
        Default weights:
        - numero: 10.0, cep: 8.0, rua: 5.0, municipio: 3.0, bairro: 2.0,
          complemento: 1.5, estado: 1.0, nome: 1.0, tipo_logradouro: 0.5
        """
        ...

    def set_field_b_values(self, b_values: Dict[str, float]) -> None:
        """
        Set length normalization (b) parameters for BM25F scoring.

        The b parameter controls how much document length affects scoring.
        - b=0.0: No normalization (field length ignored)
        - b=0.75: Standard normalization (recommended)
        - b=1.0: Full normalization (heavily penalizes long fields)

        Parameters:
        -----------
        b_values : Dict[str, float]
            Dictionary mapping field names to b values (0.0 to 1.0).
            Valid field names: 'estado', 'municipio', 'bairro', 'cep',
            'tipo_logradouro', 'rua', 'numero', 'complemento', 'nome'

        Example:
        --------
        >>> engine.set_field_b_values({
        ...     'cep': 0.0,      # No normalization (fixed-length)
        ...     'numero': 0.0,   # No normalization (fixed-length)
        ...     'rua': 0.75,     # Standard normalization
        ...     'bairro': 0.5    # Moderate normalization
        ... })

        Notes:
        ------
        Default b-values:
        - numero, cep, estado, tipo_logradouro: 0.0 (fixed-length identifiers)
        - municipio, complemento: 0.5 (moderate normalization)
        - rua, bairro, nome: 0.75 (standard normalization)
        """
        ...

    def reset_weights(self) -> None:
        """
        Reset field weights and b-values to default settings.

        Example:
        --------
        >>> engine.reset_weights()
        """
        ...

    def get_weights(self) -> Dict[str, float]:
        """
        Get current field weight configuration.

        Returns:
        --------
        Dict[str, float]
            Dictionary of field names to current weight values.

        Example:
        --------
        >>> weights = engine.get_weights()
        >>> print(weights['cep'])
        8.0
        """
        ...

    def get_total_docs(self) -> int:
        """
        Get the total number of indexed documents.

        Returns:
        --------
        int
            Total count of indexed documents

        Example:
        --------
        >>> total = engine.get_total_docs()
        >>> print(f"Indexed {total:,} documents")
        """
        ...

    def get_stats(self) -> str:
        """
        Get formatted index statistics.

        Returns:
        --------
        str
            Human-readable statistics string

        Example:
        --------
        >>> stats = engine.get_stats()
        >>> print(stats)
        Total docs indexed: 1234567
        """
        ...

    def save_metadata(self, path: str) -> None:
        """
        Save index metadata to a binary file.

        Saves document lengths, field statistics, and term document frequencies
        to a file for later loading with load_metadata().

        Parameters:
        -----------
        path : str
            File path for the metadata file

        Raises:
        -------
        IOError
            If file cannot be created or written

        Example:
        --------
        >>> engine.save_metadata("./lmdb_data/metadata.bin")

        Notes:
        ------
        - Required for search operations after restarting
        - Faster than rebuilding metadata from scratch
        - Contains: doc lengths, total field lengths, doc counts, term DFs
        """
        ...

    def load_metadata(self, path: str) -> None:
        """
        Load index metadata from a binary file.

        Loads previously saved metadata required for search operations.
        Must be called before searching when using a pre-built index.

        Parameters:
        -----------
        path : str
            File path to the metadata file

        Raises:
        -------
        IOError
            If file cannot be read or is corrupted

        Example:
        --------
        >>> engine = PySearchEngine()
        >>> engine.load_metadata("./lmdb_data/metadata.bin")
        >>> results = engine.search_complex({'rua': 'Paulista'}, 10, 1000)

        Notes:
        ------
        - Must match the current LMDB index
        - Enables BM25F scoring calculations
        - Much faster than rebuilding from scratch
        """
        ...
