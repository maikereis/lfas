"""
LFAS - Lightning-Fast Address Search

High-performance BM25F search engine for Brazilian address data.

Example:
--------
>>> from lfas import SearchEngine
>>> engine = SearchEngine()  # Uses ./lmdb_data
>>> engine = SearchEngine(db_path="./my_index")  # Custom path
>>> engine.index({'rua': 'Avenida Paulista', 'numero': '1578'}, doc_id=0)
>>> engine.flush()
>>> results = engine.search({'rua': 'Paulista'}, top_k=10)
"""

try:
    from lfas._core import PySearchEngine as _PySearchEngine
except ImportError:
    _PySearchEngine = None

from pathlib import Path
from typing import Dict, List, Tuple, Union

__version__ = "0.1.0"
__all__ = ["SearchEngine", "PySearchEngine"]


class SearchEngine:
    """
    High-performance BM25F search engine for Brazilian address data.
    
    This wrapper provides a more Pythonic API with comprehensive documentation.
    
    Example
    -------
    >>> engine = SearchEngine()  # Uses ./lmdb_data
    >>> engine = SearchEngine(db_path="./my_index")  # Custom path
    >>> engine.index({
    ...     'rua': 'Avenida Paulista',
    ...     'numero': '1578',
    ...     'municipio': 'São Paulo'
    ... }, doc_id=0)
    >>> engine.flush()
    >>> results = engine.search({'rua': 'Paulista'}, top_k=10)
    """
    
    def __init__(self, db_path: Union[str, Path] = "./lmdb_data"):
        """
        Initialize a new search engine instance.
        
        Parameters
        ----------
        db_path : str or Path, default="./lmdb_data"
            Path to the LMDB database directory
        """
        if _PySearchEngine is None:
            raise ImportError("LFAS Rust module not found. Run 'maturin develop' first.")
        self.engine = _PySearchEngine(db_path=str(db_path))
        self.db_path = str(db_path)
    
    @staticmethod
    def init_logging():
        """Enable Rust logging integration with Python."""
        if _PySearchEngine is not None:
            _PySearchEngine.init_logging()
    
    def index(self, document: Dict[str, str], doc_id: int) -> None:
        """
        Index a single document.
        
        Parameters
        ----------
        document : Dict[str, str]
            Field-value pairs for the document
        doc_id : int
            Unique document identifier (>= 0)
        """
        self.engine.index_dict(doc_id, document)
    
    def index_batch(self, documents: List[Tuple[int, Dict[str, str]]]) -> None:
        """
        Index multiple documents in a batch (recommended for bulk indexing).
        
        Parameters
        ----------
        documents : List[Tuple[int, Dict[str, str]]]
            List of (doc_id, document) tuples
        """
        self.engine.index_batch(documents)
    
    def flush(self) -> None:
        """Commit all pending writes to disk."""
        self.engine.flush()
    
    def search(
        self,
        query: Dict[str, str],
        top_k: int = 10,
        blocking_k: int = 1000
    ) -> List[Tuple[int, float]]:
        """
        Search the index with a field-aware query.
        
        Parameters
        ----------
        query : Dict[str, str]
            Field-value pairs to search for
        top_k : int, default=10
            Maximum number of results to return
        blocking_k : int, default=1000
            Maximum candidates to score
        
        Returns
        -------
        List[Tuple[int, float]]
            List of (doc_id, score) tuples sorted by score
        """
        return self.engine.search_complex(query, top_k, blocking_k)
    
    def set_weights(self, weights: Dict[str, float]) -> None:
        """Set custom field importance weights for scoring."""
        self.engine.set_field_weights(weights)
    
    def set_b_values(self, b_values: Dict[str, float]) -> None:
        """Set length normalization parameters for scoring."""
        self.engine.set_field_b_values(b_values)
    
    def reset_weights(self) -> None:
        """Reset all weights and b-values to defaults."""
        self.engine.reset_weights()
    
    def get_weights(self) -> Dict[str, float]:
        """Get current field weight configuration."""
        return self.engine.get_weights()
    
    def save_metadata(self, path: Union[str, Path] = None) -> None:
        """
        Save index metadata to file.
        
        Parameters
        ----------
        path : str or Path, optional
            File path for metadata. Defaults to {db_path}/metadata.bin
        """
        if path is None:
            path = Path(self.db_path) / "metadata.bin"
        self.engine.save_metadata(str(path))
    
    def load_metadata(self, path: Union[str, Path] = None) -> None:
        """
        Load index metadata from file.
        
        Parameters
        ----------
        path : str or Path, optional
            File path to metadata. Defaults to {db_path}/metadata.bin
        """
        if path is None:
            path = Path(self.db_path) / "metadata.bin"
        self.engine.load_metadata(str(path))
    
    @property
    def total_docs(self) -> int:
        """Get total number of indexed documents."""
        return self.engine.get_total_docs()
    
    @property
    def stats(self) -> str:
        """Get formatted index statistics."""
        return self.engine.get_stats()
    
    def __repr__(self) -> str:
        return f"<SearchEngine: {self.total_docs:,} documents indexed at {self.db_path}>"


# Export the Rust class too for backward compatibility
PySearchEngine = _PySearchEngine