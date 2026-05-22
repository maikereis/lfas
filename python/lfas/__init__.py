"""
LFAS — Lightning-Fast Address Search
=====================================
High-performance BM25F search engine for Brazilian address data, backed by a
Rust/LMDB inverted index.

Quickstart
----------
>>> from lfas import SearchEngine
>>> engine = SearchEngine()                      # default ./lmdb_data
>>> engine = SearchEngine("./my_index")          # custom path
>>> engine.index({"rua": "Avenida Paulista", "numero": "1578"}, doc_id=0)
>>> engine.flush()
>>> results = engine.search({"rua": "Paulista"}, top_k=10)
>>> for doc_id, score in results:
...     print(doc_id, score)

Concurrency
-----------
Searches are fully concurrent: multiple threads can call `search()` at the same
time and will run in parallel — the underlying Rust engine holds only a *read*
lock during searches.  Writes (index / flush / load_metadata) are serialised by
a write lock and must not be interleaved with reads from other threads without
external coordination.

Context manager
---------------
>>> with SearchEngine("./my_index") as engine:
...     engine.index_batch(records)
...     engine.flush()
...     results = engine.search(query)
"""

from __future__ import annotations

try:
    from lfas._core import PySearchEngine as _PySearchEngine
except ImportError:
    _PySearchEngine = None  # type: ignore[assignment]

from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple, Union

__version__ = "0.1.0"
__all__ = ["SearchEngine", "PySearchEngine"]

# Convenient type aliases used throughout the public API.
Document = Dict[str, str]
"""A mapping of field names to their string values."""

SearchResult = Tuple[int, float]
"""A ``(doc_id, score)`` pair returned by :py:meth:`SearchEngine.search`."""

Batch = Sequence[Tuple[int, Document]]
"""A sequence of ``(doc_id, document)`` pairs for bulk indexing."""

# Valid field names recognised by the Rust engine.
VALID_FIELDS = frozenset(
    {
        "estado",
        "municipio",
        "bairro",
        "cep",
        "tipo_logradouro",
        "rua",
        "numero",
        "complemento",
        "nome",
    }
)


def _require_engine() -> None:
    if _PySearchEngine is None:
        raise ImportError(
            "LFAS Rust extension not found.  "
            "Run `maturin develop` (development) or install the wheel first."
        )


def _validated_db_path(db_path: Union[str, Path]) -> Path:
    if not isinstance(db_path, (str, Path)):
        raise TypeError(
            f"'db_path' must be str or pathlib.Path; got {type(db_path).__name__!r}"
        )
    path = Path(db_path).expanduser()
    if path.exists() and not path.is_dir():
        raise NotADirectoryError(f"db_path exists but is not a directory: {path}")
    path.mkdir(parents=True, exist_ok=True)
    try:
        probe = path / ".write_test"
        probe.touch()
        probe.unlink()
    except OSError as exc:
        raise PermissionError(f"db_path is not writable: {path}") from exc
    return path


class SearchEngine:
    """
    High-performance BM25F search engine for Brazilian address data.

    Wraps :class:`PySearchEngine` (Rust) with a Pythonic API, input
    validation, and context-manager support.

    Parameters
    ----------
    db_path : str | Path
        Path to the LMDB database directory.  Created if absent.

    Examples
    --------
    >>> engine = SearchEngine("./lmdb_data")
    >>> engine.index({"rua": "Rua das Flores", "numero": "42"}, doc_id=0)
    >>> engine.flush()
    >>> engine.search({"rua": "flores"}, top_k=5)
    [(0, 3.14...)]
    """

    def __init__(self, db_path: Union[str, Path] = "./lmdb_data") -> None:
        _require_engine()
        path = _validated_db_path(db_path)
        self._db_path = str(path)
        self._engine: _PySearchEngine = _PySearchEngine(db_path=self._db_path)

    def __enter__(self) -> "SearchEngine":
        return self

    def __exit__(self, *_: object) -> None:
        """Flush on exit so a ``with`` block guarantees durability."""
        try:
            self.flush()
        except Exception:
            pass  # Never suppress the original exception.

    @staticmethod
    def init_logging() -> None:
        """
        Bridge Rust ``log`` output into Python's :mod:`logging` system.

        Call once at application startup, before any engine is created.
        """
        if _PySearchEngine is not None:
            _PySearchEngine.init_logging()

    def index(self, document: Document, doc_id: int) -> None:
        """
        Index a single document.

        For bulk ingestion use :meth:`index_batch` instead — it is
        10–20× faster because it amortises LMDB transaction overhead.

        Parameters
        ----------
        document : dict[str, str]
            Field-value pairs.  Unknown fields are silently ignored.
        doc_id : int
            Non-negative unique document identifier.

        Raises
        ------
        TypeError
            If *document* is not a :class:`dict` or *doc_id* is not an int.
        ValueError
            If *doc_id* is negative.
        """
        if not isinstance(document, dict):
            raise TypeError(
                f"'document' must be a dict; got {type(document).__name__!r}"
            )
        if not isinstance(doc_id, int):
            raise TypeError(f"'doc_id' must be an int; got {type(doc_id).__name__!r}")
        if doc_id < 0:
            raise ValueError(f"'doc_id' must be >= 0; got {doc_id}")
        self._engine.index_dict(doc_id, document)

    def index_batch(self, documents: Batch) -> None:
        """
        Index multiple documents in a single batch operation.

        Parameters
        ----------
        documents : sequence of (doc_id, document) tuples
            Each element is ``(int, dict[str, str])``.

        Raises
        ------
        TypeError
            If any element has the wrong structure.
        ValueError
            If any *doc_id* is negative.

        Notes
        -----
        Call :meth:`flush` afterwards to guarantee persistence.
        """
        validated: List[Tuple[int, Document]] = []
        for item in documents:
            try:
                doc_id, doc = item
            except (TypeError, ValueError):
                raise TypeError(
                    "Each element of 'documents' must be a (doc_id, dict) tuple."
                )
            if not isinstance(doc_id, int):
                raise TypeError(f"doc_id must be an int; got {type(doc_id).__name__!r}")
            if doc_id < 0:
                raise ValueError(f"doc_id must be >= 0; got {doc_id}")
            if not isinstance(doc, dict):
                raise TypeError(f"document must be a dict; got {type(doc).__name__!r}")
            validated.append((doc_id, doc))
        self._engine.index_batch(validated)

    def flush(self) -> None:
        """
        Commit all pending writes to LMDB.

        Call after every batch of indexing operations to guarantee that
        data survives a process restart.
        """
        self._engine.flush()

    def search(
        self,
        query: Document,
        top_k: int = 10,
        blocking_k: int = 1000,
    ) -> List[SearchResult]:
        """
        Execute a field-aware BM25F search.

        This method is **thread-safe**: the Rust layer holds only a read
        lock on the global engine, so multiple threads may search in
        parallel without serialisation.

        Parameters
        ----------
        query : dict[str, str]
            Field-value pairs to search for.  Empty values are skipped.
        top_k : int
            Maximum number of results to return.
        blocking_k : int
            Candidate set size limit.  Higher values increase recall at
            the cost of latency (1 000 → fast, 100 000 → high recall).

        Returns
        -------
        list of (doc_id, score)
            Sorted by score descending.

        Raises
        ------
        TypeError
            If *query* is not a dict, or *top_k* / *blocking_k* are not int.
        ValueError
            If *top_k* or *blocking_k* are not positive.
        """
        if not isinstance(query, dict):
            raise TypeError(f"'query' must be a dict; got {type(query).__name__!r}")
        if not isinstance(top_k, int) or top_k <= 0:
            raise ValueError(f"'top_k' must be a positive int; got {top_k!r}")
        if not isinstance(blocking_k, int) or blocking_k <= 0:
            raise ValueError(f"'blocking_k' must be a positive int; got {blocking_k!r}")
        return self._engine.search_complex(query, top_k, blocking_k)

    def set_weights(self, weights: Dict[str, float]) -> "SearchEngine":
        """
        Set custom field importance weights for BM25F scoring.

        Weights are stored on *this* instance only; the global engine
        is never mutated, so different :class:`SearchEngine` objects can
        use different weights concurrently.

        Parameters
        ----------
        weights : dict[str, float]
            Mapping of field name → weight.  Unknown fields are ignored.

        Returns
        -------
        SearchEngine
            *self*, for method chaining.

        Examples
        --------
        >>> engine.set_weights({"cep": 15.0, "numero": 12.0, "rua": 5.0})
        """
        self._engine.set_field_weights(weights)
        return self

    def set_b_values(self, b_values: Dict[str, float]) -> "SearchEngine":
        """
        Set BM25F length normalisation parameters.

        Parameters
        ----------
        b_values : dict[str, float]
            Field name → b value in [0.0, 1.0].  0 = no normalisation.

        Returns
        -------
        SearchEngine
            *self*, for method chaining.
        """
        self._engine.set_field_b_values(b_values)
        return self

    def reset_weights(self) -> "SearchEngine":
        """
        Reset weights and b-values to engine defaults.

        Returns
        -------
        SearchEngine
            *self*, for method chaining.
        """
        self._engine.reset_weights()
        return self

    def get_weights(self) -> Dict[str, float]:
        """
        Return the effective field weights for this instance.

        Returns custom weights when set, otherwise the engine defaults.
        """
        return self._engine.get_weights()

    def save_metadata(self, path: Optional[Union[str, Path]] = None) -> None:
        """
        Persist index metadata (doc lengths, term DFs, etc.) to disk.

        Must be called after indexing if you want to search after a
        process restart without re-indexing.

        Parameters
        ----------
        path : str | Path, optional
            Destination file.  Defaults to ``{db_path}/metadata.bin``.
        """
        resolved = (
            Path(path) if path is not None else Path(self._db_path) / "metadata.bin"
        )
        self._engine.save_metadata(str(resolved))

    def load_metadata(self, path: Optional[Union[str, Path]] = None) -> None:
        """
        Load previously saved index metadata.

        Must be called before :meth:`search` when using a pre-built
        index that was loaded into a freshly created engine.

        Parameters
        ----------
        path : str | Path, optional
            Source file.  Defaults to ``{db_path}/metadata.bin``.

        Raises
        ------
        FileNotFoundError
            If *path* does not exist.
        """
        resolved = (
            Path(path) if path is not None else Path(self._db_path) / "metadata.bin"
        )
        if not resolved.exists():
            raise FileNotFoundError(
                f"Metadata file not found: {resolved}.  "
                "Did you call save_metadata() after indexing?"
            )
        self._engine.load_metadata(str(resolved))

    @property
    def db_path(self) -> str:
        """Absolute path to the LMDB database directory."""
        return self._db_path

    @property
    def total_docs(self) -> int:
        """Total number of indexed documents."""
        return self._engine.get_total_docs()

    @property
    def stats(self) -> str:
        """Human-readable index statistics string."""
        return self._engine.get_stats()

    def __repr__(self) -> str:
        return f"SearchEngine(" f"docs={self.total_docs:,}, " f"path={self._db_path!r})"

    def __len__(self) -> int:
        """Return the number of indexed documents."""
        return self.total_docs


# Re-export the raw Rust class for backward compatibility and advanced use.
PySearchEngine = _PySearchEngine
