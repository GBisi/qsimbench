"""
QSimBench
==================================
Functional client library for retrieving and sampling from the QSimBench dataset.
"""

import os
import logging
import threading
import json
import random
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter, Retry

# ---------------------------------------------------------------------------
# Custom exception for QSimBench errors
# ---------------------------------------------------------------------------
class QSimBenchError(Exception):
    """Base exception for QSimBench errors."""
    pass

# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# ---------------------------------------------------------------------------
# HTTP retry strategy
# ---------------------------------------------------------------------------
_RETRY_STRATEGY = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods={"GET"},
)
_HTTP_ADAPTER = HTTPAdapter(max_retries=_RETRY_STRATEGY)

# ---------------------------------------------------------------------------
# Defaults & globals
# ---------------------------------------------------------------------------
DEFAULT_DATASET_URL = os.getenv(
    "QSIMBENCH_DATASET",
    "https://github.com/GBisi/qsimbench-dataset/raw/refs/heads/main/dataset"
).rstrip("/")
DEFAULT_CACHE_DIR = Path(os.getenv(
    "QSIMBENCH_CACHE_DIR",
    Path(__file__).parent / ".qsimbench_cache"
))
DEFAULT_CACHE_TIMEOUT = int(os.getenv("QSIMBENCH_CACHE_TIMEOUT", 30 * 24 * 60 * 60))

# Ensure cache directory exists
DEFAULT_CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Mutable configuration
DATASET_URL: str = DEFAULT_DATASET_URL
CACHE_DIR: Path = DEFAULT_CACHE_DIR
CACHE_TIMEOUT: int = DEFAULT_CACHE_TIMEOUT

# Thread-safe cursor storage for sequential sampling
_CURSORS: Dict[Tuple[str, int, str, str], int] = {}
_CURSORS_LOCK = threading.RLock()

# Shared HTTP session
_SESSION = requests.Session()
_SESSION.mount("https://", _HTTP_ADAPTER)
_SESSION.mount("http://", _HTTP_ADAPTER)


# ---------------------------------------------------------------------------
# Configuration functions
# ---------------------------------------------------------------------------
def set_dataset_url(url: str) -> None:
    """
    Override the base dataset URL.

    Args:
        url: Must start with 'http://' or 'https://'.

    Raises:
        QSimBenchError: If URL is invalid.
    """
    global DATASET_URL
    if not url.startswith(("http://", "https://")):
        raise QSimBenchError("Dataset URL must start with 'http://' or 'https://'.")
    DATASET_URL = url.rstrip("/")
    logger.debug(f"Dataset URL set to: {DATASET_URL}")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------
def _multinomial_sample(
    agg: Dict[str, int],
    shots: int,
    seed: int
) -> Dict[str, int]:
    """
    Down-sample an aggregated distribution to exactly `shots` via multinomial sampling.

    Args:
        agg: Mapping from outcome bitstring to count.
        shots: Total draws desired.
        seed: Random seed.

    Returns:
        A new mapping with total counts == shots.

    Raises:
        QSimBenchError: If no counts are available.
    """
    total = sum(agg.values())
    if total <= 0:
        raise QSimBenchError("No counts available for multinomial sampling.")
    rng = random.Random(seed)
    bits = list(agg.keys())
    weights = [agg[b] / total for b in bits]
    sampled = rng.choices(bits, weights=weights, k=shots)
    result: Dict[str, int] = {}
    for b in sampled:
        result[b] = result.get(b, 0) + 1
    return result


def _download_and_cache(
    url: str,
    cache_path: Path,
    force: bool = False
) -> List[Dict[str, Any]]:
    """
    Download a JSONL file from `url` and cache it at `cache_path`.

    Args:
        url: Full URL to JSONL.
        cache_path: Local Path to cache.
        force: If True, ignore existing cache.

    Returns:
        List of JSON objects (one per line).

    Raises:
        QSimBenchError: On HTTP errors or empty result.
    """
    # Serve from cache if fresh
    if cache_path.exists() and not force:
        mtime = cache_path.stat().st_mtime
        if (mtime + CACHE_TIMEOUT) > mtime:
            logger.debug(f"Loading data from cache: {cache_path}")
            return [
                json.loads(line)
                for line in cache_path.read_text().splitlines()
                if line
            ]
        cache_path.unlink()

    logger.debug(f"Fetching data from URL: {url}")
    resp = _SESSION.get(url)
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        raise QSimBenchError(f"HTTP error fetching {url}: {e}") from e

    records = [
        json.loads(line) for line in resp.text.splitlines() if line.strip()
    ]
    if not records:
        raise QSimBenchError(f"No records found at {url}")

    # Cache to disk
    cache_path.write_text("\n".join(json.dumps(r) for r in records))
    logger.debug(f"Cached {len(records)} records to {cache_path}")
    return records


def _get_data(
    algorithm: str,
    size: int,
    backend: str,
    circuit_kind: str = "circuit",
    force: bool = False
) -> List[Dict[str, Any]]:
    """
    Retrieve raw history records from the dataset.

    Args:
        algorithm: Algorithm name (non-empty).
        size: Positive integer problem size.
        backend: Backend identifier (non-empty).
        circuit_kind: Either "circuit" or "mirror".
        force: If True, bypass cache.

    Returns:
        List of record dicts.

    Raises:
        QSimBenchError: On invalid parameters or no records.
    """
    # Validate parameters
    if not algorithm or size <= 0 or not backend:
        raise QSimBenchError("algorithm, size, and backend must be valid.")
    kind = circuit_kind.lower()
    if kind not in {"circuit", "mirror"}:
        raise QSimBenchError("circuit_kind must be 'circuit' or 'mirror'.")

    # Build URL and cache path
    alg = algorithm.lower()
    be = backend.lower()
    file_name = f"{alg}_{size}_{be}.jsonl"
    url = f"{DATASET_URL}/histories/{kind}/{file_name}"
    cache_path = CACHE_DIR / kind / file_name
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    # Download & parse
    return _download_and_cache(url, cache_path, force=force)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def get_outcomes(
    algorithm: str,
    size: int,
    backend: str,
    shots: int = 1024,
    circuit_kind: str = "circuit",
    *,
    exact: bool = True,
    strategy: str = "sequential",
    seed: Optional[int] = None,
    force: bool = False
) -> Dict[str, int]:
    """
    Sample outcome counts for a given algorithm/size/backend.

    Args:
        algorithm: Algorithm name.
        size: Problem size > 0.
        backend: Backend name.
        shots: Total shots to sample (>0).
        circuit_kind: "circuit" or "mirror".
        exact: If True, enforce exactly `shots` via multinomial.
        strategy: "sequential" or "random".
        seed: Optional int seed for reproducibility.
        force: If True, refetch raw data ignoring cache.

    Returns:
        Mapping from outcome bitstring to count.

    Raises:
        QSimBenchError: On invalid args or sampling failures.
    """
    if shots <= 0:
        raise QSimBenchError("Parameter 'shots' must be > 0.")
    if strategy not in {"sequential", "random"}:
        raise QSimBenchError("strategy must be 'sequential' or 'random'.")

    data = _get_data(algorithm, size, backend, circuit_kind, force)
    n = len(data)
    if n == 0:
        raise QSimBenchError("No records available to sample.")

    # Initialize RNGs
    master_rng = random.Random(seed)
    sample_seed = master_rng.randint(0, 2**32 - 1)
    exact_seed = master_rng.randint(0, 2**32 - 1)

    agg: Dict[str, int] = {}
    total = 0

    if strategy == "sequential":
        key = (algorithm, size, backend, circuit_kind)
        with _CURSORS_LOCK:
            start_idx = _CURSORS.get(key, 0)

        idx = start_idx
        consumed = 0
        while total < shots:
            if idx >= n:
                idx = 0
            rec = data[idx]
            s = int(rec.get("shots", 0))
            if s > 0:
                for bit, cnt in rec.get("data", {}).items():
                    agg[bit] = agg.get(bit, 0) + int(cnt)
                total += s
            idx += 1
            consumed += 1

        with _CURSORS_LOCK:
            _CURSORS[key] = idx % n

    else:  # random
        rng = random.Random(sample_seed)
        while total < shots:
            rec = rng.choice(data)
            s = int(rec.get("shots", 0))
            if s <= 0:
                continue
            for bit, cnt in rec.get("data", {}).items():
                agg[bit] = agg.get(bit, 0) + int(cnt)
            total += s

    # Exact down-sampling
    if exact and total > shots:
        agg = _multinomial_sample(agg, shots, exact_seed)

    return agg


@lru_cache()
def get_index(
    circuit_kind: str = "circuit",
    by_backend: bool = False
) -> Dict[str, Any]:
    """
    List available algorithms, sizes, and backends in the dataset (via GitHub API).

    Args:
        circuit_kind: "circuit" or "mirror".
        by_backend: If True, invert mapping to backend→algorithm→sizes.

    Returns:
        Nested dict of available items.

    Raises:
        QSimBenchError: On URL parsing or HTTP errors.
    """
    kind = circuit_kind.lower()
    if kind not in {"circuit", "mirror"}:
        raise QSimBenchError("circuit_kind must be 'circuit' or 'mirror'.")

    # Convert raw URL → GitHub tree URL
    tree_url = DATASET_URL.replace("raw/refs/heads", "tree")
    parsed = urlparse(tree_url + f"/histories/{kind}")
    parts = parsed.path.strip("/").split("/")
    if len(parts) < 5 or parts[2] != "tree":
        raise QSimBenchError(f"Unexpected URL pattern: {tree_url}")

    owner, repo, _, branch, *path_parts = parts
    api_url = f"https://api.github.com/repos/{owner}/{repo}/contents/{'/'.join(path_parts)}"
    headers = {"Accept": "application/vnd.github+json"}

    resp = _SESSION.get(api_url, headers=headers)
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        raise QSimBenchError(f"GitHub API error: {e}") from e

    items = resp.json()
    result: Dict[str, Any] = {}

    for item in items:
        name = item.get("name", "")
        alg, rest = name.split("_", 1)
        size_str, backend_ext = rest.split("_", 1)
        size = int(size_str)
        backend = backend_ext.rsplit(".", 1)[0]

        if not by_backend:
            result.setdefault(alg, {}).setdefault(size, []).append(backend)
        else:
            result.setdefault(backend, {}).setdefault(alg, []).append(size)

    return result


@lru_cache()
def get_metadata(
    algorithm: str,
    size: int,
    backend: str
) -> List[Any]:
    """
    Fetch metadata JSON files for a given algorithm/size/backend.

    Args:
        algorithm: Algorithm name.
        size: Problem size.
        backend: Backend name.

    Returns:
        List of parsed JSON metadata objects.

    Raises:
        QSimBenchError: On lookup or HTTP errors.
    """
    tree_url = DATASET_URL.replace("raw/refs/heads", "tree")
    parsed = urlparse(tree_url)
    parts = parsed.path.strip("/").split("/")
    if len(parts) < 5 or parts[2] != "tree":
        raise QSimBenchError(f"Unexpected URL pattern: {tree_url}")

    owner, repo, _, branch, *path_parts = parts
    contents_url = (
        f"https://api.github.com/repos/{owner}/{repo}/contents/"
        f"{'/'.join(path_parts)}"
    )
    headers = {"Accept": "application/vnd.github+json"}

    resp = _SESSION.get(contents_url, headers=headers)
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        raise QSimBenchError(f"GitHub API error: {e}") from e

    files = [
        item["name"]
        for item in resp.json()
        if item.get("name", "").startswith(f"{algorithm}_{size}_{backend}")
    ]
    if not files:
        raise QSimBenchError(
            f"No metadata files for {algorithm}_{size}_{backend}"
        )

    metadata: List[Any] = []
    for fname in files:
        raw_url = f"{DATASET_URL}/{fname}"
        r = _SESSION.get(raw_url)
        try:
            r.raise_for_status()
        except requests.HTTPError as e:
            raise QSimBenchError(f"Error fetching {raw_url}: {e}") from e
        try:
            metadata.append(r.json())  # try full parse
        except json.JSONDecodeError:
            for line in r.text.strip().splitlines():
                metadata.append(json.loads(line))

    return metadata