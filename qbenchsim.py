import os
import json
import random
import threading
from pathlib import Path
from typing import Dict, List, Optional, Union, Tuple, Any
import shutil
import tempfile
import requests
import zipfile
import tarfile
import logging
from functools import lru_cache
from requests.adapters import HTTPAdapter, Retry
import json

__all__ = ["get_outcomes", "download_dataset", "build_index", "get_backend", "get_circuit"]

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# Thread-safe cursor lock
t_lock = threading.RLock()
# Default data directory
data_dir = Path(os.getenv("DATASETS_PATH", "datasets"))
# Default dataset name
default_dataset = Path(os.getenv("DATASET_NAME", "dataset"))
# Cursor store: (algorithm, size, backend, mirror) -> next index
_cursors: Dict[Tuple[str, int, str, bool], int] = {}

# Retry strategy for HTTP
_retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
)
_adapter = HTTPAdapter(max_retries=_retry_strategy)


@lru_cache()
def build_index(
    dataset: Union[str, Path] = default_dataset,
) -> List[Tuple[str, int, str]]:
    """
    Build or retrieve a cached index of history files for a given dataset.

    Scans `<data_dir>/<dataset>/histories/{circuit,mirror}` for files named
    `<algorithm>_<size>_<backend>.jsonl` and maps them accordingly.
    """
    base = data_dir / dataset / "histories"
    if not base.exists():
        raise FileNotFoundError(f"No histories directory under '{base}'")

    index: List[Tuple[str, int, str]] = [] 

    for kind in ("circuit", "mirror"):
        dir_path = base / kind
        if not dir_path.exists():
            logger.warning(f"Missing '{kind}' folder under {base}")
            continue

        for fn in dir_path.glob("*.jsonl"):
            stem = fn.stem
            # split from right to allow underscores in algo names
            parts = stem.split("_", 2)
            if len(parts) != 3:
                logger.debug(f"Skipping file with unexpected name: {fn.name}")
                continue
            algo, size_str, backend = parts
            try:
                size = int(size_str)
            except ValueError:
                logger.debug(f"Skipping file with non-integer size: {fn.name}")
                continue

            index.append((algo, size, backend))

    # Deduplicate and sort
    index = sorted(set(index), key=lambda x: (x[0], x[1], x[2]))
        

    logger.info(f"Built index for dataset '{dataset}': {len(index)} entries")
    return index


def download_dataset(
    url: str,
    name: Optional[str] = None,
    force: bool = False,
) -> List[Tuple[str, int, str]]:
    """
    Download and extract a dataset archive from a URL into `data_dir`, then build its index.

    Supports .zip, .tar.gz, .tar.bz2, .tar.xz, .tgz, .tar. Cleans up __MACOSX dirs.
    """
    data_dir.mkdir(parents=True, exist_ok=True)

    # Determine filename and dataset name
    filename = url.split("/")[-1].split("?")[0]
    archive_path = data_dir / filename

    # derive stem without known extensions
    stem = archive_path.name
    for ext in (".tar.gz", ".tgz", ".tar.bz2", ".tar.xz", ".zip", ".tar"):
        if stem.lower().endswith(ext):
            stem = stem[: -len(ext)]
            break
    dataset_name = name or stem
    final_path = data_dir / dataset_name

    if final_path.exists():
        if force:
            shutil.rmtree(final_path)
        else:
            logger.info(f"Dataset '{dataset_name}' already present; skipping download.")
            return build_index(dataset_name)

    # Download with retries and timeouts
    session = requests.Session()
    session.mount("https://", _adapter)
    session.mount("http://", _adapter)
    logger.info(f"Downloading dataset from {url}...")
    response = session.get(url, stream=True, timeout=(5, 30))
    response.raise_for_status()

    with open(archive_path, "wb") as f:
        for chunk in response.iter_content(8192):
            f.write(chunk)

    # Extract into temp then flatten
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        if zipfile.is_zipfile(archive_path):
            with zipfile.ZipFile(archive_path) as z:
                z.extractall(tmp)
        elif tarfile.is_tarfile(archive_path):
            with tarfile.open(archive_path, "r:*") as t:
                t.extractall(tmp)
        else:
            raise ValueError(f"Unsupported archive format: {archive_path}")

        # recursively flatten single-root dirs
        contents = [p for p in tmp.iterdir() if p.name != "__MACOSX"]
        root = tmp
        while len(contents) == 1 and contents[0].is_dir():
            root = contents[0]
            contents = [p for p in root.iterdir() if p.name != "__MACOSX"]

        final_path.mkdir(parents=True, exist_ok=True)
        for item in contents:
            target = final_path / item.name
            shutil.move(str(item), str(target))

    # clean macOS artifacts & archive
    macosx = final_path / "__MACOSX"
    if macosx.exists(): shutil.rmtree(macosx, ignore_errors=True)
    archive_path.unlink(missing_ok=True)

    logger.info(f"Dataset extracted to {final_path}")
    return build_index(dataset_name)


def _multinomial_sample(
    agg: Dict[str, int],
    shots: int,
    seed: int
) -> Dict[str, int]:
    total_counts = sum(agg.values())
    if total_counts <= 0:
        raise RuntimeError("No counts available for sampling.")
    rng = random.Random(seed)
    bits = list(agg.keys())
    probs = [agg[b] / total_counts for b in bits]
    sampled = rng.choices(bits, weights=probs, k=shots)
    out: Dict[str, int] = {}
    for b in sampled:
        out[b] = out.get(b, 0) + 1
    return out


def get_outcomes(
    algorithm: str,
    size: int,
    backend: str,
    shots: int = 1024,
    mirror: bool = False,
    *,
    exact: bool = False,
    sequential: bool = True,
    seed: Optional[int] = None,
    dataset: Union[str, Path] = default_dataset,
) -> Dict[str, int]:
    """
    Retrieve and aggregate outcome counts from historical JSONL records.

    Streams records to avoid high memory; supports sequential cursor and random sampling;
    exact=True enables overshoot + multinomial sampling for exact shot count.
    """
    if shots <= 0:
        raise ValueError("Parameter 'shots' must be > 0.")

    ds_dir = data_dir / dataset
    if not ds_dir.exists():
        raise FileNotFoundError(f"Dataset '{ds_dir}' not found.")

    kind = "mirror" if mirror else "circuit"
    hist_file = ds_dir / "histories" / kind / f"{algorithm}_{size}_{backend}.jsonl"
    if not hist_file.exists():
        raise FileNotFoundError(f"No history file for {algorithm}/{size}/{backend} ({kind})")

    # Initialize random seeds
    rng = random.Random(seed)
    sampling_seed = rng.randint(0, 2**32 - 1)
    exact_seed = rng.randint(0, 2**32 - 1)

    key = (algorithm, size, backend, mirror)
    n_records = 0

    # Two-phase streaming: first pass to count lines if sequential cursor wrap needed
    with hist_file.open("r", encoding="utf-8") as fh:
        for _ in fh:
            n_records += 1
    if n_records == 0:
        raise RuntimeError(f"History file is empty: {hist_file}")

    def stream_records(start_idx: int = 0):
        """Yield (idx, record) cyclically starting from start_idx."""
        idx = 0
        with hist_file.open("r", encoding="utf-8") as fh:
            for raw in fh:
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    rec = json.loads(raw)
                except json.JSONDecodeError:
                    logger.warning(f"Skipping malformed JSON line at index {idx}")
                    idx += 1
                    continue
                yield idx, rec
                idx += 1

    agg: Dict[str, int] = {}
    total = 0

    if sequential:
        with t_lock:
            start_idx = _cursors.get(key, 0)
        consumed = 0
        for idx, rec in stream_records():
            real_idx = (start_idx + idx) % n_records
            if idx < start_idx:
                continue  # skip until we wrap
            rec_shots = int(rec.get("shots", 0))
            if rec_shots <= 0:
                consumed += 1
                continue
            if not exact and total + rec_shots > shots:
                break
            for bit, cnt in rec.get("data", {}).items():
                agg[bit] = agg.get(bit, 0) + int(cnt)
            total += rec_shots
            consumed += 1
            if total >= shots:
                break

        # On exact overshoot
        if exact and total < shots:
            for idx2, rec2 in stream_records((start_idx + consumed) % n_records):
                rec2_shots = int(rec2.get("shots", 0))
                if rec2_shots <= 0:
                    consumed += 1
                    continue
                for bit, cnt in rec2.get("data", {}).items():
                    agg[bit] = agg.get(bit, 0) + int(cnt)
                total += rec2_shots
                consumed += 1
                if total >= shots:
                    break
            agg = _multinomial_sample(agg, shots, exact_seed)

        new_cursor = (start_idx + consumed) % n_records
        with t_lock:
            _cursors[key] = new_cursor

    else:
        # Random sampling path
        rng = random.Random(sampling_seed)
        while total < shots:
            rec = rng.choice(list(stream_records()))[1]
            rec_shots = int(rec.get("shots", 0))
            if rec_shots <= 0:
                continue
            if not exact and total + rec_shots > shots:
                break
            for bit, cnt in rec.get("data", {}).items():
                agg[bit] = agg.get(bit, 0) + int(cnt)
            total += rec_shots

        if exact and total < shots:
            rng2 = random.Random(exact_seed)
            while total < shots:
                rec = rng2.choice(list(stream_records()))[1]
                for bit, cnt in rec.get("data", {}).items():
                    agg[bit] = agg.get(bit, 0) + int(cnt)
                total += int(rec.get("shots", 0))
            agg = _multinomial_sample(agg, shots, exact_seed)

    return agg

def get_backend(
    algorithm: str,
    size: int,
    backend: str,
    *,
    dataset: Union[str, Path] = default_dataset
) -> Any:
    """
    Retrieve the backend's noise model from the first matching summary.

    Looks for a file named '{algorithm}_{size}_{backend}_*.json' under
    data_dir/<dataset>/, loads its 'metadata.backend', and returns the
    'noise_model' field.

    Raises FileNotFoundError if no summary is found, or KeyError if
    the noise_model key is missing.
    """
    ds_dir = data_dir / dataset
    # glob and sort so the "first" is deterministic
    pattern = f"{algorithm}_{size}_{backend}_*.json"
    matches = sorted(ds_dir.glob(pattern))
    if not matches:
        raise FileNotFoundError(f"No summary files matching '{pattern}' in '{ds_dir}'")
    summary_path = matches[0]
    with open(summary_path, "r", encoding="utf-8") as f:
        summary = json.load(f)
    backend_md = summary.get("metadata", {}).get("backend", {})
    return backend_md


def get_circuit(
    algorithm: str,
    size: int,
    backend: str,
    mirror: bool = False,
    *,
    dataset: Union[str, Path] = default_dataset
) -> str:
    """
    Retrieve the QASM circuit string for the given triple.

    Finds the first '{algorithm}_{size}_{backend}_*.json' summary,
    then tries these in order:
      1. metadata.circuit.doc['qasm']
      2. Any artifact file ending in '.qasm' whose path contains
         'mirror' if mirror=True (otherwise 'circuit').
    Raises FileNotFoundError if no QASM can be located.
    """
    ds_dir = data_dir / dataset
    pattern = f"{algorithm}_{size}_{backend}_*.json"
    matches = sorted(ds_dir.glob(pattern))
    if not matches:
        raise FileNotFoundError(f"No summary files matching '{pattern}' in '{ds_dir}'")
    summary_path = matches[0]
    with open(summary_path, "r", encoding="utf-8") as f:
        summary = json.load(f)

    circuit_md = summary.get("metadata", {}).get("circuit", {})
    qasm = circuit_md.get("circuit") if not mirror else circuit_md.get("mirror")
    if qasm:
        return qasm

    raise FileNotFoundError(
        f"Could not find a QASM circuit for {algorithm}/{size}/{backend}"
    )

if __name__ == "__main__":
    import time

    print("Index:", download_dataset(
        "https://github.com/GBisi/qbenchsim-dataset/raw/refs/heads/main/dataset.zip?download=true"
    ))
    start = time.time()
    for _ in range(10):
        res = get_outcomes("dj", 12, "fake_sherbrooke", shots=50_000, exact=True, sequential=True, seed=None)
        print("Outcomes:", res)
    print("Elapsed:", time.time() - start)
