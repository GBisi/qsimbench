import warnings
warnings.filterwarnings("ignore", message="pkg_resources is deprecated*")

import os
import time
import json
import threading
import tempfile
import shutil

from pathlib import Path
from sacred import Experiment
from sacred.observers import MongoObserver
from dotenv import load_dotenv
from pymongo import MongoClient
import psutil
from typing import Any
import logging


# ── Load environment ──────────────────────────────────────────────────
load_dotenv()

# MongoDB parameters
MONGO_USER = os.getenv('MONGO_INITDB_ROOT_USERNAME')
MONGO_PASSWORD = os.getenv('MONGO_INITDB_ROOT_PASSWORD')
MONGO_HOST = os.getenv('MONGO_HOST', 'localhost')
MONGO_PORT = os.getenv('MONGO_PORT', '27017')
MONGO_DB = os.getenv("MONGO_DATABASE", "sacred")
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    if not (MONGO_USER and MONGO_PASSWORD):
        raise ValueError("MONGO_INITDB_ROOT_USERNAME and MONGO_INITDB_ROOT_PASSWORD must be set if MONGO_URI is not provided.")
    MONGO_URI = f'mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/?authSource=admin'
print(f"Connecting to MongoDB at {MONGO_URI}")
client = MongoClient(MONGO_URI)

# ── Sacred Experiment ────────────────────────────────────────────────
ex = Experiment("run_experiments")
ex.observers.append(MongoObserver.create(url=MONGO_URI, db_name=MONGO_DB))

# ── Monitor Stats ──────────────────────────────────────────

def monitor_stats(interval=1.0):
    proc = psutil.Process(os.getpid())
    psutil.cpu_percent(interval=None)
    proc.cpu_percent(interval=None)
    time.sleep(interval)
    stats = {
        "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
        
        "cpu_global": {
            "percent_total": psutil.cpu_percent(interval=None),
            "percent_per_core": psutil.cpu_percent(interval=None, percpu=True),
            "times": [t._asdict() for t in psutil.cpu_times(percpu=True)],
            "count_logical": psutil.cpu_count(logical=True),
            "count_physical": psutil.cpu_count(logical=False),
            "frequency": psutil.cpu_freq()._asdict(),
            "stats": psutil.cpu_stats()._asdict(),
        },

        "ram_global": {
            "virtual": psutil.virtual_memory()._asdict(),
            "swap": psutil.swap_memory()._asdict(),
        },

        "disk_global": {
            "usage_root": psutil.disk_usage('/')._asdict(),
            "io": psutil.disk_io_counters(perdisk=False)._asdict(),
        },

        "process": {
            "pid": proc.pid,
            "name": proc.name(),
            "status": proc.status(),
            "cpu_percent": proc.cpu_percent(interval=None),
            "cpu_times": proc.cpu_times()._asdict(),
            "memory_info": proc.memory_info()._asdict(),
            "memory_percent": proc.memory_percent(),
            "num_threads": proc.num_threads(),
            "io_counters": proc.io_counters()._asdict() if proc.io_counters() else {},
            "open_files": [f.path for f in proc.open_files()],
        }
    }
    return stats

# ── Helpers ──────────────────────────────────────────
@ex.capture
def load_qasm_from_mongo(circuit_name: str, size: int, db_name="quantum_circuit", collection_name="mqt.bench==1.1.9", mongo_uri=None, _run=None):
    from qiskit import QuantumCircuit
    db_qc = client[db_name]
    col = db_qc.get_collection(collection_name)
    query = {"_id": f"{circuit_name}_{size}"}
    doc = col.find_one(query)
    if not doc:
        raise FileNotFoundError(f"No circuit found in Mongo for {query}")
    doc["created_at"] = doc.get("created_at").isoformat() if doc.get("created_at") else None
    _run.info["circuit"] = {"db": db_name, "collection": collection_name, "query": query, "doc": doc}
    qc = QuantumCircuit.from_qasm_str(doc["circuit"])
    mirror_qc = QuantumCircuit.from_qasm_str(doc["mirror"])
    return qc, mirror_qc

@ex.capture
def run_batch(qc: Any, mirror_qc: Any, shots: int, executor: Any, backends: dict, max_retries: int = 1, _log=None):
    for _ in range(max_retries):
        try:
            job_data = executor.run_experiment(qc.copy(), shots, backends, "multiplier", multiprocess=False, wait=True)
            job_mirror = executor.run_experiment(mirror_qc.copy(), shots, backends, "multiplier", multiprocess=False, wait=True)
            data = job_data.get_results()["local_aer"]
            mirror = job_mirror.get_results()["local_aer"]
            for b in backends["local_aer"]:
                err = data[b][0].get("error") or mirror[b][0].get("error")
                if err:
                    _log.error(f"{b}: {err}")
                    raise RuntimeError(err)
            return data, mirror
        except Exception as exc:
            if max_retries > 1:
                _log.warning(f"Retry after error: {exc}")
                time.sleep(1)
    raise RuntimeError("Max retries exhausted.")

# ── Configuration ──────────────────────────────────────────
@ex.config
def cfg():
    circuit = ""
    size = 0
    backend = ""
    shots = 20000
    batch_size = 50
    providers = ["local_aer"]
    n_cores = None

# ── Main Entrypoint ──────────────────────────────────────────
@ex.automain
def main(circuit, size, backend, shots, batch_size, providers, n_cores, _run, _log):
    if n_cores is not None:
        os.environ["OMP_NUM_THREADS"] = str(n_cores)
        os.environ["OPENBLAS_NUM_THREADS"] = str(n_cores)
        os.environ["MKL_NUM_THREADS"] = str(n_cores)
        os.environ["VECLIB_MAXIMUM_THREADS"] = str(n_cores)
        os.environ["NUMEXPR_NUM_THREADS"] = str(n_cores)
        os.environ["NUMBA_NUM_THREADS"] = str(n_cores)
        _log.info(f"Using up to {n_cores} CPU cores out of {psutil.cpu_count(logical=True)} available.")
    else:
        _log.info(f"Using all available CPU cores: {psutil.cpu_count(logical=True)}")
        
    from quantum_executor import QuantumExecutor
    from qiskit import QuantumCircuit
    
    logging.getLogger('qiskit').setLevel(logging.WARNING)
    logging.getLogger('quantum_executor').setLevel(logging.WARNING)
    logging.getLogger('qbraid').setLevel(logging.WARNING)
    
    qc, mirror_qc = load_qasm_from_mongo(circuit, size)
    executor = QuantumExecutor(providers=providers)
    backends = {"local_aer": [backend]}

    _backend = executor.virtual_provider.get_backend(provider_name=providers[0], backend_name=backend)._backend
    _run.info["backend"] = {
        "name": backend,
        "configuration": _backend.configuration().to_dict(),
        "properties": _backend.properties().to_dict() if hasattr(_backend, 'properties') and _backend.properties() else {}
    }


    temp_dir = tempfile.mkdtemp()
    history_circuit = Path(temp_dir) / f"history_circuit.jsonl"
    history_mirror = Path(temp_dir) / f"history_mirror.jsonl"
    stats_path = Path(temp_dir) / f"stats.jsonl"

    hc_f = open(history_circuit, 'a')
    hm_f = open(history_mirror, 'a')
    stats_f = open(stats_path, 'a')

    stop_event = threading.Event()
    def stats_loop():
        while not stop_event.is_set():
            stats = monitor_stats()
            stats_f.write(json.dumps(stats) + "\n")
            stats_f.flush()
            _run.log_scalar("cpu_global_total", stats["cpu_global"]["percent_total"])
            _run.log_scalar("ram_virtual_percent", stats["ram_global"]["virtual"]["percent"])
            _run.log_scalar("ram_swap_percent", stats["ram_global"]["swap"]["percent"])
            _run.log_scalar("disk_usage_root_percent", stats["disk_global"]["usage_root"]["percent"])
            _run.log_scalar("proc_cpu_percent", stats["process"]["cpu_percent"])
            _run.log_scalar("proc_mem_percent", stats["process"]["memory_percent"])
            time.sleep(60)
    thread = threading.Thread(target=stats_loop, daemon=True)
    thread.start()

    remaining = shots
    batch_index = 0
    while remaining > 0:
        batch = min(batch_size, remaining)
        data, mirror = run_batch(qc, mirror_qc, batch, executor, backends)
        remaining -= batch
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        batch_record = {"batch_index": batch_index, "shots": batch, "data": data[backend][0], "timestamp": timestamp}
        mirror_record = {"batch_index": batch_index, "shots": batch, "data": mirror[backend][0], "timestamp": timestamp}
        hc_f.write(json.dumps(batch_record) + "\n")
        hm_f.write(json.dumps(mirror_record) + "\n")
        hc_f.flush()
        hm_f.flush()
        batch_index += 1
        _log.info(f"Collected {shots - remaining}/{shots} shots")
        _run.log_scalar("shots_collected", shots - remaining)
        _log.info(batch_record)
        _log.info(mirror_record)

    stop_event.set()
    thread.join()
    hc_f.close()
    hm_f.close()
    stats_f.close()

    _run.add_artifact(str(history_circuit))
    _run.add_artifact(str(history_mirror))
    _run.add_artifact(str(stats_path))
    _log.info("Experiment completed successfully.")

    shutil.rmtree(temp_dir, ignore_errors=True)
