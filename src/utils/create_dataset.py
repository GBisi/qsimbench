import os
import json
import gridfs
from pathlib import Path
from pymongo import MongoClient
from qiskit import QuantumCircuit  # noqa: F401 (imported for potential future use)
from dotenv import load_dotenv

# ── Load environment variables ───────────────────────────────────────
dotenv_path = Path(__file__).parent.parent.parent / "src" / ".env"
print("Loading environment variables from:", dotenv_path)
load_dotenv(dotenv_path=dotenv_path)

# ── MongoDB configuration ───────────────────────────────────────────
MONGO_USER = os.getenv('MONGO_INITDB_ROOT_USERNAME')
MONGO_PASSWORD = os.getenv('MONGO_INITDB_ROOT_PASSWORD')
MONGO_HOST = os.getenv('MONGO_HOST', 'localhost')
MONGO_PORT = os.getenv('MONGO_PORT', '27017')
MONGO_DB = os.getenv("MONGO_DATABASE", "sacred")
MONGO_URI = os.getenv("MONGO_URI") or \
             f'mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/?authSource=admin'

print(f"Connecting to MongoDB at {MONGO_URI}")

OUTPUT_DIR = "dataset"

# ── Connect to MongoDB and GridFS ───────────────────────────────────
client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
runs = db.runs
fs = gridfs.GridFS(db)

# ── Utility function to download artifact from GridFS ───────────────
def _download_artifact(artifact, target_dir):
    """
    Download an artifact from GridFS using its file_id, save it in target_dir,
    and return the relative path.
    """
    file_id = artifact["file_id"]
    filename = artifact["name"]

    target_dir = Path(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    try:
        gfo = fs.get(file_id)
    except gridfs.errors.NoFile:
        raise FileNotFoundError(f"Artifact {filename} (id={file_id}) not found in GridFS")

    out_path = target_dir / filename
    with open(out_path, "wb") as f:
        f.write(gfo.read())

    return str(out_path.relative_to(Path(OUTPUT_DIR)))

# ── Create summary for a single run ─────────────────────────────────
def create_summary_from_run(run_doc, output_dir):
    run_id = run_doc["_id"]
    info = run_doc.get("info", {})
    circuit_md = info["circuit"]["doc"]
    backend_md = info["backend"]

    algorithm = circuit_md.get("algorithm")
    size = circuit_md.get("size")
    backend = backend_md.get("name")

    if not algorithm or not size:
        raise ValueError(f"Run {run_id} does not contain valid circuit metadata: {circuit_md}")

    if not backend:
        raise ValueError(f"Run {run_id} does not contain valid backend metadata: {backend_md}")

    # Create summary
    summary = {
        "run_id": str(run_id),
        "algorithm": algorithm,
        "size": size,
        "backend": backend,
        "metadata": {
            "backend": backend_md,
            "circuit": circuit_md,
        },
        "artifacts": []
    }

    # Download artifacts
    artifacts = run_doc.get("artifacts", [])
    for artifact in artifacts:
        try:
            rel_path = _download_artifact(artifact, Path(output_dir) / "artifacts" / str(run_id))
            summary["artifacts"].append(rel_path)
        except Exception as e:
            print(f"[WARN] {e}")

    # Write to disk
    out_file = Path(output_dir) / f"{algorithm}_{size}_{backend}_{run_id}.json"
    with open(out_file, "w") as f:
        json.dump(summary, f, indent=2)

    return str(out_file)

# ── Main function to process all completed runs ─────────────────────
def process_all_completed(output_dir=OUTPUT_DIR):
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    completed = runs.find({"status": "COMPLETED"})
    summary_paths = []

    for run_doc in completed:
        print(f"Processing run {run_doc['_id']}...")
        try:
            path = create_summary_from_run(run_doc, output_dir)
            print(f"[OK] Summary written to {path}")
            summary_paths.append(path)
        except Exception as e:
            print(f"[ERR] Run {run_doc['_id']}: {e}")

    print(f"\nGenerated {len(summary_paths)} summaries in `{output_dir}/`")
    return summary_paths

if __name__ == "__main__":
    process_all_completed()