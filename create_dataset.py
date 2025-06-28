import os
import json
import gridfs
from pathlib import Path
from pymongo import MongoClient
from dotenv import load_dotenv

# ── Load environment variables ───────────────────────────────────────
load_dotenv()

# ── MongoDB configuration ───────────────────────────────────────────
MONGO_USER = os.getenv('MONGO_INITDB_ROOT_USERNAME')
MONGO_PASSWORD = os.getenv('MONGO_INITDB_ROOT_PASSWORD')
MONGO_HOST = os.getenv('MONGO_HOST', 'localhost')
MONGO_PORT = os.getenv('MONGO_PORT', '27017')
MONGO_DB = os.getenv("MONGO_DATABASE", "sacred")
MONGO_URI = os.getenv("MONGO_URI") or \
             f'mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/?authSource=admin'

print(f"Connecting to MongoDB at {MONGO_URI}")

OUTPUT_DIR = os.getenv("OUTPUT_DIR", "output/dataset")
print(f"Output directory set to: {OUTPUT_DIR}")

# ── Connect to MongoDB and GridFS ───────────────────────────────────
client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
runs = db.runs
fs = gridfs.GridFS(db)

# ── Utility function to download artifact from GridFS ───────────────
def _download_artifact(artifact, target_dir):
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
    circuit_md = info.get("circuit", {}).get("doc", {})
    backend_md = info.get("backend", {})

    algorithm = circuit_md.get("algorithm")
    size = circuit_md.get("size")
    backend = backend_md.get("name")

    if not algorithm or not size:
        raise ValueError(f"Run {run_id} missing circuit metadata: {circuit_md}")
    if not backend:
        raise ValueError(f"Run {run_id} missing backend metadata: {backend_md}")

    summary = {
        "run_id": str(run_id),
        "algorithm": algorithm,
        "size": size,
        "backend": backend,
        "metadata": {"backend": backend_md, "circuit": circuit_md},
        "artifacts": []
    }

    artifacts = run_doc.get("artifacts", [])
    for artifact in artifacts:
        try:
            rel_path = _download_artifact(artifact, Path(output_dir) / "artifacts" / str(run_id))
            summary["artifacts"].append(rel_path)
        except Exception as e:
            print(f"[WARN] {e}")

    out_file = Path(output_dir) / f"{algorithm}_{size}_{backend}_{run_id}.json"
    with open(out_file, "w") as f:
        json.dump(summary, f, indent=2)

    return str(out_file)

# ── Generate histories for each (algorithm, size, backend) tuple ─────
def generate_histories(summary_paths, output_dir):
    from collections import defaultdict

    # Prepare containers for rows
    rows_by_type = {"circuit": defaultdict(list), "mirror": defaultdict(list)}

    for summary_path in summary_paths:
        with open(summary_path) as f:
            summary = json.load(f)
        run_id = summary["run_id"]
        algo = summary["algorithm"]
        size = summary["size"]
        backend = summary["backend"]
        tuple_key = f"{algo}_{size}_{backend}"

        for rel_art in summary.get("artifacts", []):
            art_path = Path(output_dir) / rel_art
            name_lower = art_path.name.lower()
            if "circuit" in name_lower:
                hist_type = "circuit"
            elif "mirror" in name_lower:
                hist_type = "mirror"
            else:
                continue

            if not art_path.exists():
                print(f"[WARN] Artifact file not found: {art_path}")
                continue

            with open(art_path) as af:
                for line in af:
                    if not line.strip():
                        continue
                    try:
                        row = json.loads(line)
                    except json.JSONDecodeError:
                        print(f"[WARN] Invalid JSON in {art_path}: {line.strip()}")
                        continue
                    # Attach run_id and accumulate
                    row["run_id"] = run_id
                    rows_by_type[hist_type][tuple_key].append(row)

    # Write out sorted histories
    histories_dir = Path(output_dir) / "histories"
    for hist_type, tuples in rows_by_type.items():
        type_dir = histories_dir / hist_type
        type_dir.mkdir(parents=True, exist_ok=True)
        for tuple_key, rows in tuples.items():
            rows_sorted = sorted(rows, key=lambda x: x.get("timestamp"))
            out_file = type_dir / f"{tuple_key}.jsonl"
            with open(out_file, "w") as of:
                for r in rows_sorted:
                    of.write(json.dumps(r) + "\n")
            print(f"[OK] History for {tuple_key} ({hist_type}) -> {out_file}")

# ── Main processing ─────────────────────────────────────────────────
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

    # Build and write histories
    generate_histories(summary_paths, output_dir)

    return summary_paths

if __name__ == "__main__":
    process_all_completed()