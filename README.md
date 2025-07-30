# QSimBench

> **An Execution-Level Benchmark Suite for Quantum Software Engineering**

## Introduction

**QSimBench** is an open-source Python library and dataset designed to advance **Quantum Software Engineering (QSE)** through *reproducible, scalable, and transparent* benchmarking.

Unlike traditional circuit-focused benchmarks, QSimBench provides **precomputed, high-volume execution traces** — that is, real measured outcomes of running quantum circuits — across a wide range of quantum algorithms, input sizes (number of qubits), and simulation backends (including both idealized and noisy models).

This means you can rigorously test, compare, and develop QSE tools (like workload orchestrators, error mitigation techniques, and monitoring systems) **without** needing to run thousands of quantum circuits yourself or access costly hardware. QSimBench empowers both research and development by making experiments reproducible and resource-efficient.

## Why QSimBench?

* **Reproducibility**: Get exactly the same experimental data, every time.
* **Rich Data**: Thousands of outcome batches per configuration; includes not just outcomes, but also the full quantum circuit, noise model, and backend metadata.
* **Rapid Prototyping**: Skip the heavy cost (and time!) of running large experiments; sample realistic output distributions instantly.
* **Transparency & Auditability**: Full context for every execution — retrace, analyze, and verify all details.
* **Easy to Use**: Fetch and sample outcome data with a single Python call.

## Installation

```bash
pip install qsimbench
```

## Quickstart Example

```python
from qsimbench import get_outcomes

# Sample 2048 outcomes from a QAOA circuit (8 qubits) on the 'aer_simulator' backend
counts = get_outcomes(
    algorithm="qaoa",
    size=8,
    backend="aer_simulator",
    shots=2048,
    circuit_kind="circuit",   # or "mirror"
    exact=True,               # ensures the output sums **exactly** to 'shots'
    strategy="random",        # or "sequential"
    seed=42                   # for reproducibility
)

print(counts)  # {'00110101': 96, '10100100': 123, ...}
```

## Main Features

### Sampling Execution Outcomes

Retrieve outcome counts for a given algorithm, size, and backend:

```python
from qsimbench import get_outcomes

counts = get_outcomes(
    algorithm="qft",
    size=14,
    backend="fake_fez",
    shots=20000,
)
```

### Dataset Exploration

See what is available in the dataset:

```python
from qsimbench import get_index

index = get_index()
print(index)
# {'qaoa': {8: ['aer_simulator', 'fake_fez',...], ...}, ...}
```

### Metadata Access

Access the circuit, noise model, and backend metadata for any configuration:

```python
from qsimbench import get_metadata

metadata = get_metadata("qaoa", 8, "aer_simulator")[0]["metadata"]
print(metadata['circuit']['circuit'])  # OpenQASM code string
print(metadata[0]['backend'])  # Detailed backend configurations and noise description
```

## Available Data (July 2025)

QSimBench provides high-volume execution data for a variety of quantum algorithms, input sizes, and backends. For each (algorithm, size, backend) combination, QSimBench has currenlty gathered 20,000 unique real outcomes (by 50 shots batches). These batches can be sampled either sequentially or randomly — supporting both streaming and i.i.d. experimental scenarios, and can be reused indefinitely to simulate an infinite number of shots.

Below is a snapshot of available data:
```
Algorithm: dj
  Size: 4-15 → Backends: ['aer_simulator', 'fake_fez', 'fake_kyiv', 'fake_marrakesh', 'fake_sherbrooke', 'fake_torino']

Algorithm: ghz
  Size: 4-15 → Backends: ['aer_simulator', 'fake_fez', 'fake_kyiv', 'fake_marrakesh', 'fake_sherbrooke', 'fake_torino']

Algorithm: qaoa
  Size: 4-15 → Backends: ['aer_simulator', 'fake_fez', 'fake_kyiv', 'fake_marrakesh', 'fake_sherbrooke', 'fake_torino']

Algorithm: qft
  Size: 4-15 → Backends: ['aer_simulator', 'fake_fez', 'fake_kyiv', 'fake_marrakesh', 'fake_sherbrooke', 'fake_torino']

Algorithm: qnn
  Size: 4-15 → Backends: ['aer_simulator', 'fake_fez', 'fake_kyiv', 'fake_marrakesh', 'fake_sherbrooke', 'fake_torino']

Algorithm: qpeexact
  Size: 4-15 → Backends: ['aer_simulator', 'fake_fez', 'fake_kyiv', 'fake_marrakesh', 'fake_sherbrooke', 'fake_torino']

Algorithm: random
  Size: 4-15 → Backends: ['aer_simulator', 'fake_fez', 'fake_kyiv', 'fake_marrakesh', 'fake_sherbrooke', 'fake_torino']

Algorithm: realamprandom
  Size: 4-15 → Backends: ['aer_simulator', 'fake_fez', 'fake_kyiv', 'fake_marrakesh', 'fake_sherbrooke', 'fake_torino']

Algorithm: grover-noancilla
  Size: 4-9 → Backends: ['aer_simulator', 'fake_fez', 'fake_kyiv', 'fake_marrakesh', 'fake_sherbrooke', 'fake_torino']

Algorithm: su2random
  Size: 4-15 → Backends: ['aer_simulator', 'fake_fez', 'fake_kyiv', 'fake_marrakesh', 'fake_sherbrooke', 'fake_torino']

Algorithm: twolocalrandom
  Size: 4-15 → Backends: ['aer_simulator', 'fake_fez', 'fake_kyiv', 'fake_marrakesh', 'fake_sherbrooke', 'fake_torino']

Algorithm: vqe
  Size: 4-15 → Backends: ['aer_simulator', 'fake_fez', 'fake_kyiv', 'fake_marrakesh', 'fake_sherbrooke', 'fake_torino']

Algorithm: wstate
  Size: 4-15 → Backends: ['aer_simulator', 'fake_fez', 'fake_kyiv', 'fake_marrakesh', 'fake_sherbrooke', 'fake_torino']
```

For each trace, you can sample any number of shots from the 20,000 pre-collected real batches, with QSimBench reusing these in a loop or at random as needed to simulate arbitrarily large experiments.

## API Reference

### `get_outcomes(...)`

Sample quantum execution results for a chosen algorithm, problem size, backend, and shot count.

* **algorithm**: Name of the quantum algorithm (e.g., `"qaoa"`, `"qft"`).
* **size**: Number of qubits (positive integer).
* **backend**: Backend or simulator name.
* **shots**: Number of measurement outcomes to sample.
* **circuit\_kind**: `"circuit"` (standard) or `"mirror"` (mirror circuit).
* **exact**: If `True`, total output equals `shots` (using multinomial sampling).
* **strategy**: `"sequential"` (next batch) or `"random"` (random batch).
* **seed**: Integer seed for reproducibility.

### `get_index(...)`

Lists all available algorithms, sizes, and backends in the dataset.

* **circuit\_kind**: `"circuit"` or `"mirror"`.
* **by\_backend**: If `True`, groups by backend instead of algorithm.

### `get_metadata(...)`

Fetches the circuit, backend, and noise model metadata for a given configuration.

## Dataset Architecture

Each **(algorithm, size, backend)** combination in QSimBench is backed by thousands of raw outcome batches (50 shots each), fully indexed and ready for fast sampling and analysis. All raw data is cached locally to avoid repeated downloads. The library handles all caching and networking for you.

## When Should You Use QSimBench?

* **Developing or comparing quantum software engineering tools** (error mitigation, schedulers, monitors, etc.)
* **Benchmarking quantum circuit execution under realistic noise models**
* **Building reproducible experiments without running on real hardware**
* **Rapid prototyping or teaching with quantum measurement data**

## How Does QSimBench Differ from Other Benchmarks?

* Other quantum benchmarks focus on circuit definitions; **QSimBench delivers reproducible, real-world measurement outcomes** — the data your QSE tools actually operate on.
* You can *instantly* reproduce or extend published experiments — no more re-running expensive or non-deterministic jobs.
* Full metadata (circuits, noise models, configs) enables transparency and in-depth research.

## Troubleshooting

### 🛑 403 Error: "Rate limit exceeded" or "GitHub API error"

If you encounter an error like:

```
QSimBenchError: GitHub API error: 403 Client Error: rate limit exceeded for url: https://api.github.com/repos/GBisi/qsimbench-dataset/contents/...
```

This means GitHub is rejecting unauthenticated requests due to rate limits. GitHub allows only **60 unauthenticated requests per hour** per IP.

To raise this limit to **5,000/hour**, you need to authenticate using a **GitHub Personal Access Token (PAT)**.

### ✅ Solution: Provide a GitHub Token

You can set the token using **either** an environment variable or a `.env` file.

#### Option 1: Export an environment variable

```bash
export GITHUB_TOKEN=ghp_your_actual_token_here
python your_script.py
```

#### Option 2: Use a `.env` file with `python-dotenv`

1. Create a file named `.env` in your project root:

   ```
   GITHUB_TOKEN=ghp_your_actual_token_here
   ```

2. Install the package:

   ```bash
   pip install python-dotenv
   ```

3. Load the token at the start of your script:

   ```python
   from dotenv import load_dotenv
   load_dotenv()
   ```

Then continue as usual.

#### Verify that your token is working:

```bash
curl -H "Authorization: token YOUR_TOKEN" https://api.github.com/rate_limit
```

Check that `"limit": 5000` is returned.

### Additional Tips

* Make sure the token has `public_repo` scope (or `repo` if you're accessing private data).
* Never commit your token or `.env` file to version control.
* If you're still seeing 403s, add short delays (`time.sleep(0.1)`) between many API requests.

## Citing QSimBench

If you use QSimBench in your research, please cite:

> Bisicchia, G., et al. "QSimBench: An Execution-Level Benchmark Suite for Quantum Software Engineering". 2025 IEEE International Conference on Quantum Computing and Engineering (QCE), 2025.

```
@inproceedings{bisicchia2025qsimbench,
  title={QSimBench: An Execution-Level Benchmark Suite for Quantum Software Engineering},
  author={Bisicchia, Giuseppe and Bocci, Alessandro and Garc{\'\i}a-Alonso, Jos{\'e} and Murillo, Juan M and Brogi, Antonio},
  booktitle={2025 IEEE International Conference on Quantum Computing and Engineering (QCE)},
  year={2025},
}
```

## License

[GNU AFFERO v3](./LICENSE)

## Get Involved

Issues and pull requests are welcome! For questions, feature requests, or to report bugs, please [open an issue](https://github.com/GBisi/qsimbench/issues).

**QSimBench: Making quantum experiments reproducible, scalable, and fair.**
