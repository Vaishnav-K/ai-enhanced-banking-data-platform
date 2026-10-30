# Scale Benchmark: 10 Million Transactions

This is an optional, local synthetic-data experiment that preserves the default 500-customer / 10,000-transaction demo. Scale inputs and outputs live under `data/scale/`; do not commit the generated CSV or Parquet data. The scale generator now defaults to **100,000 customers and 10,000,000 transactions**; the explicit flags in the commands below make the intended dataset size and output path visible and reproducible.

## 1. Update the existing checkout

From the project root in PowerShell:

```powershell
git pull origin main
```

If Git reports that local changes would be overwritten, stop and preserve/reconcile those changes first. Do not run `git reset --hard` or `git clean`.

## 2. Generate 10 million transactions

Use the project's existing Python 3.12 virtual environment:

```powershell
.\.venv312\Scripts\python.exe .\src\generate_scale_data.py --customers 100000 --transactions 10000000 --output-dir data\scale\raw-10m --seed 42
```

The generator streams transaction rows to CSV and does not change `data/raw/`. Expect a larger disk footprint and allow the command to finish. Ensure there is sufficient free disk space before starting.

## 3. Validate row counts and ID uniqueness

```powershell
@'
import csv, json, time
from pathlib import Path

base = Path("data/scale/raw-10m")
def inspect(path, key):
    started = time.perf_counter()
    count = 0
    seen = set()
    missing = 0
    with path.open("r", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            count += 1
            value = row.get(key, "").strip()
            if not value:
                missing += 1
            else:
                seen.add(value)
    return {
        "file": str(path), "rows": count, "unique_ids": len(seen),
        "missing_ids": missing, "duplicate_ids": count-len(seen)-missing,
        "bytes": path.stat().st_size,
        "seconds": round(time.perf_counter()-started, 2),
    }

result = {
    "customers": inspect(base / "customers.csv", "customer_id"),
    "transactions": inspect(base / "transactions.csv", "transaction_id"),
}
print(json.dumps(result, indent=2))
assert result["customers"]["rows"] == 100000
assert result["customers"]["unique_ids"] == 100000
assert result["transactions"]["rows"] == 10000000
assert result["transactions"]["unique_ids"] == 10000000
assert result["customers"]["missing_ids"] == 0
assert result["transactions"]["missing_ids"] == 0
assert result["customers"]["duplicate_ids"] == 0
assert result["transactions"]["duplicate_ids"] == 0
print("PASS: 10-million-row input validation")
'@ | .\.venv312\Scripts\python.exe -
```

## 4. Run the isolated Spark benchmark

```powershell
.\.venv312\Scripts\python.exe .\src\scale_spark_benchmark.py --input-dir data\scale\raw-10m --output-dir data\scale\spark_processed_10m --report reports\scale-benchmark-10m-results.json --master "local[2]" --shuffle-partitions 8
```

The script reads the 10-million-row CSV, deduplicates by transaction ID, validates amounts/timestamps/statuses/customer references, streams valid and quarantined results to CSV files using Spark's `toLocalIterator()` so the output step avoids Hadoop's failing Windows native file-committer path, runs grouped analytics, and writes a JSON report. The output path is separate from the baseline. It refuses to overwrite a non-empty output directory unless `--overwrite` is supplied.

If the run fails, do not immediately rerun with `--overwrite`. Read the exception and inspect the scale output directory first.

## 5. Review the measured evidence

```powershell
Get-Content .\reports\scale-benchmark-10m-results.json
Get-ChildItem .\data\scale\spark_processed_10m -Recurse -File |
    Measure-Object -Property Length -Sum
```

The JSON report records the observed row counts, reconciliation result, elapsed time, throughput, input sizes, output sizes, Spark version, and execution settings. On Windows, outputs are CSV because Hadoop's Parquet output committer can fail with `NativeIO$Windows.access0` when the matching native Hadoop library is unavailable. If Parquet output is required, run the benchmark in a Linux/WSL or Docker environment with compatible Hadoop libraries. These are measured only if the benchmark finishes and the report is produced.

## 6. Keep generated data out of Git

Review status:

```powershell
git status --short
```

Do not stage `data/scale/` or generated Parquet/CSV files. Publish only compact evidence such as the JSON result, benchmark documentation, and a summary of the run. Before publishing, inspect the report for local paths or other machine-specific details and redact those fields.

## 7. Publish actual results after a successful run

Do not fabricate timings or claim success before a run completes. Copy the successful report to a reviewed, sanitized evidence file such as `reports/scale-benchmark-10m-results-public.json`, update the README with the measured results, then commit only the code, documentation, and sanitized evidence. A GitHub Actions or portfolio badge should be added only if it is backed by a repeatable automated workflow.

## Measured local run (29 October 2026)

The 10-million-transaction Spark run completed successfully on a local Windows environment using synthetic data.

| Measure | Observed result |
|---|---:|
| Customers | 100,000 |
| Raw transactions | 10,000,000 |
| Deduplicated transactions | 10,000,000 |
| Duplicate rows removed | 0 |
| Valid transactions | 10,000,000 |
| Quarantined transactions | 0 |
| Reconciliation | PASS |
| Elapsed time | 356.266 seconds |
| Throughput | 28,068.91 raw transactions/second |
| Spark version | 3.5.7 |
| Master | `local[2]` |
| Shuffle partitions | 16 |

The elapsed time includes Spark startup, reads, transformations, writes, and grouped analytics. This is a local synthetic benchmark, not a production workload, cluster benchmark, or SLA. The Windows run wrote CSV output; it does not measure Parquet-write performance.

Sanitized evidence is published at [`reports/scale-benchmark-10m-results-public.json`](../reports/scale-benchmark-10m-results-public.json). The full local report contains machine-specific input/output paths and should remain local. Generated scale data should remain out of Git.
