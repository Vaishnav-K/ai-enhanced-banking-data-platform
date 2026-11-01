# Project Continuity and Maintenance Notes

> Maintainer reference for the Enterprise Banking Data Platform. Keep this file current whenever a meaningful repository, architecture, workflow, or evidence change is made.

## Project identity

- **Display name:** Enterprise Banking Data Platform
- **Repository:** https://github.com/Vaishnav-K/enterprise-banking-data-platform
- **Featured portfolio:** https://vaishnav-k.github.io/data-engineer-portfolio/
- **Portfolio source repository:** https://github.com/Vaishnav-K/data-engineer-portfolio
- **Default branch:** `main`
- **Data policy:** generated synthetic banking-style data only; no real customer records.

## One-paragraph summary

An end-to-end synthetic banking data engineering platform demonstrating batch and incremental ETL, schema and value validation, deduplication, referential-integrity checks, quarantine with rejection reasons, analytical outputs, threshold-based anomaly screening, DuckDB/PySpark cross-engine reconciliation, run-level evidence, lineage, recovery diagnostics, operational observability, Docker-based Airflow orchestration, and optional authenticated Amazon S3 ingestion.

## Architecture at a glance

1. **Synthetic source data:** customer and transaction CSV files under `data/raw/`.
2. **Ingestion and validation:** schema/value parsing, type conversion, duplicate handling, transaction checks and customer-reference checks.
3. **Processing:** DuckDB is the core local ETL path; a separate PySpark path performs comparable transformations for reconciliation.
4. **Quality handling:** valid rows are written to curated Parquet outputs; rejected rows are written to quarantine with a reason.
5. **Incremental processing:** a later transaction batch is checked against IDs already processed and prior incremental data.
6. **Analytics and diagnostics:** summaries, threshold-based anomaly screening, quality reports, lineage, stage observations and recovery assessments.
7. **Orchestration:** an Apache Airflow DAG in Docker Compose launches the pipeline and validates the generated run report.
8. **Optional cloud path:** authenticated S3 upload of raw CSVs, with SHA-256 checksums, verification and audit records.
9. **Evidence:** structured JSON reports feed the HTML health dashboard and documented benchmark results.

## Technology map

| Area | Technology | Purpose |
|---|---|---|
| Main local processing | Python, DuckDB, SQL | Read, validate, deduplicate, classify and write curated data |
| Storage format | CSV, Parquet, JSON/JSONL, SQLite manifest | Inputs, typed outputs, reports and ingestion history |
| Distributed processing | PySpark 3.5.7 | Independent Spark execution and scale experiment |
| Orchestration | Apache Airflow 3.1.7 | Trigger and validate pipeline runs |
| Runtime packaging | Docker, Docker Compose, Java 17 | Reproducible Airflow/Spark execution environment |
| Cloud storage | Amazon S3, boto3 | Optional raw-file upload and verification |
| Query validation | AWS Glue Data Catalog, Amazon Athena | SQL checks against S3-backed Parquet |
| Quality and operations | Unit tests, lineage, observability, dashboard | Verify results and explain execution outcomes |
| CI | GitHub Actions | Core pipeline, Spark checks and portfolio deployment |

## Important implementation boundaries

- The dataset is synthetic. Do not describe it as real bank data.
- Anomaly screening is rule/threshold-based, not a trained fraud model.
- Recovery and diagnostic outputs are advisory and read-only. They do not restart jobs or remediate production systems.
- S3 upload is optional for the local demo and requires AWS credentials for a bucket the operator controls.
- Benchmark figures are local synthetic-data evidence, not a production SLA or cluster benchmark.
- The 10-million-row Spark run wrote CSV output on Windows; it does not measure Parquet-write performance.
- Missing historical evidence must be reported as unavailable, not converted into a passing metric.

## Baseline demo evidence

Recorded synthetic sample:

| Measure | Result |
|---|---:|
| Customers | 500 |
| Raw transactions | 10,004 |
| Rows after transaction-ID deduplication | 10,003 |
| Valid transactions written to Parquet | 10,000 |
| Quarantined transactions | 3 |
| Reconciliation | PASS |

The three seeded rejected records cover an invalid/non-positive amount, a missing customer ID, and an unknown customer ID. The arithmetic reconciliation is **10,003 = 10,000 valid + 3 quarantined**.

Incremental sample:

- 4 incoming transactions.
- 2 valid rows.
- 2 quarantined rows.
- Duplicate detection, transaction-type/value validation and customer-reference checks are applied.

## 10-million-transaction Spark evidence

Recorded local Windows run:

| Measure | Observed result |
|---|---:|
| Synthetic customers | 100,000 |
| Raw transactions | 10,000,000 |
| Deduplicated transactions | 10,000,000 |
| Valid transactions | 10,000,000 |
| Quarantined transactions | 0 |
| Reconciliation | PASS |
| Spark | 3.5.7 |
| Master | `local[2]` |
| Shuffle partitions for the recorded run | 16 |
| Elapsed time | 356.266 seconds |
| Throughput | 28,068.91 raw rows/second |

Timing includes Spark startup, reads, transformations, writes and grouped analytics. The output was CSV on Windows. Do not call this a production-scale benchmark, cluster benchmark, SLA, or Parquet-write benchmark. The public sanitized evidence is `reports/scale-benchmark-10m-results-public.json`.

## Main repository entry points

| Purpose | File |
|---|---|
| Main demo/orchestration entry point | `src/demo.py` |
| Synthetic data generation | `src/generate_data.py` |
| Main DuckDB ETL | `src/etl_pipeline.py` |
| Incremental processing | `src/incremental_pipeline.py` |
| Spark pipeline | `src/spark_pipeline.py` |
| DuckDB/Spark comparison | `src/compare_engines.py` |
| S3 raw ingestion | `src/s3_ingestion.py` |
| Airflow DAG on main | `orchestration/airflow/dags/banking_data_platform_pipeline.py` |
| Docker image | `orchestration/airflow/Dockerfile` |
| Docker Compose | `orchestration/airflow/docker-compose.yaml` |
| Run lineage | `src/lineage_tracker.py`, `src/runtime_lineage_report.py` |
| Recovery assessment | `src/recovery_assessment.py`, `src/recovery_decision_engine.py` |
| Observability | `src/pipeline_observability.py` |
| Dashboard | `src/platform_dashboard.py` |
| Architecture | `docs/architecture.md` |
| Beginner demo walkthrough | `docs/demo-walkthrough.md` |
| Quality rules | `docs/data-quality.md` |
| Anomaly rules | `docs/anomaly-detection.md` |
| Athena evidence | `docs/aws-athena-s3-validation.md` |
| Scale methodology | `docs/scale-benchmark-10m.md` |

## Run and validation guidance

### Local run

Use Python 3.12 and the repository's Windows virtual environment:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\setup.ps1
.\.venv312\Scripts\python.exe .\src\generate_data.py
.\.venv312\Scripts\python.exe .\src\demo.py
```

Run unit tests:

```powershell
.\.venv312\Scripts\python.exe -m unittest discover -s tests -v
```

Generate the dashboard:

```powershell
.\.venv312\Scripts\python.exe .\src\platform_dashboard.py
```

### Optional Airflow/S3 execution

The committed main-branch DAG currently has one PythonOperator task named `run_and_validate_pipeline`. It invokes:

```text
src/demo.py --include-spark --upload-s3
```

The DAG removes the previous run report, checks the process exit code, requires a newly written report, parses it, requires `status == PASS`, and checks for a run ID. The Airflow demo needs Docker Desktop and AWS credentials configured in the local environment. Follow `docs/demo-walkthrough.md`; never commit secrets or `.env` files.

**Version note:** a separate local validation worktree has also been used to exercise a more granular three-task preflight/run/evidence-validation flow. Do not assume that this three-task local validation variant is identical to the single-task DAG currently committed on `main`. Inspect the checked-out branch and source before explaining or changing the DAG.

### Shutdown

Use `docker compose down` to stop services. Do not add `-v` unless deliberately deleting persistent named volumes.

## Naming and compatibility rules

- Public project name: **Enterprise Banking Data Platform**.
- Canonical repository slug: `enterprise-banking-data-platform`.
- Update public display text, README headings, badge/source links and portfolio links when the project identity changes.
- Do **not** broadly rename internal runtime identifiers as part of a branding change. Current compatibility-sensitive values include:
  - Airflow DAG ID: `banking_data_platform_pipeline`
  - Airflow task ID: `run_and_validate_pipeline`
  - Container project path: `/opt/banking-platform`
  - S3 object prefix: `banking-data/raw/`
  - Airflow/Compose names, volume names, environment variables and generated data paths.
- Do not rename a bucket or change an object prefix without a separate migration plan.
- Historical run records may retain a former `pipeline_name` value. The dashboard aliases that exact legacy display label to the current project name at presentation time; stored run IDs, timestamps, statuses and source evidence are not rewritten. The exact former label therefore remains as a compatibility constant in `src/platform_dashboard.py`; it must not be used as the dashboard's public title or visible project label.

## Portfolio deployment and branding history

| Date | Change | Verification |
|---|---|---|
| 2026-11-01 | GitHub repository renamed from the previous AI-branded slug to `enterprise-banking-data-platform`; repository description updated | GitHub CLI reported success; repository metadata shows the new slug |
| 2026-11-01 | Project README rebrand PR #5 merged | New README heading, clone path and badges use the new slug |
| 2026-11-01 | Portfolio rebrand PR #2 merged | Portfolio source, links, README and deployment workflow use the new slug |
| 2026-11-01 | GitHub Pages deployment run #24 completed successfully | Run ID `41336863723`; deploy job and all recorded steps concluded `success` |
| 2026-11-02 | Rebranded the additional embedded portfolio copy in `portfolio-site/index.html` | Commit `74c9c15089cdf2ef9758339d6a2050e1fc93cbd2`; replaced the old visible title, description and repository links |
| 2026-11-02 | Aligned the architecture document title | Commit `06906b60040c005b741e959f59c387dd139dd4e2` |
| 2026-11-02 | Added `PROJECT_HANDOFF.md` and linked it from the public README | Handoff creation commit `9e6606506950cac3ffa8e873f67591fa0a9de400`; README link commit `e5bdf21d837b75e734b30ef48a067953f39dee88` |
| 2026-11-02 | Updated dashboard heading/title and normalized the exact historical pipeline display label | Dashboard generator commit `9c97d12260f343fed7edc422a0e501639714d033`; published dashboard refresh commit `908fbeba7bdf89ebaad6dc0def47297ab6acb85f`. Persisted historical evidence was not rewritten. |
| 2026-11-02 | Audited public branding references across both repositories | Old repository slug and old visible project title absent from the reviewed user-facing README, handoff, architecture, embedded portfolio, main portfolio source, portfolio README, deployment workflow, banking-repository workflows and published dashboard. The dashboard generator intentionally retains the exact former title as a non-displayed compatibility constant for historical run labels. This is a targeted source audit, not a rewrite of Git history. |
| 2026-11-02 | Added this continuity file and linked it from README | Current maintenance notes |

The main portfolio source is `Vaishnav-K/data-engineer-portfolio/site/index.html`. The banking repository also contains a second embedded site copy at `portfolio-site/index.html`; keep its visible project name and repository links aligned when editing it. Do not confuse these two locations.

## Open engineering work

At the latest reviewed state (2026-11-02):

- PR #2, **Align Spark and DuckDB transformations with 10M-scale mock workflow**, remains open.
- PR #3, **Add Airflow preflight and evidence-gated orchestration**, remains open and is intended to follow PR #2.
- Review each PR's current diff, base/head, checks and mergeability before merging. Do not infer that a PR is ready merely because it was mergeable at the last check.
- Do not reset, clean, force-push or delete worktrees without checking local uncommitted changes and generated evidence first.

## Continuity update policy

For every meaningful GitHub change batch:

1. Review the current repository state and preserve existing user changes.
2. Make the requested code/documentation change.
3. Run the relevant tests or workflow and record the actual outcome.
4. Update this file's **Portfolio deployment and branding history** or add a new dated engineering note.
5. Keep README links and portfolio links accurate.
6. State what remains unverified or pending; do not report a workflow as successful before checking its run result.

## Source-of-truth order

When answering project questions, prioritize:

1. Current code on the branch being discussed.
2. Current README and technical docs in that branch.
3. Actual JSON run reports, audit records, test results and workflow logs.
4. Historical notes in this file.

If a written note conflicts with current code or a current run report, investigate the discrepancy and update this file rather than silently assuming either source is correct.
