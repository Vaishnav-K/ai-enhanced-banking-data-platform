# AI-Enhanced Banking Data Platform

A banking data engineering portfolio project built with Python, DuckDB, SQL, Parquet, and Amazon S3. It demonstrates batch ETL, incremental processing, data quality controls, analytics, anomaly screening, automated testing, and cloud object storage using synthetic banking data.

## Architecture

```mermaid
flowchart TB
    subgraph S1["01 | DATA SOURCES"]
        A["Synthetic Banking CSVs"]
        B["Incremental Transaction Batch"]
    end

    subgraph S2["02 | INGESTION"]
        C["DuckDB Batch ETL"]
        D["Incremental ETL"]
    end

    subgraph S3["03 | DATA QUALITY"]
        E["Schema Validation"]
        F["Deduplication"]
        G{"Valid Record?"}
        H["Processed Parquet"]
        I["Quarantine + Rejection Reasons"]
        J["Incremental Validation"]
        K["Incremental Parquet"]
        L["Incremental Quarantine"]
    end

    subgraph S4["04 | ANALYTICS"]
        M["SQL Analytics"]
        N["Data Quality Report"]
        O["Anomaly Screening"]
        P["CSV Exports"]
    end

    subgraph S5["05 | CLOUD & CI/CD"]
        Q[("Amazon S3 | Raw Data")]
        R["GitHub Actions"]
        T["Automated Tests"]
        R --> T
    end

    A --> C --> E --> F --> G
    G -->|Valid| H
    G -->|Invalid| I
    B --> D --> J
    J --> K
    J --> L

    H --> M --> P
    H --> N
    H --> O
    I --> N
    K --> N
    L --> N

    A -. "CSV upload" .-> Q

    classDef source fill:#E8F1FF,stroke:#3267B2,color:#17365D
    classDef ingest fill:#DDF5E8,stroke:#21876A,color:#124D3E
    classDef quality fill:#FFF0D0,stroke:#C48A1B,color:#67450A
    classDef analytics fill:#EEE4FF,stroke:#8256C5,color:#42256D
    classDef cloud fill:#DDF4FA,stroke:#1684A5,color:#10495B

    class A,B source
    class C,D ingest
    class E,F,G,H,I,J,K,L quality
    class M,N,O,P analytics
    class Q,R,T cloud
```

## Demonstrated Results

### Main ETL Pipeline
- 500 customer records processed.
- 10,004 raw transaction rows received.
- 10,003 rows remaining after deduplication.
- 10,000 valid transactions written to Parquet.
- 3 records quarantined with rejection reasons.
- Reconciliation check passed.

### Incremental Processing
- 4 incoming transactions processed.
- 2 valid transactions produced.
- 2 records quarantined.
- Duplicate detection and customer-reference validation implemented.

### Analytics and Quality
- Daily transaction summary.
- Transaction status summary.
- Customer-level transaction summary.
- JSON data quality report with reconciliation and duplicate checks.
- Threshold-based anomaly screening.
- 9 automated unit tests passing locally and in GitHub Actions.

### AWS S3 Integration
- Region: Asia Pacific (Sydney), `ap-southeast-2`.
- Private S3 bucket with Block Public Access enabled.
- SSE-S3 encryption.
- Dedicated IAM user with restricted upload permissions.
- Three synthetic CSV files uploaded and verified under `banking-data/raw/`.
- S3 upload is optional in the end-to-end demo.

### Run Locally

Run the pipeline and tests without uploading to S3:

```powershell
.\.venv\Scripts\python.exe .\src\demo.py
```

Preview S3 upload destinations:

```powershell
$env:BANKING_S3_BUCKET = "<your-bucket-name>"
.\.venv\Scripts\python.exe .\src\s3_ingestion.py
```

Upload raw CSV files:

```powershell
.\.venv\Scripts\python.exe .\src\s3_ingestion.py --upload
```

Run the pipeline and upload raw files after successful tests:

```powershell
.\.venv\Scripts\python.exe .\src\demo.py --upload-s3
```

### Engineering Practices
- Modular Python scripts and SQL transformations.
- Parquet-based processed datasets.
- Incremental ingestion and quarantine handling.
- Automated unit tests and GitHub Actions CI.
- Git version control.
- Synthetic data only; no real banking data.
- Cloud usage kept minimal and monitored against available AWS credits.## Running the Pipeline

Use Python 3.12 and the project virtual environment.

### 1. Local workflow
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_demo_with_report.ps1
```

Runs the ETL pipeline, incremental processing, quality reporting, anomaly detection, analytics exports, automated tests, and ingestion health report. S3 upload is disabled by default.

### 2. Workflow with S3 ingestion
```powershell
$env:BANKING_S3_BUCKET = "your-s3-bucket-name"
powershell -ExecutionPolicy Bypass -File .\scripts\run_demo_with_report.ps1 -UploadS3
```

S3 ingestion calculates SHA-256 checksums, skips unchanged files, uploads changed or missing files, verifies uploads, and records results.

### 3. Ingestion health report
Generated reports and the local SQLite manifest are stored under `data/processed/`:
- `ingestion_report.json` — ingestion health summary.
- `s3_ingestion_audit.jsonl` — ingestion audit events.
- `ingestion_manifest.db` — file status and checksum history.

Run the report separately:
```powershell
.\.venv312\Scripts\python.exe src\manifest_report.py
```

### 4. Automated tests
```powershell
.\.venv312\Scripts\python.exe -m unittest discover -s tests -v
```

S3 tests use mocked AWS responses and do not upload real objects.


## Portfolio Documentation

- [Architecture diagram and design principles](docs/architecture.md)
- [End-to-end demo walkthrough](docs/demo-walkthrough.md)
- Dashboard generator: `src/platform_dashboard.py`
- Executive health summary: `src/platform_health_demo.py`

### Run the dashboard

```powershell
.\.venv312\Scripts\python.exe src\platform_dashboard.py
```

Open `reports/platform_health_dashboard.html` in your browser. The dashboard is generated from the local pipeline evidence and can be refreshed by rerunning the command.

The project uses synthetic banking data. Review the documented limitations and actual runtime evidence before interpreting metrics as production guarantees.
