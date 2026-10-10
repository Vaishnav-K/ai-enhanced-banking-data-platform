# AI-Enhanced Banking Data Platform

A banking data engineering portfolio project built with Python, DuckDB, SQL, Parquet, and Amazon S3. It demonstrates batch ETL, incremental processing, data quality controls, analytics, anomaly screening, automated testing, and cloud object storage using synthetic banking data.

## Architecture

```mermaid
flowchart LR
    subgraph SRC["01 · DATA SOURCES"]
        A["Synthetic Banking CSVs"]
        B["Incremental Transaction Batch"]
    end

    subgraph ING["02 · INGESTION"]
        C["DuckDB Batch Ingestion"]
        D["Incremental Ingestion"]
    end

    subgraph PROC["03 · QUALITY & PROCESSING"]
        E["Schema & Data Validation"]
        F["Deduplication"]
        G{"Record Quality"}
        H["Processed Parquet"]
        I["Quarantine with Reasons"]
        J["Incremental Validation"]
        K["Incremental Parquet"]
        L["Incremental Quarantine"]
    end

    subgraph ANA["04 · ANALYTICS & MONITORING"]
        M["SQL Analytics"]
        N["Data Quality Report"]
        O["Anomaly Screening"]
        P["CSV Analytics Exports"]
    end

    subgraph CLOUD["05 · CLOUD & CI/CD"]
        Q[("Amazon S3 Raw Data Lake")]
        R["GitHub Actions"]
        S["Automated Unit Tests"]
    end

    A --> C
    C --> E
    E --> F
    F --> G
    G -->|Valid| H
    G -->|Invalid| I

    B --> D
    D --> J
    J --> K
    J --> L

    H --> M
    H --> N
    H --> O
    M --> P
    I --> N
    K --> N
    L --> N

    A -. "Raw CSV upload" .-> Q
    R --> S

    classDef source fill:#E8F1FF,stroke:#3267B2,color:#17365D,stroke-width:1.5px
    classDef ingest fill:#E5F7F0,stroke:#21876A,color:#124D3E,stroke-width:1.5px
    classDef process fill:#FFF3D6,stroke:#C48A1B,color:#67450A,stroke-width:1.5px
    classDef analytics fill:#F0E8FF,stroke:#8256C5,color:#42256D,stroke-width:1.5px
    classDef cloud fill:#E1F4FA,stroke:#1684A5,color:#10495B,stroke-width:1.5px

    class A,B source
    class C,D ingest
    class E,F,G,H,I,J,K,L process
    class M,N,O,P analytics
    class Q,R,S cloud
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
- Cloud usage kept minimal and monitored against available AWS credits.