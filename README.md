# AI-Enhanced Banking Data Platform

A local banking data engineering platform built with Python, DuckDB, SQL, and Parquet. It demonstrates batch ETL, data quality validation, incremental processing, analytics, and anomaly screening using synthetic banking data.

## Architecture

```mermaid
flowchart TD
    A[ Synthetic Banking CSVs ] --> B[ DuckDB Ingestion ]
    B --> C[ Data Validation ]
    C --> D[ Deduplication ]
    D --> E{ Record Quality }
    E -->|Valid| F[ Processed Parquet ]
    E -->|Invalid| G[ Quarantine with Reasons ]
    F --> H[ SQL Analytics ]
    F --> I[ Anomaly Screening ]
    H --> J[ CSV Reports ]
    F --> K[ Quality Report ]
    G --> K
    L[ Incremental CSV Batch ] --> M[ Incremental Validation ]
    M --> N[ Incremental Parquet Output ]
    M --> O[ Incremental Quarantine ]
    P[ GitHub Actions ] --> Q[ Automated Unit Tests ]

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
- Duplicate detection against existing transactions and customer-reference validation implemented.

### Analytics and Quality
- Daily transaction summary.
- Transaction status summary.
- Customer-level transaction summary.
- JSON data quality report with reconciliation and duplicate checks.
- Threshold-based anomaly screening.
- 9 automated unit tests passing.

### Engineering Practices
- Modular Python scripts.
- SQL-based transformations using DuckDB.
- Parquet storage for processed datasets.
- Automated test execution.
- Git version control and GitHub Actions CI.
- Synthetic data; no paid cloud infrastructure required.
