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
