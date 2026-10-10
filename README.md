\# AI-Enhanced Banking Data Platform



\## Overview

A banking data engineering project that processes synthetic customer and transaction data, validates records, removes duplicate transactions, quarantines invalid records, and generates analytics-ready Parquet datasets.



\## Architecture

1\. \*\*Data Generation:\*\* Synthetic banking customers and transactions.

2\. \*\*Data Ingestion:\*\* Load raw CSV files using DuckDB.

3\. \*\*Data Quality:\*\* Validate required fields, transaction types, amounts, timestamps, statuses, and customer references.

4\. \*\*Deduplication:\*\* Remove duplicate transactions using `transaction\_id`.

5\. \*\*Quarantine:\*\* Store invalid records with rejection reasons for investigation.

6\. \*\*Storage:\*\* Write processed datasets in Parquet format.

7\. \*\*Analytics:\*\* Execute SQL queries for daily transaction metrics, status summaries, and customer-level analysis.

8\. \*\*Testing:\*\* Automated unit tests validate output counts and quarantine behavior.



\## Technology Stack

\- Python

\- DuckDB

\- SQL

\- CSV

\- Apache Parquet

\- Python `unittest`



\## Data Quality Results

| Metric | Result |

|---|---:|

| Customers processed | 500 |

| Raw transaction rows | 10,004 |

| Rows after deduplication | 10,003 |

| Valid transactions | 10,000 |

| Invalid transactions | 2 |

| Unknown-customer records | 1 |

| Total quarantined records | 3 |



The pipeline reconciliation check passed. All five automated tests passed.



\## Output Datasets

\- `data/processed/customers.parquet`

\- `data/processed/transactions.parquet`

\- `data/quarantine/transactions.parquet`



\## Analytics

SQL queries cover:

\- Daily transaction volume and value

\- Successful, failed, and pending transaction summaries

\- Customer-level transaction counts and total transaction values



\## Run the Project

Activate the virtual environment:



```powershell

.\\.venv\\Scripts\\Activate.ps1

```



Run the pipeline:



```powershell

python .\\src\\etl\_pipeline.py

```



Run automated tests:



```powershell

python -m unittest discover -s tests -p "test\_\*.py" -v



\## Incremental Processing



The incremental pipeline processes a separate batch of incoming transactions.

It validates records, detects duplicates against the existing transaction

dataset, checks customer references, and routes rejected records to quarantine.



Run the incremental pipeline:



```powershell

python .\\src\\incremental\_pipeline.py

```



\## Data Privacy

This project uses synthetic banking data and does not require real customer information or paid cloud services.


## Analytics Exports

The project generates three business-ready CSV reports:

- `daily_transaction_summary.csv` — daily transaction volume, value, and status counts.
- `transaction_status_summary.csv` — transaction counts, total amounts, and status percentages.
- `customer_transaction_summary.csv` — customer-level transaction counts, total amounts, and average amounts.

Reports are written to `data/processed/` and can be opened in Excel.
