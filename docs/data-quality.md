\# Data Quality and Reconciliation



\## Validation Rules



| Rule | Expected behavior |

|---|---|

| Transaction ID | Reject missing IDs |

| Customer ID | Reject missing IDs |

| Transaction type | Validate permitted transaction types |

| Amount | Reject invalid amounts |

| Timestamp | Reject invalid timestamps |

| Transaction status | Validate permitted statuses |

| Duplicate transactions | Deduplicate using transaction ID |

| Customer reference | Quarantine transactions referencing unknown customers |



\## Pipeline Results



| Metric | Count |

|---|---:|

| Raw transaction rows | 10,004 |

| Rows after deduplication | 10,003 |

| Valid transactions | 10,000 |

| Quarantined records | 3 |



\## Quarantine Breakdown



| Rejection reason | Count |

|---|---:|

| INVALID\_AMOUNT | 1 |

| MISSING\_CUSTOMER\_ID | 1 |

| UNKNOWN\_CUSTOMER\_ID | 1 |



\## Reconciliation



Raw rows after deduplication are divided into valid and quarantined records:



10,003 = 10,000 + 3



The reconciliation check passed.



\## Output Locations



\- Valid transactions: `data/processed/transactions.parquet`

\- Processed customers: `data/processed/customers.parquet`

\- Rejected transactions: `data/quarantine/transactions.parquet`

