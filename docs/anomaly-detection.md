# Transaction Anomaly Detection

## Purpose
Identify potentially unusual transactions in the processed banking dataset.

## Current Rules
- High-value transaction: amount greater than or equal to 100,000.
- Non-positive amount: amount less than or equal to zero.

## Output
Results are written to `data/processed/transaction_anomalies.json`.

## Current Sample Result
The current dataset produced zero matches for these two rules. This means no records met the configured thresholds; it does not establish that the dataset contains no other anomalies.

## Limitations
These are threshold-based screening rules, not a trained machine-learning model. Thresholds should be calibrated to the transaction distribution and business requirements.
