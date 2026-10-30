"""Shared transaction-domain rules used by DuckDB and Spark ETL.

Keep these values in one place so both execution engines enforce the same
business contract. Values are normalized with trim + uppercase before checks.
"""

TRANSACTION_TYPES = ("CREDIT", "DEBIT", "TRANSFER", "PAYMENT")
TRANSACTION_STATUSES = ("SUCCESS", "FAILED", "PENDING")

# A stable total ordering is used when duplicate transaction IDs contain
# conflicting records. The lexicographically smallest normalized record wins.
DEDUPLICATION_COLUMNS = (
    "transaction_id",
    "customer_id",
    "transaction_type",
    "amount",
    "transaction_timestamp",
    "status",
)

REJECTION_REASONS = (
    "MISSING_TRANSACTION_ID",
    "MISSING_CUSTOMER_ID",
    "INVALID_TRANSACTION_TYPE",
    "INVALID_AMOUNT",
    "INVALID_TIMESTAMP",
    "INVALID_STATUS",
    "UNKNOWN_CUSTOMER_ID",
)
