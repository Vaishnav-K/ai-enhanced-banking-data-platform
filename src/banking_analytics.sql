-- 1. Daily transaction volume and value
SELECT
    CAST(transaction_timestamp AS DATE) AS transaction_date,
    COUNT(*) AS total_transactions,
    SUM(amount) AS total_transaction_amount,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_transactions,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_transactions
FROM read_parquet('data/processed/transactions.parquet')
GROUP BY 1
ORDER BY 1;


-- 2. Transaction status summary
SELECT
    status,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_amount
FROM read_parquet('data/processed/transactions.parquet')
GROUP BY status
ORDER BY transaction_count DESC;


-- 3. Customer transaction summary
SELECT
    c.customer_id,
    c.customer_name,
    c.account_type,
    COUNT(t.transaction_id) AS transaction_count,
    COALESCE(SUM(t.amount), 0) AS total_transaction_amount
FROM read_parquet('data/processed/customers.parquet') c
LEFT JOIN read_parquet('data/processed/transactions.parquet') t
    ON c.customer_id = t.customer_id
GROUP BY c.customer_id, c.customer_name, c.account_type
ORDER BY total_transaction_amount DESC
LIMIT 20;