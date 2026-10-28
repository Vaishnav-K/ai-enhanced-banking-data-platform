-- AWS Athena validation for the synthetic banking platform.
-- Replace YOUR-BUCKET with an S3 bucket you control.
-- Ensure the curated Parquet data already exists at the locations below.
-- Configure Athena query results to an S3 prefix such as:
-- s3://YOUR-BUCKET/banking-data/athena-results/
--
-- Database creation (run once if needed):
CREATE DATABASE IF NOT EXISTS banking_analytics;

-- External tables over existing curated Parquet data.
-- If a table already exists with an incorrect schema, IF NOT EXISTS will not
-- alter it. Use a new table name or deliberately manage the existing Glue
-- table definition rather than assuming this statement updates it.
CREATE EXTERNAL TABLE IF NOT EXISTS banking_analytics.customers_v2 (
    customer_id STRING,
    customer_name STRING,
    account_type STRING,
    registration_date DATE
)
STORED AS PARQUET
LOCATION 's3://YOUR-BUCKET/banking-data/curated/customers/';

CREATE EXTERNAL TABLE IF NOT EXISTS banking_analytics.transactions_v2 (
    transaction_id STRING,
    customer_id STRING,
    transaction_type STRING,
    amount DECIMAL(18,2),
    transaction_timestamp TIMESTAMP,
    status STRING
)
STORED AS PARQUET
LOCATION 's3://YOUR-BUCKET/banking-data/curated/transactions/';

-- 1. Customer row count.
SELECT COUNT(*) AS total_customers
FROM banking_analytics.customers_v2;

-- 2. Transaction count, unique IDs, customers, and amount total.
SELECT
    COUNT(*) AS total_transactions,
    COUNT(DISTINCT transaction_id) AS unique_transaction_ids,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(amount) AS total_transaction_amount
FROM banking_analytics.transactions_v2;

-- 3. Status distribution and percentage.
SELECT
    status,
    COUNT(*) AS transaction_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM banking_analytics.transactions_v2
GROUP BY status
ORDER BY transaction_count DESC;

-- 4. Cross-table analytics by customer account type.
SELECT
    c.account_type,
    COUNT(t.transaction_id) AS transaction_count,
    COUNT(DISTINCT c.customer_id) AS customers_with_transactions,
    SUM(t.amount) AS total_transaction_amount,
    SUM(CASE WHEN t.status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_transactions,
    SUM(CASE WHEN t.status = 'FAILED' THEN 1 ELSE 0 END) AS failed_transactions
FROM banking_analytics.customers_v2 c
INNER JOIN banking_analytics.transactions_v2 t
    ON c.customer_id = t.customer_id
GROUP BY c.account_type
ORDER BY total_transaction_amount DESC;

-- 5. Selected data-quality checks: null IDs, null amount, negative amount,
-- and transaction customer IDs that do not exist in the customer table.
SELECT
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN t.transaction_id IS NULL THEN 1 ELSE 0 END) AS null_transaction_ids,
    SUM(CASE WHEN t.customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_ids,
    SUM(CASE WHEN t.amount IS NULL THEN 1 ELSE 0 END) AS null_amounts,
    SUM(CASE WHEN t.amount < 0 THEN 1 ELSE 0 END) AS negative_amounts,
    SUM(CASE WHEN c.customer_id IS NULL THEN 1 ELSE 0 END) AS orphan_transactions
FROM banking_analytics.transactions_v2 t
LEFT JOIN banking_analytics.customers_v2 c
    ON t.customer_id = c.customer_id;

-- Note: This script does not create, upload, or modify Parquet data.
-- It registers external tables and runs read-only SELECT queries.
