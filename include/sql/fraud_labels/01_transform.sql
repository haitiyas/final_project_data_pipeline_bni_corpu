-- Transform: stg_customers → dim_customers
-- Cast tipe data, tambah derived columns, deduplikasi

TRUNCATE TABLE dim_fraud_labels;

INSERT INTO dim_fraud_labels (
    transaction_id,
    transaction_code,
    is_fraud,
    fraud_type,
    fraud_score,
    flagged_at
)
SELECT DISTINCT ON (transaction_id)
    transaction_id,
    transaction_code,
    is_fraud::BOOLEAN,
    fraud_type,
    fraud_score::NUMERIC(5,4),
    flagged_at::TIMESTAMP
FROM stg_fraud_labels
WHERE transaction_id IS NOT NULL
ORDER BY transaction_id;
