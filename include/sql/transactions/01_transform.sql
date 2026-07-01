TRUNCATE TABLE fact_transactions;

INSERT INTO fact_transactions (
    transaction_id,
    transaction_code,
    account_id,
    customer_id,
    branch_id,
    channel_id,
    transaction_date,
    transaction_at,
    transaction_type,
    amount,
    balance_before,
    balance_after,
    status,
    reference_no
)

SELECT DISTINCT ON (transaction_id)
    transaction_id::BIGINT,
    transaction_code,
    account_id::INTEGER,
    customer_id::INTEGER,
    branch_id::INTEGER,
    channel_id::SMALLINT,
    transaction_date::DATE,
    transaction_at::TIMESTAMP,
    transaction_type,
    amount::NUMERIC(18,2),
    balance_before::NUMERIC(18,2),
    balance_after::NUMERIC(18,2),
    status,
    reference_no
FROM stg_transactions
WHERE transaction_id IS NOT NULL
ORDER BY transaction_id;