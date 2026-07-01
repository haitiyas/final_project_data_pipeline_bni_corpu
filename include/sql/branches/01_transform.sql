TRUNCATE TABLE dim_branches;

INSERT INTO dim_branches (

    branch_id,
    branch_code,
    branch_name,
    city,
    province,
    region,
    branch_type,
    open_date,
    is_active
)

SELECT DISTINCT ON (branch_id)
    branch_id::SMALLINT,
    branch_code,
    branch_name,
    city,
    province,
    region,
    branch_type,
    open_date::DATE,
    is_active
FROM stg_branches
WHERE branch_id IS NOT NULL
ORDER BY branch_id;