-- crm_cust_info
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Result
SELECT
    cst_id,
    COUNT(*) AS cnt
FROM
    silver.crm_cust_info
GROUP BY
    cst_id
HAVING
    COUNT(*) > 1
    OR cst_id IS NULL;

-- Transform and Clean
--Window Function to get the latest record based on cst_create_date
SELECT
    *,
    ROW_NUMBER() OVER (
        PARTITION BY
            cst_id
        ORDER BY
            cst_create_date DESC
    ) AS flag_last
FROM
    silver.crm_cust_info
WHERE
    cst_id = 29466;

-- Query by latest record only
SELECT
    *
FROM
    (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY
                    cst_id
                ORDER BY
                    cst_create_date DESC
            ) AS flag_last
        FROM
            silver.crm_cust_info
    )
WHERE
    flag_last = 1;

-- Check for unwanted spaces
-- Expectation: No Result
SELECT
    cst_lastname
FROM
    silver.crm_cust_info
WHERE
    cst_lastname != TRIM(cst_lastname);

-- Data Standardization & Consistency
SELECT DISTINCT
    cst_gndr
FROM
    silver.crm_cust_info;

SELECT DISTINCT
    cst_marital_status
FROM
    silver.crm_cust_info;

-- crm_prd_info
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Result
SELECT
    prd_id,
    COUNT(*) AS cnt
FROM
    bronze.crm_prd_info
GROUP BY
    prd_id
HAVING
    COUNT(*) > 1
    OR prd_id IS NULL;

-- Check for unwanted spaces
-- Expectation: No Result
SELECT
    prd_nm
FROM
    bronze.crm_prd_info
WHERE
    prd_nm != TRIM(prd_nm);

-- Check for negative and null prices
-- Expectation: No Result
SELECT
    *
FROM
    bronze.crm_prd_info
WHERE
    prd_cost < 0
    OR prd_cost IS NULL;

-- Fix Dates
SELECT
    prd_id,
    prd_key,
    prd_nm,
    prd_start_dt,
    prd_end_dt,
    LEAD (prd_start_dt) OVER (
        PARTITION BY
            prd_key
        ORDER BY
            prd_start_dt
    ) - INTERVAL '1 day' AS next_prd_start_dt
FROM
    bronze.crm_prd_info
WHERE
    prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')
    -- Data Standardization & Consistency
SELECT
    prd_id,
    REPLACE (SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7, LENGTH (prd_key)) AS prd_key,
    prd_nm,
    COALESCE(prd_cost, 0) AS prd_cost,
    COALESCE(
        CASE UPPER(TRIM(prd_line))
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
        END,
        'n/a'
    ) AS prd_line,
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    CAST(
        LEAD (prd_start_dt) OVER (
            PARTITION BY
                prd_key
            ORDER BY
                prd_start_dt
        ) - INTERVAL '1 day' AS DATE
    ) AS prd_end_dt
FROM
    bronze.crm_prd_info;