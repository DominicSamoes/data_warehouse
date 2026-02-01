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
