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

-- crm_sales_details
-- Check for Invalid Dates
SELECT
    NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM
    bronze.crm_sales_details
WHERE
    sls_order_dt <= 0
    OR LENGTH (CAST(sls_order_dt AS VARCHAR)) != 8
    OR sls_order_dt > CAST(TO_CHAR (CURRENT_DATE, 'YYYYMMDD') AS INT);

-- Sale order date should not be greater than ship date and due date
SELECT
    *
FROM
    bronze.crm_sales_details
WHERE
    sls_order_dt > sls_ship_dt
    OR sls_order_dt > sls_due_dt
    --Sales = Quantity * Price
SELECT DISTINCT
    sls_sales AS old_sls_sales,
    sls_quantity,
    sls_price AS old_sls_price,
    CASE
        WHEN sls_price IS NULL
        OR sls_price <= 0
        OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
        ELSE sls_price
    END AS sls_sales,
    CASE
        WHEN sls_price IS NULL
        or sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
    END AS sls_price
FROM
    bronze.crm_sales_details
WHERE
    sls_sales != sls_quantity * sls_price
    OR sls_sales IS NULL
    OR sls_quantity IS NULL
    OR sls_price IS NULL
    OR sls_sales <= 0
    OR sls_quantity <= 0
    OR sls_price <= 0
ORDER BY
    sls_sales,
    sls_quantity,
    sls_price DESC;

-- erp_cust_az12
-- Data Standardization & Consistency
SELECT
    CASE
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH (cid))
        ELSE cid
    END cid,
    bdate,
    gen
FROM
    bronze.erp_cust_az12
WHERE
    cid NOT IN (
        SELECT DISTINCT
            cst_key
        FROM
            silver.crm_cust_info
    );

-- Check for Invalid Dates
SELECT
    bdate
FROM
    bronze.erp_cust_az12
WHERE
    bdate < '1924-01-01'
    AND bdate > CURRENT_DATE()
    -- Data Standardization & Consistency
SELECT DISTINCT
    gen,
    CASE
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
        ELSE 'n/a'
    END AS gen
FROM
    bronze.erp_cust_az12;

-- erp_loc_a101
-- Data Standardization & Consistency
SELECT DISTINCT
    cntry
    --REPLACE (cid, '-', '') cid,
    CASE
        WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
        WHEN UPPER(TRIM(cntry)) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(cntry) = ''
        OR cntry IS NULL THEN 'n/a'
        ELSE TRIM(cntry)
    END AS cntry
FROM
    bronze.erp_loc_a101


-- erp_px_cat_g1v2
SELECT
    id,
    cat,
    subcat,
    maintenance
FROM
    bronze.erp_px_cat_g1v2

-- Check Unwanted Spaces
SELECT
    id,
    cat,
    subcat,
    maintenance
FROM
    bronze.erp_px_cat_g1v2
WHERE
    cat != TRIM(cat)
    OR subcat != TRIM(subcat)
    OR maintenance != TRIM(maintenance)

-- Data Standardization & Consistency
SELECT DISTINCT
    cat
FROM
    bronze.erp_px_cat_g1v2