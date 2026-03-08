-- CUSTOMER

--- Cleanup and Standardization of Customer Gender
    SELECT DISTINCT
        ci.cst_gndr,
        ca.gen,
        CASE
            WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the Master for Customer gender Info
            ELSE COALESCE(ca.gen, 'n/a')
        END AS new_gen
    FROM
        silver.crm_cust_info ci
        LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
        LEFT JOIN silver.erp_loc_a101 la ON ci.cst_key = la.cid
    ORDER BY
        1,
        2

-- PRODUCT

--- Check for Duplicate Product Keys 
SELECT prd_key, COUNT(*) FROM(
SELECT
    pn.prd_id,
    pn.cat_id,
    pn.prd_key,
    pn.prd_nm,
    pn.prd_cost,
    pn.prd_line,
    pn.prd_start_dt,
    pc.cat,
    pc.subcat,
    pc.maintenance
FROM
    silver.crm_prd_info pn
    LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE    pn.prd_end_dt IS NULL --Filter out all historical records and keep only the current version of the product information
)t GROUP BY prd_key
HAVING COUNT(*) > 1

--- Sort the columns into logical groups
SELECT
    pn.prd_id AS product_id,
    pn.prd_key AS product_number,
    pn.prd_nm AS product_name,
    pn.cat_id AS category_id,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pc.maintenance,
    pn.prd_cost AS cost,
    pn.prd_line AS product_line,
    pn.prd_start_dt AS start_date
FROM
    silver.crm_prd_info pn
    LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id


-- Foreign Key Integrity (Dimensions and Facts)
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON f.customer_key = c.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL
