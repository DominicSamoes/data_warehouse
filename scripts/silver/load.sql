CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_ts   TIMESTAMP;
    v_step_ts    TIMESTAMP;
    v_rows       BIGINT;
    v_total_rows BIGINT := 0;
BEGIN
    v_start_ts := clock_timestamp();

    RAISE NOTICE '=================================================';
    RAISE NOTICE 'Starting SILVER load at %', v_start_ts;
    RAISE NOTICE '=================================================';

    /* ============================================================
       crm_cust_info
    ============================================================ */
    v_step_ts := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.crm_cust_info';
    TRUNCATE TABLE silver.crm_cust_info;

    RAISE NOTICE 'crm_cust_info truncated in % ms',
        EXTRACT(MILLISECOND FROM clock_timestamp() - v_step_ts);

    v_step_ts := clock_timestamp();
    RAISE NOTICE '>> Inserting CRM Customer Data';

    INSERT INTO silver.crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date
    )
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname),
        TRIM(cst_lastname),
        CASE
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END,
        CASE
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            ELSE 'n/a'
        END,
        cst_create_date
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY cst_id
                   ORDER BY cst_create_date DESC
               ) AS flag_last
        FROM bronze.crm_cust_info
    ) t
    WHERE flag_last = 1
      AND cst_id IS NOT NULL;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    v_total_rows := v_total_rows + v_rows;

    RAISE NOTICE 'crm_cust_info loaded: % rows in % ms',
        v_rows,
        EXTRACT(MILLISECOND FROM clock_timestamp() - v_step_ts);

    /* ============================================================
       crm_prd_info
    ============================================================ */
    v_step_ts := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info;

    RAISE NOTICE 'crm_prd_info truncated in % ms',
        EXTRACT(MILLISECOND FROM clock_timestamp() - v_step_ts);

    v_step_ts := clock_timestamp();
    RAISE NOTICE '>> Inserting CRM Product Data';

    INSERT INTO silver.crm_prd_info (
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'),
        SUBSTRING(prd_key, 7),
        prd_nm,
        COALESCE(prd_cost, 0),
        COALESCE(
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
            END,
            'n/a'
        ),
        prd_start_dt::date,
        (LEAD(prd_start_dt) OVER (
            PARTITION BY prd_key
            ORDER BY prd_start_dt
        ) - INTERVAL '1 day')::date
    FROM bronze.crm_prd_info;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    v_total_rows := v_total_rows + v_rows;

    RAISE NOTICE 'crm_prd_info loaded: % rows in % ms',
        v_rows,
        EXTRACT(MILLISECOND FROM clock_timestamp() - v_step_ts);

    /* ============================================================
       crm_sales_details
    ============================================================ */
    v_step_ts := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details;

    RAISE NOTICE 'crm_sales_details truncated in % ms',
        EXTRACT(MILLISECOND FROM clock_timestamp() - v_step_ts);

    v_step_ts := clock_timestamp();
    RAISE NOTICE '>> Inserting CRM Sales Data';

    INSERT INTO silver.crm_sales_details (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE
            WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::text) != 8 THEN NULL
            ELSE sls_order_dt::text::date
        END,
        CASE
            WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::text) != 8 THEN NULL
            ELSE sls_ship_dt::text::date
        END,
        CASE
            WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::text) != 8 THEN NULL
            ELSE sls_due_dt::text::date
        END,
        CASE
            WHEN sls_price IS NULL
              OR sls_price <= 0
              OR sls_sales != sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
            ELSE sls_price
        END,
        sls_quantity,
        CASE
            WHEN sls_price IS NULL OR sls_price <= 0
            THEN sls_sales / NULLIF(sls_quantity, 0)
            ELSE sls_price
        END
    FROM bronze.crm_sales_details;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    v_total_rows := v_total_rows + v_rows;

    RAISE NOTICE 'crm_sales_details loaded: % rows in % ms',
        v_rows,
        EXTRACT(MILLISECOND FROM clock_timestamp() - v_step_ts);

    /* ============================================================
       erp_cust_az12
    ============================================================ */
    v_step_ts := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.erp_cust_az12';
    TRUNCATE TABLE silver.erp_cust_az12;

    v_step_ts := clock_timestamp();
    RAISE NOTICE '>> Inserting ERP Customer Data';

    INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
    SELECT
        CASE
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4)
            ELSE cid
        END,
        CASE
            WHEN bdate > CURRENT_DATE THEN NULL
            ELSE bdate
        END,
        CASE
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'n/a'
        END
    FROM bronze.erp_cust_az12;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    v_total_rows := v_total_rows + v_rows;

    RAISE NOTICE 'erp_cust_az12 loaded: % rows in % ms',
        v_rows,
        EXTRACT(MILLISECOND FROM clock_timestamp() - v_step_ts);

    /* ============================================================
       erp_loc_a101
    ============================================================ */
    v_step_ts := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.erp_loc_a101';
    TRUNCATE TABLE silver.erp_loc_a101;

    v_step_ts := clock_timestamp();
    RAISE NOTICE '>> Inserting ERP Location Data';

    INSERT INTO silver.erp_loc_a101 (cid, cntry)
    SELECT
        REPLACE(cid, '-', ''),
        CASE
            WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
            WHEN UPPER(TRIM(cntry)) IN ('US', 'USA') THEN 'United States'
            WHEN cntry IS NULL OR TRIM(cntry) = '' THEN 'n/a'
            ELSE TRIM(cntry)
        END
    FROM bronze.erp_loc_a101;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    v_total_rows := v_total_rows + v_rows;

    RAISE NOTICE 'erp_loc_a101 loaded: % rows in % ms',
        v_rows,
        EXTRACT(MILLISECOND FROM clock_timestamp() - v_step_ts);

    /* ============================================================
       erp_px_cat_g1v2
    ============================================================ */
    v_step_ts := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.erp_px_cat_g1v2';
    TRUNCATE TABLE silver.erp_px_cat_g1v2;

    v_step_ts := clock_timestamp();
    RAISE NOTICE '>> Inserting ERP Category Data';

    INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
    SELECT id, cat, subcat, maintenance
    FROM bronze.erp_px_cat_g1v2;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    v_total_rows := v_total_rows + v_rows;

    RAISE NOTICE 'erp_px_cat_g1v2 loaded: % rows in % ms',
        v_rows,
        EXTRACT(MILLISECOND FROM clock_timestamp() - v_step_ts);

    /* ============================================================
       FINAL STATS
    ============================================================ */
    RAISE NOTICE '=================================================';
    RAISE NOTICE 'SILVER load completed successfully';
    RAISE NOTICE 'Total rows loaded: %', v_total_rows;
    RAISE NOTICE 'Total runtime: % ms',
        EXTRACT(MILLISECOND FROM clock_timestamp() - v_start_ts);
    RAISE NOTICE '=================================================';

END;
$$;