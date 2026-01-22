DELIMITER $$

DROP PROCEDURE IF EXISTS bronze.load_bronze $$
CREATE PROCEDURE bronze.load_bronze()
BEGIN
    DECLARE start_time DATETIME;
    DECLARE end_time DATETIME;
    DECLARE batch_start_time DATETIME;
    DECLARE batch_end_time DATETIME;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SELECT '==========================================' AS msg;
        SELECT 'ERROR OCCURRED DURING LOADING BRONZE LAYER' AS msg;
        SELECT '==========================================' AS msg;
    END;

    SET batch_start_time = NOW();

    SELECT '================================================' AS msg;
    SELECT 'Loading Bronze Layer' AS msg;
    SELECT '================================================' AS msg;

    -- CRM CUSTOMER INFO
    SET start_time = NOW();
    TRUNCATE TABLE bronze.crm_cust_info;

    LOAD DATA LOCAL INFILE '/sql/dwh_project/datasets/source_crm/cust_info.csv'
    INTO TABLE bronze.crm_cust_info
    FIELDS TERMINATED BY ','
    IGNORE 1 ROWS;

    SET end_time = NOW();
    SELECT CONCAT(
        'crm_cust_info load duration: ',
        TIMESTAMPDIFF(SECOND, start_time, end_time),
        ' seconds'
    ) AS msg;

    -- CRM PRODUCT INFO
    SET start_time = NOW();
    TRUNCATE TABLE bronze.crm_prd_info;

    LOAD DATA LOCAL INFILE '/sql/dwh_project/datasets/source_crm/prd_info.csv'
    INTO TABLE bronze.crm_prd_info
    FIELDS TERMINATED BY ','
    IGNORE 1 ROWS;

    SET end_time = NOW();
    SELECT CONCAT(
        'crm_prd_info load duration: ',
        TIMESTAMPDIFF(SECOND, start_time, end_time),
        ' seconds'
    ) AS msg;

    -- CRM SALES DETAILS
    SET start_time = NOW();
    TRUNCATE TABLE bronze.crm_sales_details;

    LOAD DATA LOCAL INFILE '/sql/dwh_project/datasets/source_crm/sales_details.csv'
    INTO TABLE bronze.crm_sales_details
    FIELDS TERMINATED BY ','
    IGNORE 1 ROWS;

    SET end_time = NOW();
    SELECT CONCAT(
        'crm_sales_details load duration: ',
        TIMESTAMPDIFF(SECOND, start_time, end_time),
        ' seconds'
    ) AS msg;

    -- ERP TABLES
    SET start_time = NOW();
    TRUNCATE TABLE bronze.erp_loc_a101;

    LOAD DATA LOCAL INFILE '/sql/dwh_project/datasets/source_erp/loc_a101.csv'
    INTO TABLE bronze.erp_loc_a101
    FIELDS TERMINATED BY ','
    IGNORE 1 ROWS;

    SET end_time = NOW();
    SELECT CONCAT(
        'erp_loc_a101 load duration: ',
        TIMESTAMPDIFF(SECOND, start_time, end_time),
        ' seconds'
    ) AS msg;

    SET batch_end_time = NOW();
    SELECT CONCAT(
        'Total Load Duration: ',
        TIMESTAMPDIFF(SECOND, batch_start_time, batch_end_time),
        ' seconds'
    ) AS msg;

END $$

DELIMITER ;