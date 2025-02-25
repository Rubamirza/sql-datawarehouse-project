
DELIMITER //

DROP PROCEDURE IF EXISTS load_silver;

CREATE PROCEDURE load_silver()
BEGIN
    -- Declare variables to track execution time
    DECLARE batch_start_time DATETIME;
    DECLARE batch_end_time DATETIME;

    -- Start batch processing
    SET batch_start_time = NOW();
    SELECT '==========================================' AS Message;
    SELECT 'Starting Truncation of Silver Tables' AS Message;
    SELECT '==========================================' AS Message;

    -- Truncate all Silver tables before loading new data
    TRUNCATE TABLE silver_crm_cust_info;
    SELECT 'Truncated: silver_crm_cust_info' AS Message;

    TRUNCATE TABLE silver_crm_prd_info;
    SELECT 'Truncated: silver_crm_prd_info' AS Message;

    TRUNCATE TABLE silver_crm_sales_details;
    SELECT 'Truncated: silver_crm_sales_details' AS Message;

    TRUNCATE TABLE silver_erp_loc_a101;
    SELECT 'Truncated: silver_erp_loc_a101' AS Message;

    TRUNCATE TABLE silver_erp_cust_az12;
    SELECT 'Truncated: silver_erp_cust_az12' AS Message;

    TRUNCATE TABLE silver_erp_px_cat_g1v2;
    SELECT 'Truncated: silver_erp_px_cat_g1v2' AS Message;

    -- Begin data transformation and insertion into Silver tables
    SELECT '==========================================' AS Message;
    SELECT 'Starting Data Load Into Silver Tables' AS Message;
    SELECT '==========================================' AS Message;

    -- Load Data Into silver_crm_cust_info
    SET @start_time = NOW();
    INSERT INTO silver_crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
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
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END,
        CAST(cst_create_date AS DATE)
    FROM bronze_crm_cust_info;
    SET @end_time = NOW();
    SELECT CONCAT('>> Load Duration (silver_crm_cust_info): ', TIMESTAMPDIFF(SECOND, @start_time, @end_time), ' seconds') AS Message;

    -- Load Data Into silver_crm_prd_info
    SET @start_time = NOW();
    INSERT INTO silver_crm_prd_info (prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
    SELECT 
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-.', '_'),
        SUBSTRING(prd_key, 7, CHAR_LENGTH(prd_key)),
        prd_nm,
        IFNULL(prd_cost, 0),
        CASE 
            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'Unknown'
        END,
        CAST(prd_start_dt AS DATE),
        CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL 1 DAY AS DATE)
    FROM bronze_crm_prd_info;
    SET @end_time = NOW();
    SELECT CONCAT('>> Load Duration (silver_crm_prd_info): ', TIMESTAMPDIFF(SECOND, @start_time, @end_time), ' seconds') AS Message;

    -- Load Data Into silver_crm_sales_details
    SET @start_time = NOW();
    INSERT INTO silver_crm_sales_details (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
    SELECT 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        STR_TO_DATE(sls_order_dt, '%Y%m%d'),
        STR_TO_DATE(sls_ship_dt, '%Y%m%d'),
        STR_TO_DATE(sls_due_dt, '%Y%m%d'),
        sls_sales,
        sls_quantity,
        sls_price
    FROM bronze_crm_sales_details;
    SET @end_time = NOW();
    SELECT CONCAT('>> Load Duration (silver_crm_sales_details): ', TIMESTAMPDIFF(SECOND, @start_time, @end_time), ' seconds') AS Message;

    -- Load Data Into silver_erp_loc_a101
    SET @start_time = NOW();
    INSERT INTO silver_erp_loc_a101 (cid, cntry)
    SELECT
        REPLACE(cid, '-', ''),
        CASE 
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(cntry)
        END
    FROM bronze_erp_loc_a101;
    SET @end_time = NOW();
    SELECT CONCAT('>> Load Duration (silver_erp_loc_a101): ', TIMESTAMPDIFF(SECOND, @start_time, @end_time), ' seconds') AS Message;

    -- Load Data Into silver_erp_cust_az12
    SET @start_time = NOW();
    INSERT INTO silver_erp_cust_az12 (cid, bdate, gen)
    SELECT
        CASE 
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid)) 
            ELSE cid 
        END,
        CASE 
            WHEN bdate > CURRENT_DATE() THEN NULL 
            ELSE bdate 
        END,
        CASE 
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'n/a'
        END
    FROM bronze_erp_cust_az12;
    SET @end_time = NOW();
    SELECT CONCAT('>> Load Duration (silver_erp_cust_az12): ', TIMESTAMPDIFF(SECOND, @start_time, @end_time), ' seconds') AS Message;

    -- Load Data Into silver_erp_px_cat_g1v2
    SET @start_time = NOW();
    INSERT INTO silver_erp_px_cat_g1v2 (id, cat, subcat, maintenance)
    SELECT id, cat, subcat, maintenance
    FROM bronze_erp_px_cat_g1v2;
    SET @end_time = NOW();
    SELECT CONCAT('>> Load Duration (silver_erp_px_cat_g1v2): ', TIMESTAMPDIFF(SECOND, @start_time, @end_time), ' seconds') AS Message;

    -- Final Execution Summary
    SET batch_end_time = NOW();
    SELECT '==========================================' AS Message;
    SELECT 'Silver Layer Load Completed' AS Message;
    SELECT CONCAT('   - Total Execution Time: ', TIMESTAMPDIFF(SECOND, batch_start_time, batch_end_time), ' seconds') AS Message;
    SELECT '==========================================' AS Message;

END //

DELIMITER ;

