/*
===============================================================================
Purpose: Transform raw product data from the Bronze Layer to the Silver Layer 
          - Cleansing & Standardization
          - Data Type Corrections
          - Handling NULLs & Invalid Values
===============================================================================
*/

/*
===============================================================================
Bronze to Silver Transformation: silver_crm_cust_info Table
===============================================================================
*/

PRINT '>> Truncating Table: silver_crm_cust_info';
TRUNCATE TABLE silver_crm_cust_info;

PRINT '>> Inserting Data Into: silver_crm_cust_info';
INSERT INTO silver_crm_cust_info (
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
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE 
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'n/a'
    END AS cst_marital_status,
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n/a'
    END AS cst_gndr,
    CAST(cst_create_date AS DATE) AS cst_create_date
FROM bronze_crm_cust_info;

/*
===============================================================================
Bronze to Silver Transformation: silver_crm_prd_info Table
===============================================================================
*/

PRINT '>> Truncating Table: silver_crm_prd_info';
TRUNCATE TABLE silver_crm_prd_info;

PRINT '>> Inserting Data Into: silver_crm_prd_info';
INSERT INTO silver_crm_prd_info (
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
    REPLACE(SUBSTRING(prd_key, 1, 5), '-.', '_') AS cat_id,
    SUBSTRING(prd_key, 7, CHAR_LENGTH(prd_key)) AS prd_key,
    prd_nm,
    IFNULL(prd_cost, 0) AS prd_cost,
    CASE 
        WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
        WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
        WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
        WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
        ELSE 'Unknown'
    END AS prd_line,
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL 1 DAY AS DATE) AS prd_end_dt
FROM bronze_crm_prd_info;

/*
===============================================================================
Bronze to Silver Transformation: silver_crm_sales_details Table
===============================================================================
*/

PRINT '>> Truncating Table: silver_crm_sales_details';
TRUNCATE TABLE silver_crm_sales_details;

PRINT '>> Inserting Data Into: silver_crm_sales_details';
INSERT INTO silver_crm_sales_details (
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
        WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
        ELSE STR_TO_DATE(sls_order_dt, '%Y%m%d')
    END AS sls_order_dt,
    CASE 
        WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL
        ELSE STR_TO_DATE(sls_ship_dt, '%Y%m%d')
    END AS sls_ship_dt,
    CASE 
        WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) != 8 THEN NULL
        ELSE STR_TO_DATE(sls_due_dt, '%Y%m%d')
    END AS sls_due_dt,
    CASE 
        WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,
    sls_quantity,
    CASE 
        WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
    END AS sls_price
FROM bronze_crm_sales_details;

/*
===============================================================================
Bronze to Silver Transformation: silver_erp_cust_az12 Table
===============================================================================
*/

PRINT '>> Truncating Table: silver_erp_cust_az12';
TRUNCATE TABLE silver_erp_cust_az12;

PRINT '>> Inserting Data Into: silver_erp_cust_az12';
INSERT INTO silver_erp_cust_az12 (cid, bdate, gen)
SELECT
    CASE 
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid)) 
        ELSE cid 
    END AS cid,
    CASE 
        WHEN bdate > CURRENT_DATE() THEN NULL 
        ELSE bdate 
    END AS bdate,
    CASE 
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
        ELSE 'n/a'
    END AS gen
FROM bronze_erp_cust_az12;

/*
===============================================================================
Bronze to Silver Transformation: silver_erp_loc_a101 Table
===============================================================================
*/

PRINT '>> Truncating Table: silver_erp_loc_a101';
TRUNCATE TABLE silver_erp_loc_a101;

PRINT '>> Inserting Data Into: silver_erp_loc_a101';
INSERT INTO silver_erp_loc_a101 (cid, cntry)
SELECT
    REPLACE(cid, '-', '') AS cid,
    CASE 
        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
        ELSE TRIM(cntry)
    END AS cntry
FROM bronze_erp_loc_a101;

/*
===============================================================================
Bronze to Silver Transformation: silver_erp_px_cat_g1v2 Table
===============================================================================
*/

PRINT '>> Truncating Table: silver_erp_px_cat_g1v2';
TRUNCATE TABLE silver_erp_px_cat_g1v2;

PRINT '>> Inserting Data Into: silver_erp_px_cat_g1v2';
INSERT INTO silver_erp_px_cat_g1v2 (id, cat, subcat, maintenance)
SELECT id, cat, subcat, maintenance
FROM bronze_erp_px_cat_g1v2;
