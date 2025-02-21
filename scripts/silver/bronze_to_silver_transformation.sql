/*
===============================================================================
Purpose: Transform raw product data from the Bronze Layer to the Silver Layer 
          - Cleansing & Standardization
          - Data Type Corrections
          - Handling NULLs & Invalid Values
===============================================================================
Bronze to Silver Transformation: silver_crm_cust_info Table
===============================================================================
*/
-- =============================================================================
--  Step 2: Transform & Load Data for silver_crm_cust_info (Customer Information)
-- =============================================================================

--  WHY ARE WE USING A TEMP TABLE HERE? 
-- We must filter out duplicate `cst_id` records and retain only the latest entry per customer (based on `cst_create_date`).
-- Since MySQL does not allow `WITH CTE` inside `INSERT INTO`, we use a TEMP TABLE 
-- as a workaround to first filter the data before inserting it into the silver table.

CREATE TEMPORARY TABLE temp_cte_result AS
WITH cte_cust AS (
    SELECT 
        cst_id,
        cst_key,
        --  Trim whitespace from first & last names
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname) AS cst_lastname,

        --  Standardize Marital Status
        CASE 
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'n/a'  
        END AS cst_marital_status,

        -- Standardize Gender
        CASE 
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'  
        END AS cst_gndr,

        --  Ensure date formatting
        CAST(cst_create_date AS DATE) AS cst_create_date,

        --  Assign row numbers to filter out duplicate customer IDs (keep latest)
        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last

    FROM bronze_crm_cust_info
    WHERE cst_id IS NOT NULL
)
--  Select only the latest record per customer
SELECT * FROM cte_cust WHERE flag_last = 1;




 /*
===============================================================================
Bronze to Silver Transformation: silver_crm_prd_info Table
===============================================================================
*/

--  Insert transformed data into Silver Table
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
    -- Extract & Transform Category ID
    REPLACE(SUBSTRING(prd_key, 1, 5), '-.', '_') AS cat_id,
    
    -- Extract Product Key after the 7th character
    SUBSTRING(prd_key, 7, CHAR_LENGTH(prd_key)) AS prd_key,
    
    prd_nm,
    
    -- Handle NULLs: If prd_cost is NULL, replace with 0
    IFNULL(prd_cost, 0) AS prd_cost,

    -- Standardize prd_line values
    CASE 
        WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
        WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
        WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
        WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
        ELSE 'Unknown'
    END AS prd_line,

    -- Convert prd_start_dt to DATE format
    CAST(prd_start_dt AS DATE) AS prd_start_dt,

    -- Use LEAD() to get the next start date and subtract 1 day for prd_end_dt
    CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL 1 DAY AS DATE) AS prd_end_dt

FROM bronze_crm_prd_info;
