/*
===============================================================================
Purpose : Transform raw product data from Bronze Layer to Silver Layer 
          - Cleansing & Standardization
          - Data Type Corrections
          - Handling NULLs & Invalid Values
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
