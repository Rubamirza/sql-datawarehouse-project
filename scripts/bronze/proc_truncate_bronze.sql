
/*
===============================================================================
Stored Procedure: Truncate Bronze Tables
===============================================================================
Purpose:
    - Clears all data from Bronze tables before loading new data.
    - Ensures no duplicate records.
===============================================================================
How to Use:
    1Run:
      ```sql
      CALL load_bronze();
      ```
===============================================================================
*/

DELIMITER //

DROP PROCEDURE IF EXISTS load_bronze;

CREATE PROCEDURE load_bronze()
BEGIN
    DECLARE batch_start_time DATETIME;
    DECLARE batch_end_time DATETIME;

    -- Start batch processing
    SET batch_start_time = NOW();
    SELECT '==========================================' AS Message;
    SELECT 'Truncating Bronze Tables' AS Message;
    SELECT '==========================================' AS Message;

    -- Truncate all Bronze tables before loading new data
    TRUNCATE TABLE bronze_crm_cust_info;
    TRUNCATE TABLE bronze_crm_prd_info;
    TRUNCATE TABLE bronze_crm_sales_details;
    TRUNCATE TABLE bronze_erp_loc_a101;
    TRUNCATE TABLE bronze_erp_cust_az12;
    TRUNCATE TABLE bronze_erp_px_cat_g1v2;

    -- End batch processing
    SET batch_end_time = NOW();
    SELECT '==========================================' AS Message;
    SELECT 'Truncation Completed. Please execute LOAD DATA commands manually.' AS Message;
    SELECT CONCAT('   - Total Execution Time: ', TIMESTAMPDIFF(SECOND, batch_start_time, batch_end_time), ' seconds') AS Message;
    SELECT '==========================================' AS Message;
END //

DELIMITER ;
