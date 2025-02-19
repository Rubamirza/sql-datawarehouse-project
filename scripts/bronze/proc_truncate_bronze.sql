/*
===============================================================================
 **Stored Procedure: Truncate Bronze Tables**
===============================================================================
 **Purpose:**
    - Clears all data from Bronze tables before loading new data.
    - Prevents duplicate records and ensures data consistency.
    - Logs execution time for performance tracking.

===============================================================================
 **How to Use:**
     **Run the stored procedure to truncate Bronze tables:**
      ```sql
      CALL load_bronze();
      ```

     This ensures all old data is removed before inserting fresh data.
===============================================================================
*/

DELIMITER //

DROP PROCEDURE IF EXISTS load_bronze;

CREATE PROCEDURE load_bronze()
BEGIN
    -- Declare variables to track execution time
    DECLARE batch_start_time DATETIME;
    DECLARE batch_end_time DATETIME;

    -- Start batch processing
    SET batch_start_time = NOW();
    SELECT '==========================================' AS Message;
    SELECT 'Starting Truncation of Bronze Tables' AS Message;
    SELECT '==========================================' AS Message;

    -- Truncate all Bronze tables before loading new data
    TRUNCATE TABLE bronze_crm_cust_info;
    SELECT 'Truncated: bronze_crm_cust_info' AS Message;

    TRUNCATE TABLE bronze_crm_prd_info;
    SELECT 'Truncated: bronze_crm_prd_info' AS Message;

    TRUNCATE TABLE bronze_crm_sales_details;
    SELECT 'Truncated: bronze_crm_sales_details' AS Message;

    TRUNCATE TABLE bronze_erp_loc_a101;
    SELECT 'Truncated: bronze_erp_loc_a101' AS Message;

    TRUNCATE TABLE bronze_erp_cust_az12;
    SELECT 'Truncated: bronze_erp_cust_az12' AS Message;

    TRUNCATE TABLE bronze_erp_px_cat_g1v2;
    SELECT 'Truncated: bronze_erp_px_cat_g1v2' AS Message;

    -- End batch processing
    SET batch_end_time = NOW();
    SELECT '==========================================' AS Message;
    SELECT 'Truncation Completed. Please execute LOAD DATA commands manually.' AS Message;
    SELECT CONCAT('   - Total Execution Time: ', TIMESTAMPDIFF(SECOND, batch_start_time, batch_end_time), ' seconds') AS Message;
    SELECT '==========================================' AS Message;
END //

DELIMITER ;
