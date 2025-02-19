/*
===============================================================================
 **Data Load Script: Bronze Layer (Truncate & Load)**
===============================================================================
 **Purpose:**
    - **Loads raw data into Bronze tables** using `LOAD DATA LOCAL INFILE`.
    - **Measures execution time** for each table to track performance.
    - **Logs execution progress** to monitor the ETL process.

===============================================================================
 **How to Use:**
     **Truncate Tables Before Loading Data:**
    
      ```sql
      CALL load_bronze();
      ```

     **Run This Script to Load Data into Bronze Tables:**
    
      ```sql
      SOURCE sql/load_bronze.sql;
      ```

     **This ensures that stale data is removed before inserting fresh data.**

===============================================================================
 **Prerequisites:**
    - **Ensure `local_infile` is enabled** in MySQL:
    
      ```sql
      SET GLOBAL local_infile = 1;
      SHOW VARIABLES LIKE 'local_infile';
      ```

    - **Ensure all CSV files are available in these locations:**
    
      ```
      C:/Program Files/MySQL/MySQL Server 8.0/Uploads/source_crm/
      C:/Program Files/MySQL/MySQL Server 8.0/Uploads/source_erp/
      ```

    - **The following tables must exist before running this script:**
        - `bronze_crm_cust_info`
        - `bronze_crm_prd_info`
        - `bronze_crm_sales_details`
        - `bronze_erp_loc_a101`
        - `bronze_erp_cust_az12`
        - `bronze_erp_px_cat_g1v2`

===============================================================================
 **Execution Order in ETL Process:**
     **Run Stored Procedure to Truncate Tables** → `CALL load_bronze();`  
     **Load Data from CSVs into Bronze Tables** → Execute this script  
     **Track Execution Time for Each Table** → Logs time taken for each table  
     **Log Process Completion** → Helps with debugging & monitoring  

===============================================================================
 **Logging & Debugging:**
    - Messages will be logged for each step.
    - Execution times will be displayed for performance monitoring.
    - Errors will be shown if any file is missing or MySQL permissions are incorrect.

===============================================================================
 **Maintainer & Contact:**
    - **Maintainer:** [Your Name or Team Name]
    - **GitHub Repository:** [Your GitHub Link]
    - **Version:** 1.0
===============================================================================
*/

-- Start tracking total execution time
SET @batch_start_time = NOW();

-- Load CRM Customer Information
SET @start_time = NOW();
SELECT '>> Inserting Data Into: bronze_crm_cust_info' AS Message;

LOAD DATA LOCAL INFILE 'C:/Program Files/MySQL/MySQL Server 8.0/Uploads/source_crm/cust_info.csv'
INTO TABLE bronze_crm_cust_info
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

SET @end_time = NOW();
SELECT CONCAT('>> Load Duration (bronze_crm_cust_info): ', TIMESTAMPDIFF(SECOND, @start_time, @end_time), ' seconds') AS Message;
SELECT '>> -------------' AS Message;

-- Load CRM Product Information
SET @start_time = NOW();
SELECT '>> Inserting Data Into: bronze_crm_prd_info' AS Message;

LOAD DATA LOCAL INFILE 'C:/Program Files/MySQL/MySQL Server 8.0/Uploads/source_crm/prd_info.csv'
INTO TABLE bronze_crm_prd_info
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

SET @end_time = NOW();
SELECT CONCAT('>> Load Duration (bronze_crm_prd_info): ', TIMESTAMPDIFF(SECOND, @start_time, @end_time), ' seconds') AS Message;
SELECT '>> -------------' AS Message;

-- Load CRM Sales Details
SET @start_time = NOW();
SELECT '>> Inserting Data Into: bronze_crm_sales_details' AS Message;

LOAD DATA LOCAL INFILE 'C:/Program Files/MySQL/MySQL Server 8.0/Uploads/source_crm/sales_details.csv'
INTO TABLE bronze_crm_sales_details
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

SET @end_time = NOW();
SELECT CONCAT('>> Load Duration (bronze_crm_sales_details): ', TIMESTAMPDIFF(SECOND, @start_time, @end_time), ' seconds') AS Message;
SELECT '>> -------------' AS Message;

-- Load ERP Location Information
SET @start_time = NOW();
SELECT '>> Inserting Data Into: bronze_erp_loc_a101' AS Message;

LOAD DATA LOCAL INFILE 'C:/Program Files/MySQL/MySQL Server 8.0/Uploads/source_erp/loc_a101.csv'
INTO TABLE bronze_erp_loc_a101
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

SET @end_time = NOW();
SELECT CONCAT('>> Load Duration (bronze_erp_loc_a101): ', TIMESTAMPDIFF(SECOND, @start_time, @end_time), ' seconds') AS Message;
SELECT '>> -------------' AS Message;

-- Load ERP Customer Demographics
SET @start_time = NOW();
SELECT '>> Inserting Data Into: bronze_erp_cust_az12' AS Message;

LOAD DATA LOCAL INFILE 'C:/Program Files/MySQL/MySQL Server 8.0/Uploads/source_erp/cust_az12.csv'
INTO TABLE bronze_erp_cust_az12
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

SET @end_time = NOW();
SELECT CONCAT('>> Load Duration (bronze_erp_cust_az12): ', TIMESTAMPDIFF(SECOND, @start_time, @end_time), ' seconds') AS Message;
SELECT '>> -------------' AS Message;

-- Load ERP Product Categories
SET @start_time = NOW();
SELECT '>> Inserting Data Into: bronze_erp_px_cat_g1v2' AS Message;

LOAD DATA LOCAL INFILE 'C:/Program Files/MySQL/MySQL Server 8.0/Uploads/source_erp/px_cat_g1v2.csv'
INTO TABLE bronze_erp_px_cat_g1v2
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

SET @end_time = NOW();
SELECT CONCAT('>> Load Duration (bronze_erp_px_cat_g1v2): ', TIMESTAMPDIFF(SECOND, @start_time, @end_time), ' seconds') AS Message;
SELECT '>> -------------' AS Message;

-- Track total execution time
SET @batch_end_time = NOW();
SELECT '==========================================' AS Message;
SELECT 'Loading Bronze Layer is Completed' AS Message;
SELECT CONCAT('   - Total Load Duration: ', TIMESTAMPDIFF(SECOND, @batch_start_time, @batch_end_time), ' seconds') AS Message;
SELECT '==========================================' AS Message;
