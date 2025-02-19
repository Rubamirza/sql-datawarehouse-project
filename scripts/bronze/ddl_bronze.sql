/*
===================================================================
DDL for Bronze Tables in DataWarehouse
===================================================================
Script Purpose:
    This script defines the Data Definition Language (DDL) 
    for the Bronze-layer tables in the DataWarehouse database.
    
    The Bronze layer stores raw ingested data from different sources 
    before transformation.
    
WARNING:
    Running this script will DROP existing tables and recreate them.
    Ensure backups exist before execution.
*/

-- Switch to the correct database
USE DataWarehouse;

-- Step 1: Create Bronze Tables (Prefixed with `bronze_`)

-- Customer Information Table
DROP TABLE IF EXISTS bronze_crm_cust_info;
CREATE TABLE bronze_crm_cust_info (
    cst_id              INT AUTO_INCREMENT PRIMARY KEY,
    cst_key             VARCHAR(50) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci,
    cst_firstname       VARCHAR(50) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci,
    cst_lastname        VARCHAR(50) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci,
    cst_marital_status  VARCHAR(50) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci,
    cst_gndr            VARCHAR(50) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci,
    cst_create_date     DATE
);

-- Product Information Table
DROP TABLE IF EXISTS bronze_crm_prd_info;
CREATE TABLE bronze_crm_prd_info (
    prd_id       INT AUTO_INCREMENT PRIMARY KEY,
    prd_key      VARCHAR(50) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci,
    prd_nm       VARCHAR(50) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci,
    prd_cost     INT,
    prd_line     VARCHAR(50) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci,
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME
);

-- Sales Details Table
DROP TABLE IF EXISTS bronze_crm_sales_details;
CREATE TABLE bronze_crm_sales_details (
    sls_ord_num  VARCHAR(50) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci,
    sls_prd_key  VARCHAR(50) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci,
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);

-- Location Information Table
DROP TABLE IF EXISTS bronze_erp_loc_a101;
CREATE TABLE bronze_erp_loc_a101 (
    cid    VARCHAR(50) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci,
    cntry  VARCHAR(50) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci
);

-- Customer Demographics Table
DROP TABLE IF EXISTS bronze_erp_cust_az12;
CREATE TABLE bronze_erp_cust_az12 (
    cid    VARCHAR(50) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci,
    bdate  DATE,
    gen    VARCHAR(50) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci
);

-- Product Category Table
DROP TABLE IF EXISTS bronze_erp_px_cat_g1v2;
CREATE TABLE bronze_erp_px_cat_g1v2 (
    id           VARCHAR(50) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci,
    cat          VARCHAR(50) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci,
    subcat       VARCHAR(50) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci,
    maintenance  VARCHAR(50) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci
);

