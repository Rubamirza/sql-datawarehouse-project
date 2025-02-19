/*
===================================================================
Initialize DataWarehouse Database
===================================================================
Script Purpose:
    This script initializes the 'DataWarehouse' database.
    If the database already exists, it will be dropped and recreated.
    
WARNING:
    Running this script will delete all existing data in 'DataWarehouse'.
    Ensure backups exist before execution.
*/

-- Step 1: Drop and Create Database
DROP DATABASE IF EXISTS DataWarehouse;
CREATE DATABASE DataWarehouse CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci;

-- Use the New Database
USE DataWarehouse;
