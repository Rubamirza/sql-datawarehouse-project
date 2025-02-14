/*
===================================================================
Create Database and Schemas
===================================================================
Script Purpose:
	This script creates a new database named 'DataWarehouse' after checking if it already exists.
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas
    (Bronze, Silver, and Gold).
    
WARNING:
	Running this script will drop the entire 'DataWarehouse' database if it already exists.
    All data in the database will be permanently deleted. Proceed with caution and ensure you have
    proper backups before running this script.
*/

-- Create database called 'DataWarehouse'
USE master;

-- Drop the 'DataWarehouse' database if it exists:
DROP DATABASE IF EXISTS DataWarehouse;

-- Create the 'DataWarehouse' database:
CREATE DATABASE DataWarehouse;
USE DataWarehouse;

-- Create schemas:

CREATE SCHEMA bronze;

CREATE SCHEMA silver;

CREATE SCHEMA gold;


