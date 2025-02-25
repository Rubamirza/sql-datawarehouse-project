# sql-datawarehouse-project

#This project demonstrates a comprehensive data warehousing and analytics solution, from building a warehouse to generating actionable insights. Designed as a porfolio project, it highlights industry best practices in data engineering and analytics.

Project Overview:
This project involves:
1. Data Architecure: Designing a Modern Data Warehouse using Medallion Architecture bronze, silver and gold layers.
2. ETL Pipelines: Extracting, transforming, and loading data from source systems into the warehouse.
3. Data Modeling: Developing fact and dimension tables optimized for analytical queries.
4. Analytics and Reporting: Creating SQL-based reports and dashboards for actionable insights

PROJECT REQUIREMENTS:

Building the data warehouse (Data Engineering):

Objective:

Develop a modern data warehouse using SQL Server to consolidate sales data, enabling analytical reporting and informed decision-making.

Specifications:
- Data Sources: Import data from two source systems (ERP and CRM) provided as CSV files.
- Data Quality: Cleanse and resolve data quality issues prior to analysis
- Integration: Combine both sources into a single, user-friendly data model designed for analytical queries.
- Scope: Focus on the latest dataset only, historization is not required
- Documentation: Provide a clear documentation of the data model to support both business stakeholders and analytics teams.

BI: Analytics & Reporting (Data Analysis)
Objective
Develop SQL-based analytics to deliver detailed insights into:

Customer Behavior
Product Performance
Sales Trends
These insights empower stakeholders with key business metrics, enabling strategic decision-making.








Challenges and Solutions:

-Challenge 1: LOAD DATA not allowed in stored procedures

Problem: 
While implementing the Bronze Layer Data Load in MySQL, I initially attempted to combine table truncation and data loading inside a stored procedure (load_bronze()). However, this resulted in the following error:

Error Code: 1314. LOAD DATA is not allowed in stored procedures

Cause:
MySQL does not allow LOAD DATA INFILE inside the stored procedure for security reasons.

Solution:
To work around this, I split the process into two separate scripts:
1. proc_truncate_bronze.sql : Here, I stored a procedure that only truncates tables
2.  load_bronze.sql : This SQL script loads data from CSV files using LOAD DATA INFILE

This maintains automation, security, and logging while following MYSQL's restrictions.

Alternative Approach: Using Python for More Flexibility:

In real-world ETL pipelines, Python is commonly used for data ingestion and automation.

While I implemented a SQL-based approach for data loading, an alternative solution could have been using Python with pandas and pymysql to handle the data ingestion process. This approach would allow for:

-Automating file detection and loading, eliminating the need for hardcoded filenames.
-Better error handling, ensuring the pipeline continues running even if one file fails.
-Flexibility in choosing between direct SQL insertion (pymysql) and bulk insertion (pandas.to_sql) for performance optimization.
-Integration with external data sources, such as APIs, cloud storage (AWS S3, Google Drive), or other databases.

Another possible hybrid approach could involve:
- Using Python for automation (detecting and managing files).
- Using LOAD DATA INFILE in MySQL for fast bulk insertion.

Although I opted for a pure SQL-based ingestion approach, recognizing alternative methods showcases adaptability and problem-solving skills in ETL design.
