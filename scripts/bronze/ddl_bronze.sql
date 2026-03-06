/*
===============================================================================
DDL Script: Create Bronze Layer Tables
===============================================================================
Description:
    This script creates the schema and tables for the 'bronze' layer of the 
    Data Warehouse. The Bronze layer serves as the Landing Zone (Raw Data), 
    where data is ingested from source systems (CRM and ERP) in its 
    original format.

Data Sources:
    - CRM: Customer Info, Product Info, Sales, Campaigns, and Support Tickets.
    - ERP: Locations, Demographics, Category Hierarchy, Inventory, Vendors, 
           Purchase Orders, and HR.

Usage:
    - These tables should be created before running any Data Load (ETL) process.
    - Note: Data types are kept flexible (NVARCHAR) in several tables to 
      accommodate inconsistent source formatting for later cleansing in 
      the Silver layer.
===============================================================================
*/
-- =============================================================================
-- CRM Tables
-- =============================================================================

-- 1. Customer Information (CRM)
IF OBJECT_ID('bronze.crm_cust_info','U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gnder NVARCHAR(50),
	cst_create_date DATE
);

-- 2. Product Information (CRM)
IF OBJECT_ID('bronze.crm_prd_info','U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info(
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATETIME,
	prd_end_dt DATETIME
);

-- 3. Sales Transactions (CRM)
IF OBJECT_ID('bronze.crm_sales_details','U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details(
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cst_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);
-- 4. Campaign Information (CRM)
IF OBJECT_ID('bronze.crm_camp_info','U') IS NOT NULL
	DROP TABLE bronze.crm_camp_info;
CREATE TABLE bronze.crm_camp_info (
    cmp_id    NVARCHAR(20),
    cmp_name  NVARCHAR(200),
    cmp_channel        NVARCHAR(100),
    cmp_budget         NVARCHAR(50),      
    cmp_start_date     NVARCHAR(30),      
    cmp_end_date       NVARCHAR(30),      
);

-- 5. Customer Support Tickets (CRM)
IF OBJECT_ID('bronze.crm_supp_tkts','U') IS NOT NULL
	DROP TABLE bronze.crm_supp_tkts;
CREATE TABLE bronze.crm_supp_tkts (
    tkt_id        NVARCHAR(20),
    tkt_cst_id           NVARCHAR(20),    
    tkt_prd_key          NVARCHAR(100),  
    tkt_emp_id           NVARCHAR(20),    
    tkt_issue_cat   NVARCHAR(100),   
    tkt_status           NVARCHAR(50),    
    tkt_open_date        NVARCHAR(30),   
    tkt_resolution_date  NVARCHAR(30),    
);

-- =============================================================================
-- ERP Tables
-- =============================================================================

-- 1. Location Details (ERP)
IF OBJECT_ID('bronze.erp_loc_a101','U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101(
	cid NVARCHAR(50),
	cntry NVARCHAR(50)
);

-- 2. Customer Demographics (ERP)
IF OBJECT_ID('bronze.erp_cust_az12','U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12(
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(50)
);

-- 3. Product Category Hierarchy (ERP)
IF OBJECT_ID('bronze.erp_px_cat_g1v2','U') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2(
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50) 
);


-- 4. Inventory Master (ERP)
IF OBJECT_ID('bronze.erp_inv_q4v1','U') IS NOT NULL
	DROP TABLE bronze.erp_inv_q4v1;
CREATE TABLE bronze.erp_inv_q4v1 (
    inv_id          NVARCHAR(20),   
    inv_prd_id          NVARCHAR(20),   
    inv_wh_loc   NVARCHAR(50),   
    inv_stock_on_hand   NVARCHAR(20),   
    inv_reorder_level   NVARCHAR(20),
    inv_snap_date   NVARCHAR(30),   
);

-- 5. Vendors (ERP)
IF OBJECT_ID('bronze.erp_vnd_z90','U') IS NOT NULL
	DROP TABLE bronze.erp_vnd_z90;
CREATE TABLE bronze.erp_vnd_z90 (
    vnd_id    NVARCHAR(20),    
    vnd_name  NVARCHAR(200),   
    vnd_country      NVARCHAR(100),   
);

-- 6. Purchase Orders (ERP)
IF OBJECT_ID('bronze.erp_po_ord44','U') IS NOT NULL
	DROP TABLE bronze.erp_po_ord44;
CREATE TABLE bronze.erp_po_ord44 (
    po_number         NVARCHAR(20),  
    po_vnd_id         NVARCHAR(20),  
    po_prd_id            NVARCHAR(20),
    po_quantity_ordered  NVARCHAR(30),  
    po_unit_cost         NVARCHAR(50),  
    po_order_date        NVARCHAR(30),  
);

-- 7. Employees / HR (ERP)
IF OBJECT_ID('bronze.erp_hr_hx19','U') IS NOT NULL
	DROP TABLE bronze.erp_hr_hx19;
CREATE TABLE bronze.erp_hr_hx19 (
    emp_id      NVARCHAR(20),     
    emp_first_name  NVARCHAR(100),    
    emp_last_name   NVARCHAR(100),   
    emp_role        NVARCHAR(100),   
    emp_branch_id   NVARCHAR(20),     
    emp_hire_date   NVARCHAR(30),     
);




