/*
===============================================================================
DDL Script: Create Silver Layer Tables
===============================================================================
Description:
    This script creates the schema and tables for the 'silver' layer of the 
    Data Warehouse. The Silver layer represents the "Cleansed Zone," where 
    data is transformed from the raw Bronze layer into a standardized format.

Key Transformations:
    - Data Type Standardization: Casting strings to proper DATE, INT, and FLOAT types.
    - Metadata Integration: Each table includes a 'dwh_create_date' for audit trails.
    - Derived Logic: Includes calculated columns like 'total_cost' and flags 
      (e.g., 'flag_invalid_date_range') to handle data quality issues.
    - Standardized Naming: Cleaning up inconsistent source field names.

===============================================================================
*/
PRINT '>> Creating Table: silver.crm_cust_info';
IF OBJECT_ID('silver.crm_cust_info','U') IS NOT NULL
	DROP TABLE silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info(
	cst_id INT NOT NULL,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_material_status NVARCHAR(50),
	cst_gnder NVARCHAR(50),
	cst_create_date DATE, 
	dwh_create_date DATETIME2 DEFAULT GETDATE() --metadata column
);

PRINT '>> Creating Table: silver.crm_prd_info';
IF OBJECT_ID('silver.crm_prd_info','U') IS NOT NULL
	DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info(
	prd_id INT NOT NULL,
	cat_id NVARCHAR(50),
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE() --metadata column

);

PRINT '>> Creating Table: silver.crm_sales_details';
IF OBJECT_ID('silver.crm_sales_details','U') IS NOT NULL
	DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details(
	sls_ord_num NVARCHAR(50) NOT NULL,
	sls_prd_key NVARCHAR(50) ,
	sls_cst_id INT,
	sls_order_dt DATE,
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	dwh_create_date DATETIME2 DEFAULT GETDATE() --metadata column

);

PRINT '>> Creating Table: silver.crm_camp_info';
IF OBJECT_ID('silver.crm_camp_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_camp_info;

CREATE TABLE silver.crm_camp_info (
    cmp_id            NVARCHAR(20)   NOT NULL,
    cmp_name          NVARCHAR(200),
    cmp_channel                NVARCHAR(50),
    cmp_budget                 FLOAT,
    cmp_start_date             DATE,
    cmp_end_date               DATE,
    flag_invalid_date_range BIT           DEFAULT 0,
    dwh_create_date        DATETIME2      DEFAULT GETDATE()
);

PRINT '>> Creating Table: silver.crm_supp_tkts';
IF OBJECT_ID('silver.crm_supp_tkts', 'U') IS NOT NULL
    DROP TABLE silver.crm_supp_tkts;

CREATE TABLE silver.crm_supp_tkts (
    tkt_id        NVARCHAR(20)  NOT NULL,
    tkt_cst_id           INT,
    tkt_prd_key          NVARCHAR(100),
    tkt_emp_id           NVARCHAR(20),
    tkt_issue_cat   NVARCHAR(50),
    tkt_status           NVARCHAR(20),
    tkt_open_date        DATE,
    tkt_resolution_date  DATE,
    dwh_create_date  DATETIME2     DEFAULT GETDATE()
);
GO



-- =============================================================================
-- ERP Tables
-- =============================================================================

PRINT '>> Creating Table: silver.erp_loc_a101';
IF OBJECT_ID('silver.erp_loc_a101','U') IS NOT NULL
	DROP TABLE silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101(
	cid NVARCHAR(50) NOT NULL,
	cntry NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE() --metadata column

);

PRINT '>> Creating Table: silver.erp_cust_az12';
IF OBJECT_ID('silver.erp_cust_az12','U') IS NOT NULL
	DROP TABLE silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12(
	cid NVARCHAR(50) NOT NULL,
	bdate DATE,
	gen NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE() --metadata column

);

PRINT '>> Creating Table: silver.erp_px_cat_g1v2';
IF OBJECT_ID('silver.erp_px_cat_g1v2','U') IS NOT NULL
	DROP TABLE silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2(
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50), 
	dwh_create_date DATETIME2 DEFAULT GETDATE() --metadata column

);

PRINT '>> Creating Table: silver.erp_inv_q4v1';
IF OBJECT_ID('silver.erp_inv_q4v1', 'U') IS NOT NULL
    DROP TABLE silver.erp_inv_q4v1;
CREATE TABLE silver.erp_inv_q4v1 (
    inv_id          NVARCHAR(20)  NOT NULL,
    inv_prd_id          INT           NOT NULL,
    inv_wh_loc   NVARCHAR(10),
    inv_stock_on_hand   INT,
    inv_reorder_level   INT,
    below_reorder   BIT,           -- derived: 1 if stock < reorder level
    inv_snap_date   DATE,
    dwh_create_date DATETIME2     DEFAULT GETDATE()
);

PRINT '>> Creating Table: silver.erp_vnd_z90';
IF OBJECT_ID('silver.erp_vnd_z90', 'U') IS NOT NULL
    DROP TABLE silver.erp_vnd_z90;
CREATE TABLE silver.erp_vnd_z90 (
    vnd_id       NVARCHAR(20)  NOT NULL,
    vnd_name     NVARCHAR(200),
    vnd_country         NVARCHAR(100),
    dwh_create_date DATETIME2     DEFAULT GETDATE()
);

PRINT '>> Creating Table: silver.erp_po_ord44';
IF OBJECT_ID('silver.erp_po_ord44', 'U') IS NOT NULL
    DROP TABLE silver.erp_po_ord44;
CREATE TABLE silver.erp_po_ord44 (
    po_number          NVARCHAR(20)  NOT NULL,
    po_vnd_id          NVARCHAR(20),
    po_prd_id             INT,
    po_quantity_ordered   INT,
    po_unit_cost          FLOAT,
    total_cost         FLOAT,          -- derived: quantity × unit_cost
    po_order_date         DATE,
    flag_missing_vendor BIT DEFAULT 0, -- 1 if vendor_id is NULL or not in VND_Z90
    dwh_create_date    DATETIME2       DEFAULT GETDATE()
);

PRINT '>> Creating Table: silver.erp_hr_hx19';
IF OBJECT_ID('silver.erp_hr_hx19', 'U') IS NOT NULL
    DROP TABLE silver.erp_hr_hx19;
CREATE TABLE silver.erp_hr_hx19 (
    emp_id          NVARCHAR(20)  NOT NULL,
    emp_first_name      NVARCHAR(100),
    emp_last_name       NVARCHAR(100),
    emp_full_name       NVARCHAR(200),
    emp_role            NVARCHAR(100),
    emp_branch_id       NVARCHAR(20),
    emp_hire_date       DATE,
    dwh_create_date DATETIME2     DEFAULT GETDATE()
);

PRINT '>> silver Layer DDL Completed.';
GO


