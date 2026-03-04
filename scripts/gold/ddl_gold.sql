/*
===============================================================================
Script Type:    View Creation
Description:    This script defines the final Star Schema for the Analytics layer.
                It transforms Silver-layer data into business-friendly formats,
                implements Surrogate Keys, and resolves data gaps.
Usage:          This views can be used for analytics and reporting.
Layer:          Gold 
===============================================================================
*/

-- =============================================================================
-- Create Dimension Table: gold.dim_customers
-- Source  : silver.crm_cust_info, silver.erp_cust_az12, silver.erp_loc_a101
-- Grain   : One row per unique customer
-- Note    : Integrates CRM and ERP data; CRM acts as the master for gender
-- =============================================================================
PRINT '>> Creating View: gold.dim_customers';
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY a.cst_id) AS customer_key,
    a.cst_id AS customer_id,
    a.cst_key AS customer_number,
    a.cst_firstname AS first_name,
    a.cst_lastname AS last_name,
    c.cntry AS country,
    a.cst_marital_status AS marital_status,
    CASE 
        WHEN a.cst_gndr != 'n/a' THEN a.cst_gndr -- CRM is the master for gender info
        ELSE COALESCE(b.gen, 'n/a')
    END AS gender,
    a.cst_create_date AS create_date,
    b.bdate AS birth_date
FROM silver.crm_cust_info a 
LEFT JOIN silver.erp_cust_az12 b 
    ON a.cst_key = b.cid
LEFT JOIN silver.erp_loc_a101 c 
    ON a.cst_key = c.cid;
GO

-- =============================================================================
-- Create Dimension Table: gold.dim_products
-- Source  : silver.crm_prd_info, silver.erp_px_cat_g1v2
-- Grain   : One row per unique product
-- Note    : Filters for active products only (where prd_end_dt IS NULL)
-- =============================================================================
PRINT '>> Creating View: gold.dim_products';
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY a.prd_start_dt, a.prd_id) AS product_key,
    a.prd_id AS product_id,
    a.prd_key AS product_number,
    a.prd_nm AS product_name,
    a.cat_id AS category_id,
    b.cat AS category,
    b.subcat AS subcategory,
    b.maintenance,
    a.prd_cost AS cost,
    a.prd_line AS product_line,
    a.prd_start_dt AS start_date 
FROM silver.crm_prd_info a
LEFT JOIN silver.erp_px_cat_g1v2 b
    ON a.cat_id = b.id
WHERE a.prd_end_dt IS NULL; -- Filter only active products
GO

-- =============================================================================
-- Create Dimension Table: gold.dim_employees
-- Source  : silver.erp_hr_hx19
-- Grain   : One row per unique employee
-- Used by : fact_support_tickets (agent dimension)
-- =============================================================================

CREATE OR ALTER VIEW gold.dim_employees AS
SELECT
    -- surrogate business key (natural key used directly — emp_id is stable)
    emp_id                                          AS employee_id,
    emp_first_name                                  AS employee_first_name,
    emp_last_name                                   AS employee_last_name,
    emp_full_name                                       AS employee_full_name,
    emp_role                                            AS job_title,
    emp_branch_id                                       AS branch_id,
    emp_hire_date                                       AS hire_date,
    -- derived: years of service
    DATEDIFF(YEAR, emp_hire_date, CAST(GETDATE() AS DATE)) AS years_of_service,
    -- derived: seniority band
    CASE
        WHEN DATEDIFF(YEAR, emp_hire_date, CAST(GETDATE() AS DATE)) < 2  THEN 'Junior'
        WHEN DATEDIFF(YEAR, emp_hire_date, CAST(GETDATE() AS DATE)) < 5  THEN 'Mid-Level'
        WHEN DATEDIFF(YEAR, emp_hire_date, CAST(GETDATE() AS DATE)) < 10 THEN 'Senior'
        ELSE 'Veteran'
    END AS seniority_band
FROM silver.erp_hr_hx19;
GO

-- =============================================================================
-- Create Dimension Table: gold.dim_vendors
-- Source  : silver.erp_vnd_z90
-- Grain   : One row per unique vendor
-- Used by : fact_purchase_orders
-- =============================================================================
CREATE OR ALTER VIEW gold.dim_vendors AS
SELECT
    vnd_id                                             AS vendor_id,
    vnd_name                                           AS vendor_name,
    vnd_country                                        AS vendor_country,
    -- derived: vendor region grouping
    CASE
        WHEN vnd_country IN ('United States','Canada')              THEN 'North America'
        WHEN vnd_country IN ('United Kingdom','Germany','France',
                         'Italy','Spain')                       THEN 'Europe'
        WHEN vnd_country IN ('Japan','India','Australia')           THEN 'Asia-Pacific'
        ELSE 'Other'
    END AS vendor_region
FROM silver.erp_vnd_z90;
GO

-- =============================================================================
-- Create Dimension Table: gold.dim_campaigns
-- Source  : silver.crm_camp_info
-- Grain   : One row per unique marketing campaign
-- Note    : Excludes 38 rows flagged with invalid date range
-- =============================================================================
CREATE OR ALTER VIEW gold.dim_campaigns AS
SELECT
    cmp_id                                          AS campaign_id,
    cmp_name                                        AS campaign_name,
    cmp_channel                                     AS channel,
    cmp_budget                                      AS budget,
    cmp_start_date                                  AS start_date,
    cmp_end_date                                    AS end_date,
    -- derived: campaign duration in days
    DATEDIFF(DAY, cmp_start_date, cmp_end_date)            AS campaign_duration_days,
    -- derived: year of campaign start
    YEAR(cmp_start_date)                               AS campaign_year,
    flag_invalid_date_range
FROM silver.crm_camp_info;
GO


-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- Source  : silver.crm_sales_details, gold.dim_products, gold.dim_customers
-- Grain   : One row per sales transaction line
-- Note    : Uses Surrogate Keys (product_key, customer_key) from Gold dimensions
-- =============================================================================
PRINT '>> Creating View: gold.fact_sales';
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS 
SELECT 
    a.sls_ord_num AS order_number,
    b.product_key,   -- Joining to Gold Dim for Surrogate Key
    c.customer_key,  -- Joining to Gold Dim for Surrogate Key
    a.sls_order_dt AS order_date,
    a.sls_ship_dt AS shipping_date,
    a.sls_due_dt AS due_date,
    a.sls_sales AS sales_amount,
    a.sls_quantity AS quantity,
    a.sls_price AS price
FROM silver.crm_sales_details a
LEFT JOIN gold.dim_products b 
    ON a.sls_prd_key = b.product_number
LEFT JOIN gold.dim_customers c
    ON a.sls_cst_id = c.customer_id;
GO


IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
    EXEC('CREATE SCHEMA gold');
GO

-- =============================================================================
-- Create Fact Table: gold.fact_support_tickets
-- Source  : silver.crm_supp_tkts
--           + gold.dim_customers  (existing)
--           + gold.dim_employees  (new)
--           + gold.dim_products   (existing)
-- Grain   : One row per support ticket
-- Measures: days_to_resolve, is_overdue
-- =============================================================================
CREATE OR ALTER VIEW gold.fact_support_tickets AS
SELECT
    -- ticket identifiers
    t.tkt_id                                        AS id,
    t.tkt_open_date                                 AS open_date,
    t.tkt_resolution_date                           AS resolution_date,

    -- measures
    DATEDIFF(
        DAY,
        t.tkt_open_date,
        ISNULL(t.tkt_resolution_date, CAST(GETDATE() AS DATE))
    )                                              AS days_to_resolve,

    -- flag: open ticket older than 30 days
    CASE
        WHEN t.tkt_status != 'Closed'
         AND DATEDIFF(DAY, t.tkt_open_date, CAST(GETDATE() AS DATE)) > 30
        THEN 1
        ELSE 0
    END                                            AS is_overdue,

    -- ticket attributes
    t.tkt_issue_cat                                AS issue_category,
    t.tkt_status                                   AS status,

    -- customer dimension (existing)
    c.customer_id,
    c.customer_number,
    CONCAT(c.first_name,' ',c.last_name)           AS customer_full_name,
    c.country                                      AS customer_country,

    -- employee / agent dimension (new)
    e.employee_id,
    e.employee_full_name                           AS assigned_agent,
    e.job_title                                    AS agent_role,
    e.branch_id                                    AS agent_branch,

    -- product dimension (existing)
    p.product_number                               AS product_key,
    p.product_name,
    p.category                                     AS product_category,
    p.subcategory                                  AS product_subcategory

FROM silver.crm_supp_tkts           t
LEFT JOIN gold.dim_customers        c ON t.tkt_cst_id   = c.customer_id
LEFT JOIN gold.dim_employees        e ON t.tkt_emp_id   = e.employee_id
LEFT JOIN gold.dim_products         p ON t.tkt_prd_key  = p.product_number;
GO

-- =============================================================================
-- Create Fact Table: gold.fact_inventory
-- Source  : silver.erp_inv_q4v1
--           + gold.dim_products (existing)
-- Grain   : One row per inventory snapshot (product × warehouse × date)
-- Measures: stock_on_hand, reorder_level, below_reorder flag, stock_value
-- =============================================================================
CREATE OR ALTER VIEW gold.fact_inventory AS
SELECT
    -- snapshot identifiers
    i.inv_id                                        AS inventory_id,
    i.inv_snap_date                                 AS snapshot_date,
    i.inv_wh_loc                                    AS warehouse_location,

    -- product dimension
    p.product_id,
    p.product_number                               AS product_key,
    p.product_name,
    p.category                                     AS product_category,
    p.subcategory                                  AS product_subcategory,

    -- inventory measures
    i.inv_stock_on_hand                            AS stock_on_hand,
    i.inv_reorder_level                            AS reorder_level,
    i.below_reorder,
    -- derived: stock gap (how far below/above reorder point)
    i.inv_stock_on_hand - i.inv_reorder_level              AS stock_vs_reorder,
    -- derived: estimated stock value (stock × product cost)
    i.inv_stock_on_hand * ISNULL(p.cost, 0)    AS estimated_stock_value

FROM silver.erp_inv_q4v1           i
LEFT JOIN gold.dim_products         p ON i.inv_prd_id   = p.product_id;
GO

-- =============================================================================
-- Create Fact Table: gold.fact_purchase_orders
-- Source  : silver.erp_po_ord44
--           + gold.dim_vendors  (new)
--           + gold.dim_products (existing)
-- Grain   : One row per purchase order line
-- Measures: quantity_ordered, unit_cost, total_cost
-- =============================================================================
CREATE OR ALTER VIEW gold.fact_purchase_orders AS
SELECT
    -- order identifiers
    pr.po_number,
    pr.po_order_date                                  AS order_date,
    YEAR(pr.po_order_date)                            AS order_year,
    MONTH(pr.po_order_date)                           AS order_month,

    -- vendor dimension
    v.vendor_id,
    v.vendor_name,
    v.vendor_country,
    v.vendor_region,

    -- product dimension
    p.product_id,
    p.product_name,
    p.category                                     AS product_category,

    -- measures
    pr.po_quantity_ordered                         AS quantity_ordered,
    pr.po_unit_cost                                AS unit_cost,
    pr.total_cost,

    -- quality flags
    pr.flag_missing_vendor

FROM silver.erp_po_ord44            pr
LEFT JOIN gold.dim_vendors           v  ON pr.po_vnd_id = v.vendor_id
LEFT JOIN gold.dim_products          p  ON pr.po_prd_id    = p.product_id;
GO
