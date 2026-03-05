/*
===============================================================================
Data Quality Checks — Silver Layer + Gold Layer
===============================================================================
Project  : SQL Server Data Warehouse — Medallion Architecture
Purpose  : Validate the Silver and Gold layers after running the full pipeline.
           All checks return 0 rows (or 0 issue_count) when data is clean.
           Any non-zero result = an issue to investigate.

Usage    : Run AFTER executing all Bronze, Silver, and Gold scripts in order:
             1. EXEC bronze.load_bronze
             2. EXEC silver.load_silver
             3. Execute DDL_Create_Views_Gold.sql

Sections :
    G1.  NULL Checks — Dimensions
    G2.  NULL Checks — Facts (join key results)
    G3.  Duplicate Checks — Dimensions
    G4.  Date Logic Checks — Gold
    G5.  Business Rule Checks — Gold
    G6.  Gold Row Count Summary
    G7.  Join Match Rate Summary

===============================================================================
*/

-- ============================================================================
-- G1. NULL CHECKS — Dimensions
-- ============================================================================
PRINT '================================================================';
PRINT '== G1. NULL Checks — Gold Dimensions ==';
PRINT '================================================================';

-- dim_customers
SELECT 'dim_customers - NULL customer_id'      AS check_name, COUNT(*) AS issue_count
FROM gold.dim_customers WHERE customer_id IS NULL
UNION ALL
SELECT 'dim_customers - NULL customer_number', COUNT(*)
FROM gold.dim_customers WHERE customer_number IS NULL
UNION ALL
SELECT 'dim_customers - NULL first_name',      COUNT(*)
FROM gold.dim_customers WHERE first_name IS NULL
UNION ALL
SELECT 'dim_customers - NULL last_name',       COUNT(*)
FROM gold.dim_customers WHERE last_name IS NULL

UNION ALL
-- dim_products
SELECT 'dim_products - NULL product_id',       COUNT(*)
FROM gold.dim_products WHERE product_id IS NULL
UNION ALL
SELECT 'dim_products - NULL product_number',   COUNT(*)
FROM gold.dim_products WHERE product_number IS NULL
UNION ALL
SELECT 'dim_products - NULL product_name',     COUNT(*)
FROM gold.dim_products WHERE product_name IS NULL
UNION ALL
SELECT 'dim_products - NULL category',         COUNT(*)
FROM gold.dim_products WHERE category IS NULL

UNION ALL
-- dim_employees
SELECT 'dim_employees - NULL employee_id',     COUNT(*)
FROM gold.dim_employees WHERE employee_id IS NULL
UNION ALL
SELECT 'dim_employees - NULL employee_full_name', COUNT(*)
FROM gold.dim_employees WHERE employee_full_name IS NULL
UNION ALL
SELECT 'dim_employees - NULL job_title',       COUNT(*)
FROM gold.dim_employees WHERE job_title IS NULL

UNION ALL
-- dim_vendors
SELECT 'dim_vendors - NULL vendor_id',         COUNT(*)
FROM gold.dim_vendors WHERE vendor_id IS NULL
UNION ALL
SELECT 'dim_vendors - NULL vendor_name',       COUNT(*)
FROM gold.dim_vendors WHERE vendor_name IS NULL
UNION ALL
SELECT 'dim_vendors - NULL vendor_region',     COUNT(*)
FROM gold.dim_vendors WHERE vendor_region IS NULL

UNION ALL
-- dim_campaigns
SELECT 'dim_campaigns - NULL campaign_id',     COUNT(*)
FROM gold.dim_campaigns WHERE campaign_id IS NULL
UNION ALL
SELECT 'dim_campaigns - NULL campaign_name',   COUNT(*)
FROM gold.dim_campaigns WHERE campaign_name IS NULL
UNION ALL
SELECT 'dim_campaigns - NULL channel',         COUNT(*)
FROM gold.dim_campaigns WHERE channel IS NULL;


-- ============================================================================
-- G2. NULL CHECKS — Facts (join key resolution)
-- ============================================================================
PRINT '================================================================';
PRINT '== G2. NULL Checks — Gold Fact Join Integrity ==';
PRINT '================================================================';

-- fact_sales: both dimension joins must resolve for every row
SELECT 'fact_sales - NULL product_key (unresolved product join)' AS check_name,
       COUNT(*) AS issue_count
FROM gold.fact_sales WHERE product_key IS NULL
UNION ALL
SELECT 'fact_sales - NULL customer_key (unresolved customer join)', COUNT(*)
FROM gold.fact_sales WHERE customer_key IS NULL

UNION ALL
-- fact_support_tickets
-- Note: customer_id / employee_id NULLs are expected for ghost/invalid source IDs;
--       the join match % is reported accurately in Section G7.
SELECT 'fact_support_tickets - NULL id (ticket has no identifier)', COUNT(*)
FROM gold.fact_support_tickets WHERE id IS NULL
UNION ALL
SELECT 'fact_support_tickets - NULL product_key (unresolved product join)', COUNT(*)
FROM gold.fact_support_tickets WHERE product_key IS NULL

UNION ALL
-- fact_inventory: every inventory row must link to a product
SELECT 'fact_inventory - NULL product_id (unresolved product join)', COUNT(*)
FROM gold.fact_inventory WHERE product_id IS NULL

UNION ALL
-- fact_purchase_orders
SELECT 'fact_purchase_orders - NULL product_id (unresolved product join)', COUNT(*)
FROM gold.fact_purchase_orders WHERE product_id IS NULL;


-- ============================================================================
-- G3. DUPLICATE CHECKS — Dimensions must be unique on their key
-- ============================================================================
PRINT '================================================================';
PRINT '== G3. Duplicate Checks — Gold Dimensions ==';
PRINT '================================================================';

SELECT 'dim_customers - duplicate customer_id'   AS check_name, COUNT(*) AS issue_count
FROM (
    SELECT customer_id, COUNT(*) AS c
    FROM gold.dim_customers GROUP BY customer_id HAVING COUNT(*) > 1
) x
UNION ALL
SELECT 'dim_customers - duplicate customer_key', COUNT(*)
FROM (
    SELECT customer_key, COUNT(*) AS c
    FROM gold.dim_customers GROUP BY customer_key HAVING COUNT(*) > 1
) x
UNION ALL
SELECT 'dim_products - duplicate product_id',    COUNT(*)
FROM (
    SELECT product_id, COUNT(*) AS c
    FROM gold.dim_products GROUP BY product_id HAVING COUNT(*) > 1
) x
UNION ALL
SELECT 'dim_products - duplicate product_key',   COUNT(*)
FROM (
    SELECT product_key, COUNT(*) AS c
    FROM gold.dim_products GROUP BY product_key HAVING COUNT(*) > 1
) x
UNION ALL
SELECT 'dim_employees - duplicate employee_id',  COUNT(*)
FROM (
    SELECT employee_id, COUNT(*) AS c
    FROM gold.dim_employees GROUP BY employee_id HAVING COUNT(*) > 1
) x
UNION ALL
SELECT 'dim_vendors - duplicate vendor_id',      COUNT(*)
FROM (
    SELECT vendor_id, COUNT(*) AS c
    FROM gold.dim_vendors GROUP BY vendor_id HAVING COUNT(*) > 1
) x
UNION ALL
SELECT 'dim_campaigns - duplicate campaign_id',  COUNT(*)
FROM (
    SELECT campaign_id, COUNT(*) AS c
    FROM gold.dim_campaigns GROUP BY campaign_id HAVING COUNT(*) > 1
) x;


-- ============================================================================
-- G4. DATE LOGIC CHECKS — Gold
-- ============================================================================
PRINT '================================================================';
PRINT '== G4. Date Logic Checks — Gold ==';
PRINT '================================================================';

-- fact_sales date ordering
SELECT 'fact_sales - shipping_date before order_date' AS check_name, COUNT(*) AS issue_count
FROM gold.fact_sales WHERE shipping_date < order_date
UNION ALL
SELECT 'fact_sales - due_date before order_date', COUNT(*)
FROM gold.fact_sales WHERE due_date < order_date

UNION ALL
-- fact_support_tickets
SELECT 'fact_support_tickets - resolution_date before open_date', COUNT(*)
FROM gold.fact_support_tickets WHERE resolution_date < open_date

UNION ALL
-- fact_purchase_orders: order date sanity
SELECT 'fact_purchase_orders - order_date in the future', COUNT(*)
FROM gold.fact_purchase_orders WHERE order_date > CAST(GETDATE() AS DATE)

UNION ALL
-- dim_campaigns: valid date ranges (outside of intentionally flagged rows)
SELECT 'dim_campaigns - end_date before start_date (not flagged)', COUNT(*)
FROM gold.dim_campaigns
WHERE end_date < start_date AND flag_invalid_date_range = 0

UNION ALL
-- dim_employees: hire date must not be in the future
SELECT 'dim_employees - future hire_date', COUNT(*)
FROM gold.dim_employees WHERE hire_date > CAST(GETDATE() AS DATE)

UNION ALL
-- dim_campaigns: campaign_duration_days must be >= 0 for valid rows
SELECT 'dim_campaigns - negative campaign_duration_days (not flagged)', COUNT(*)
FROM gold.dim_campaigns
WHERE campaign_duration_days < 0 AND flag_invalid_date_range = 0

UNION ALL
-- dim_employees: years_of_service must be non-negative
SELECT 'dim_employees - negative years_of_service', COUNT(*)
FROM gold.dim_employees WHERE years_of_service < 0;


-- ============================================================================
-- G5. BUSINESS RULE CHECKS — Gold
-- ============================================================================
PRINT '================================================================';
PRINT '== G5. Business Rule Checks — Gold ==';
PRINT '================================================================';

-- fact_sales: financial measures must be positive
SELECT 'fact_sales - zero or negative sales_amount'  AS check_name, COUNT(*) AS issue_count
FROM gold.fact_sales WHERE sales_amount <= 0
UNION ALL
SELECT 'fact_sales - zero or negative quantity', COUNT(*)
FROM gold.fact_sales WHERE quantity <= 0
UNION ALL
SELECT 'fact_sales - zero or negative price', COUNT(*)
FROM gold.fact_sales WHERE price <= 0

UNION ALL
-- fact_inventory: stock values after silver cleansing
SELECT 'fact_inventory - negative stock_on_hand', COUNT(*)
FROM gold.fact_inventory WHERE stock_on_hand < 0
UNION ALL
SELECT 'fact_inventory - negative reorder_level', COUNT(*)
FROM gold.fact_inventory WHERE reorder_level <= 0
UNION ALL
-- below_reorder flag must be consistent with actual stock levels
SELECT 'fact_inventory - below_reorder flag mismatch', COUNT(*)
FROM gold.fact_inventory
WHERE below_reorder != CASE
    WHEN stock_on_hand < reorder_level THEN 1
    ELSE 0
END
UNION ALL
-- stock_vs_reorder must equal stock_on_hand - reorder_level
SELECT 'fact_inventory - stock_vs_reorder calculation mismatch', COUNT(*)
FROM gold.fact_inventory
WHERE stock_vs_reorder != stock_on_hand - reorder_level

UNION ALL
-- fact_purchase_orders
SELECT 'fact_purchase_orders - zero or negative quantity_ordered', COUNT(*)
FROM gold.fact_purchase_orders WHERE quantity_ordered <= 0
UNION ALL
SELECT 'fact_purchase_orders - negative unit_cost', COUNT(*)
FROM gold.fact_purchase_orders WHERE unit_cost < 0
UNION ALL
-- total_cost must be consistent: quantity * unit_cost (allow for NULLs)
SELECT 'fact_purchase_orders - total_cost calculation mismatch', COUNT(*)
FROM gold.fact_purchase_orders
WHERE total_cost IS NOT NULL
  AND unit_cost IS NOT NULL
  AND ABS(total_cost - (quantity_ordered * unit_cost)) > 0.01

UNION ALL
-- fact_support_tickets: days_to_resolve must be >= 0
SELECT 'fact_support_tickets - negative days_to_resolve', COUNT(*)
FROM gold.fact_support_tickets WHERE days_to_resolve < 0

UNION ALL
-- dim_campaigns: budget must be >= 0
SELECT 'dim_campaigns - negative budget', COUNT(*)
FROM gold.dim_campaigns WHERE budget < 0

UNION ALL
-- dim_products: cost must be >= 0
SELECT 'dim_products - negative cost', COUNT(*)
FROM gold.dim_products WHERE cost < 0

UNION ALL
-- dim_employees: seniority_band must be one of the 4 defined bands
SELECT 'dim_employees - invalid seniority_band', COUNT(*)
FROM gold.dim_employees
WHERE seniority_band NOT IN ('Junior', 'Mid-Level', 'Senior', 'Veteran')

UNION ALL
-- dim_vendors: vendor_region must be one of the 4 defined regions
SELECT 'dim_vendors - invalid vendor_region', COUNT(*)
FROM gold.dim_vendors
WHERE vendor_region NOT IN ('North America', 'Europe', 'Asia-Pacific', 'Other');


-- ============================================================================
-- G6. ROW COUNT SUMMARY — Gold
-- ============================================================================
PRINT '================================================================';
PRINT '== G6. Gold Row Count Summary ==';
PRINT '================================================================';

SELECT 'gold.dim_customers'        AS object_name, COUNT(*) AS row_count FROM gold.dim_customers
UNION ALL
SELECT 'gold.dim_products',         COUNT(*) FROM gold.dim_products
UNION ALL
SELECT 'gold.dim_employees',        COUNT(*) FROM gold.dim_employees
UNION ALL
SELECT 'gold.dim_vendors',          COUNT(*) FROM gold.dim_vendors
UNION ALL
SELECT 'gold.dim_campaigns',        COUNT(*) FROM gold.dim_campaigns
UNION ALL
SELECT 'gold.fact_sales',           COUNT(*) FROM gold.fact_sales
UNION ALL
SELECT 'gold.fact_support_tickets', COUNT(*) FROM gold.fact_support_tickets
UNION ALL
SELECT 'gold.fact_inventory',       COUNT(*) FROM gold.fact_inventory
UNION ALL
SELECT 'gold.fact_purchase_orders', COUNT(*) FROM gold.fact_purchase_orders
ORDER BY 1;


-- ============================================================================
-- G7. JOIN MATCH RATE SUMMARY
--     Reports the % of fact rows that successfully resolved each dimension join.
--     Use this alongside G2 — G2 catches hard failures, G7 gives the full picture.
-- ============================================================================
PRINT '================================================================';
PRINT '== G7. Join Match Rate Summary ==';
PRINT '================================================================';

-- fact_sales: product and customer match rate
PRINT '-- fact_sales join match rates --';
SELECT
    COUNT(*)                                                                        AS total_sales,
    SUM(CASE WHEN product_key  IS NOT NULL THEN 1 ELSE 0 END)                      AS matched_products,
    SUM(CASE WHEN customer_key IS NOT NULL THEN 1 ELSE 0 END)                      AS matched_customers,
    CAST(100.0 * SUM(CASE WHEN product_key  IS NOT NULL THEN 1 ELSE 0 END)
         / NULLIF(COUNT(*), 0) AS DECIMAL(5,1))                                    AS product_match_pct,
    CAST(100.0 * SUM(CASE WHEN customer_key IS NOT NULL THEN 1 ELSE 0 END)
         / NULLIF(COUNT(*), 0) AS DECIMAL(5,1))                                    AS customer_match_pct
FROM gold.fact_sales;

-- fact_support_tickets: 3-way join match rates
-- Note: customer NULLs = ghost/NULL source IDs (expected ~513)
--       employee NULLs = raw NULLs + EMP292-EMP300 not in HR (expected ~824)
--       product NULLs  = raw NULL prd_key in source tickets (expected ~441)
PRINT '-- fact_support_tickets join match rates --';
SELECT
    COUNT(*)                                                                        AS total_tickets,
    SUM(CASE WHEN customer_id   IS NOT NULL THEN 1 ELSE 0 END)                     AS matched_customers,
    SUM(CASE WHEN employee_id   IS NOT NULL THEN 1 ELSE 0 END)                     AS matched_employees,
    SUM(CASE WHEN product_key   IS NOT NULL THEN 1 ELSE 0 END)                     AS matched_products,
    CAST(100.0 * SUM(CASE WHEN customer_id IS NOT NULL THEN 1 ELSE 0 END)
         / NULLIF(COUNT(*), 0) AS DECIMAL(5,1))                                    AS customer_match_pct,
    CAST(100.0 * SUM(CASE WHEN employee_id IS NOT NULL THEN 1 ELSE 0 END)
         / NULLIF(COUNT(*), 0) AS DECIMAL(5,1))                                    AS employee_match_pct,
    CAST(100.0 * SUM(CASE WHEN product_key IS NOT NULL THEN 1 ELSE 0 END)
         / NULLIF(COUNT(*), 0) AS DECIMAL(5,1))                                    AS product_match_pct
FROM gold.fact_support_tickets;

-- fact_purchase_orders: vendor and product match rates
-- Note: 429 rows are flagged as flag_missing_vendor=1; this is expected
PRINT '-- fact_purchase_orders join match rates --';
SELECT
    COUNT(*)                                                                        AS total_pos,
    SUM(CASE WHEN vendor_id  IS NOT NULL THEN 1 ELSE 0 END)                        AS matched_vendors,
    SUM(CASE WHEN product_id IS NOT NULL THEN 1 ELSE 0 END)                        AS matched_products,
    SUM(CASE WHEN flag_missing_vendor = 1 THEN 1 ELSE 0 END)                       AS flagged_missing_vendor,
    CAST(100.0 * SUM(CASE WHEN vendor_id  IS NOT NULL THEN 1 ELSE 0 END)
         / NULLIF(COUNT(*), 0) AS DECIMAL(5,1))                                    AS vendor_match_pct,
    CAST(100.0 * SUM(CASE WHEN product_id IS NOT NULL THEN 1 ELSE 0 END)
         / NULLIF(COUNT(*), 0) AS DECIMAL(5,1))                                    AS product_match_pct
FROM gold.fact_purchase_orders;

-- fact_inventory: product match rate
PRINT '-- fact_inventory join match rates --';
SELECT
    COUNT(*)                                                                        AS total_inv_snapshots,
    SUM(CASE WHEN product_id IS NOT NULL THEN 1 ELSE 0 END)                        AS matched_products,
    SUM(CASE WHEN product_id IS NULL     THEN 1 ELSE 0 END)                        AS unmatched_products,
    CAST(100.0 * SUM(CASE WHEN product_id IS NOT NULL THEN 1 ELSE 0 END)
         / NULLIF(COUNT(*), 0) AS DECIMAL(5,1))                                    AS product_match_pct
FROM gold.fact_inventory;
