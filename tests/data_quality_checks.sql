/*
===============================================================================
Data Quality Checks — Silver Layer 
===============================================================================
Project  : SQL Server Data Warehouse — Medallion Architecture
Purpose  : Validate the Silver and Gold layers after running the full pipeline.
           All checks return 0 rows (or 0 issue_count) when data is clean.
           Any non-zero result = an issue to investigate.

Usage    : Run AFTER executing all Bronze, Silver :
             1. EXEC bronze.load_bronze
             2. EXEC silver.load_silver

Sections :
    S1.  NULL Checks — CRM Silver Tables
    S2.  NULL Checks — ERP Silver Tables
    S3.  Duplicate Checks — All Silver Tables
    S4.  Data Standardization Checks (canonical values)
    S5.  Date Logic Checks — Silver
    S6.  Business Rule Checks — Silver
    S7.  Silver Row Count Summary

===============================================================================
*/

-- ============================================================================
-- S1. NULL CHECKS — CRM Silver Tables
-- ============================================================================
PRINT '================================================================';
PRINT '== S1. NULL Checks — CRM Silver Tables ==';
PRINT '================================================================';

-- silver.crm_cust_info
SELECT 'silver.crm_cust_info - NULL cst_id'        AS check_name, COUNT(*) AS issue_count
FROM silver.crm_cust_info WHERE cst_id IS NULL
UNION ALL
SELECT 'silver.crm_cust_info - NULL cst_key',        COUNT(*)
FROM silver.crm_cust_info WHERE cst_key IS NULL
UNION ALL
SELECT 'silver.crm_cust_info - NULL cst_firstname',  COUNT(*)
FROM silver.crm_cust_info WHERE cst_firstname IS NULL
UNION ALL
SELECT 'silver.crm_cust_info - NULL cst_lastname',   COUNT(*)
FROM silver.crm_cust_info WHERE cst_lastname IS NULL

UNION ALL
-- silver.crm_prd_info
SELECT 'silver.crm_prd_info - NULL prd_id',          COUNT(*)
FROM silver.crm_prd_info WHERE prd_id IS NULL
UNION ALL
SELECT 'silver.crm_prd_info - NULL prd_key',         COUNT(*)
FROM silver.crm_prd_info WHERE prd_key IS NULL
UNION ALL
SELECT 'silver.crm_prd_info - NULL prd_nm',          COUNT(*)
FROM silver.crm_prd_info WHERE prd_nm IS NULL
UNION ALL
SELECT 'silver.crm_prd_info - NULL cat_id',          COUNT(*)
FROM silver.crm_prd_info WHERE cat_id IS NULL

UNION ALL
-- silver.crm_sales_details
SELECT 'silver.crm_sales_details - NULL sls_ord_num', COUNT(*)
FROM silver.crm_sales_details WHERE sls_ord_num IS NULL
UNION ALL
SELECT 'silver.crm_sales_details - NULL sls_prd_key', COUNT(*)
FROM silver.crm_sales_details WHERE sls_prd_key IS NULL
UNION ALL
SELECT 'silver.crm_sales_details - NULL sls_cst_id',  COUNT(*)
FROM silver.crm_sales_details WHERE sls_cst_id IS NULL

UNION ALL
-- silver.crm_camp_info
SELECT 'silver.crm_camp_info - NULL cmp_id',          COUNT(*)
FROM silver.crm_camp_info WHERE cmp_id IS NULL
UNION ALL
SELECT 'silver.crm_camp_info - NULL cmp_name',        COUNT(*)
FROM silver.crm_camp_info WHERE cmp_name IS NULL

UNION ALL
-- silver.crm_supp_tkts
SELECT 'silver.crm_supp_tkts - NULL tkt_id',         COUNT(*)
FROM silver.crm_supp_tkts WHERE tkt_id IS NULL;


-- ============================================================================
-- S2. NULL CHECKS — ERP Silver Tables
-- ============================================================================
PRINT '================================================================';
PRINT '== S2. NULL Checks — ERP Silver Tables ==';
PRINT '================================================================';

SELECT 'silver.erp_cust_az12 - NULL cid'              AS check_name, COUNT(*) AS issue_count
FROM silver.erp_cust_az12 WHERE cid IS NULL
UNION ALL
-- silver.erp_loc_a101
SELECT 'silver.erp_loc_a101 - NULL cid',              COUNT(*)
FROM silver.erp_loc_a101 WHERE cid IS NULL
UNION ALL
SELECT 'silver.erp_loc_a101 - NULL cntry',            COUNT(*)
FROM silver.erp_loc_a101 WHERE cntry IS NULL

UNION ALL
-- silver.erp_px_cat_g1v2
SELECT 'silver.erp_px_cat_g1v2 - NULL id',           COUNT(*)
FROM silver.erp_px_cat_g1v2 WHERE id IS NULL
UNION ALL
SELECT 'silver.erp_px_cat_g1v2 - NULL cat',          COUNT(*)
FROM silver.erp_px_cat_g1v2 WHERE cat IS NULL

UNION ALL
-- silver.erp_hr_hx19
SELECT 'silver.erp_hr_hx19 - NULL emp_id',           COUNT(*)
FROM silver.erp_hr_hx19 WHERE emp_id IS NULL
UNION ALL
SELECT 'silver.erp_hr_hx19 - NULL emp_full_name',    COUNT(*)
FROM silver.erp_hr_hx19 WHERE emp_full_name IS NULL
UNION ALL
SELECT 'silver.erp_hr_hx19 - NULL emp_role',         COUNT(*)
FROM silver.erp_hr_hx19 WHERE emp_role IS NULL

UNION ALL
-- silver.erp_vnd_z90
SELECT 'silver.erp_vnd_z90 - NULL vnd_id',           COUNT(*)
FROM silver.erp_vnd_z90 WHERE vnd_id IS NULL
UNION ALL
SELECT 'silver.erp_vnd_z90 - NULL vnd_name',         COUNT(*)
FROM silver.erp_vnd_z90 WHERE vnd_name IS NULL

UNION ALL
-- silver.erp_inv_q4v1
SELECT 'silver.erp_inv_q4v1 - NULL inv_id',          COUNT(*)
FROM silver.erp_inv_q4v1 WHERE inv_id IS NULL
UNION ALL
SELECT 'silver.erp_inv_q4v1 - NULL inv_prd_id',      COUNT(*)
FROM silver.erp_inv_q4v1 WHERE inv_prd_id IS NULL

UNION ALL
-- silver.erp_po_ord44
SELECT 'silver.erp_po_ord44 - NULL po_number',       COUNT(*)
FROM silver.erp_po_ord44 WHERE po_number IS NULL;


-- ============================================================================
-- S3. DUPLICATE CHECKS — All Silver Tables
--     Each table should have a unique natural key after Silver deduplication except orders in crm_sales_details.
-- ============================================================================
PRINT '================================================================';
PRINT '== S3. Duplicate Checks — Silver Tables ==';
PRINT '================================================================';

-- crm_cust_info: unique on cst_id (silver deduplication by most recent date)
SELECT 'silver.crm_cust_info - duplicate cst_id'     AS check_name, COUNT(*) AS issue_count
FROM (
    SELECT cst_id, COUNT(*) AS c
    FROM silver.crm_cust_info
    GROUP BY cst_id HAVING COUNT(*) > 1
) x
UNION ALL
-- crm_prd_info: a prd_key can appear >1 time (versioned: active + historical rows)
-- uniqueness check is on (prd_key, prd_start_dt) — no two rows same key+start
SELECT 'silver.crm_prd_info - duplicate (prd_key, prd_start_dt)', COUNT(*)
FROM (
    SELECT prd_key, prd_start_dt, COUNT(*) AS c
    FROM silver.crm_prd_info
    GROUP BY prd_key, prd_start_dt HAVING COUNT(*) > 1
) x
UNION ALL
-- crm_sales_details: unique on sls_ord_num 
SELECT 'silver.crm_sales_details - duplicate sls_ord_num', COUNT(*)
FROM (
    SELECT sls_ord_num, COUNT(*) AS c
    FROM silver.crm_sales_details
    GROUP BY sls_ord_num HAVING COUNT(*) > 1
) x
UNION ALL
-- crm_camp_info: unique on cmp_id
SELECT 'silver.crm_camp_info - duplicate cmp_id', COUNT(*)
FROM (
    SELECT cmp_id, COUNT(*) AS c
    FROM silver.crm_camp_info
    GROUP BY cmp_id HAVING COUNT(*) > 1
) x
UNION ALL
-- crm_supp_tkts: unique on tkt_id
SELECT 'silver.crm_supp_tkts - duplicate tkt_id', COUNT(*)
FROM (
    SELECT tkt_id, COUNT(*) AS c
    FROM silver.crm_supp_tkts
    GROUP BY tkt_id HAVING COUNT(*) > 1
) x
UNION ALL
-- erp_cust_az12: unique on cid
SELECT 'silver.erp_cust_az12 - duplicate cid', COUNT(*)
FROM (
    SELECT cid, COUNT(*) AS c
    FROM silver.erp_cust_az12
    GROUP BY cid HAVING COUNT(*) > 1
) x
UNION ALL
-- erp_loc_a101: unique on cid
SELECT 'silver.erp_loc_a101 - duplicate cid', COUNT(*)
FROM (
    SELECT cid, COUNT(*) AS c
    FROM silver.erp_loc_a101
    GROUP BY cid HAVING COUNT(*) > 1
) x
UNION ALL
-- erp_px_cat_g1v2: unique on id
SELECT 'silver.erp_px_cat_g1v2 - duplicate id', COUNT(*)
FROM (
    SELECT id, COUNT(*) AS c
    FROM silver.erp_px_cat_g1v2
    GROUP BY id HAVING COUNT(*) > 1
) x
UNION ALL
-- erp_hr_hx19: unique on emp_id (silver dedup: keep latest hire_date)
SELECT 'silver.erp_hr_hx19 - duplicate emp_id', COUNT(*)
FROM (
    SELECT emp_id, COUNT(*) AS c
    FROM silver.erp_hr_hx19
    GROUP BY emp_id HAVING COUNT(*) > 1
) x
UNION ALL
-- erp_vnd_z90: unique on vnd_id (silver dedup applied)
SELECT 'silver.erp_vnd_z90 - duplicate vnd_id', COUNT(*)
FROM (
    SELECT vnd_id, COUNT(*) AS c
    FROM silver.erp_vnd_z90
    GROUP BY vnd_id HAVING COUNT(*) > 1
) x
UNION ALL
-- erp_inv_q4v1: unique on inv_id (silver dedup: keep latest snapshot_date)
SELECT 'silver.erp_inv_q4v1 - duplicate inv_id', COUNT(*)
FROM (
    SELECT inv_id, COUNT(*) AS c
    FROM silver.erp_inv_q4v1
    GROUP BY inv_id HAVING COUNT(*) > 1
) x
UNION ALL
-- erp_po_ord44: unique on po_number (silver dedup: keep latest order_date)
SELECT 'silver.erp_po_ord44 - duplicate po_number', COUNT(*)
FROM (
    SELECT po_number, COUNT(*) AS c
    FROM silver.erp_po_ord44
    GROUP BY po_number HAVING COUNT(*) > 1
) x;


-- ============================================================================
-- S4. DATA STANDARDIZATION CHECKS
--     Verify that Silver transforms produced only valid canonical values.
--     Any row returned means an unexpected/unhandled raw variant slipped through.
-- ============================================================================
PRINT '================================================================';
PRINT '== S4. Data Standardization Checks ==';
PRINT '================================================================';

-- crm_cust_info: marital_status must be Single / Married / n/a only
SELECT 'silver.crm_cust_info - invalid cst_marital_status' AS check_name,
       COUNT(*) AS issue_count
FROM silver.crm_cust_info
WHERE cst_marital_status NOT IN ('Single', 'Married', 'n/a')
UNION ALL
-- crm_cust_info: gender must be Male / Female / n/a only
SELECT 'silver.crm_cust_info - invalid cst_gnder', COUNT(*)
FROM silver.crm_cust_info
WHERE cst_gndr NOT IN ('Male', 'Female', 'n/a')

UNION ALL
-- crm_prd_info: product_line must be canonical values only
SELECT 'silver.crm_prd_info - invalid prd_line', COUNT(*)
FROM silver.crm_prd_info
WHERE prd_line NOT IN ('Mountain', 'Road', 'Touring', 'Other Sales', 'n/a')

UNION ALL
-- crm_camp_info: channel must be one of 8 canonical values + Other
SELECT 'silver.crm_camp_info - invalid cmp_channel', COUNT(*)
FROM silver.crm_camp_info
WHERE cmp_channel NOT IN (
    'Email', 'Social Media', 'SMS', 'Direct Mail',
    'Online', 'TV', 'Radio', 'Events', 'Other'
)

UNION ALL
-- crm_supp_tkts: status must be one of 4 canonical values + Unknown
SELECT 'silver.crm_supp_tkts - invalid tkt_status', COUNT(*)
FROM silver.crm_supp_tkts
WHERE tkt_status NOT IN ('Open', 'Closed', 'Resolved', 'Pending', 'Unknown')

UNION ALL
-- crm_supp_tkts: issue_category must be canonical + Unknown
SELECT 'silver.crm_supp_tkts - invalid tkt_issue_cat', COUNT(*)
FROM silver.crm_supp_tkts
WHERE tkt_issue_cat NOT IN (
    'Billing', 'Technical', 'Shipping', 'Returns',
    'Product', 'Account', 'General', 'Unknown'
)

UNION ALL
-- erp_cust_az12: gender must be Male / Female / n/a
SELECT 'silver.erp_cust_az12 - invalid gen', COUNT(*)
FROM silver.erp_cust_az12
WHERE gen NOT IN ('Male', 'Female', 'n/a')

UNION ALL
-- erp_hr_hx19: branch_id must never be NULL or empty (mapped to Unknown in silver)
SELECT 'silver.erp_hr_hx19 - NULL or empty emp_branch_id', COUNT(*)
FROM silver.erp_hr_hx19
WHERE emp_branch_id IS NULL OR TRIM(emp_branch_id) = ''

UNION ALL
-- erp_vnd_z90: vnd_name must never be NULL (mapped to Unknown Vendor in silver)
SELECT 'silver.erp_vnd_z90 - NULL vnd_name (should be Unknown Vendor)', COUNT(*)
FROM silver.erp_vnd_z90
WHERE vnd_name IS NULL OR TRIM(vnd_name) = ''

UNION ALL
-- erp_inv_q4v1: stock_on_hand must never be negative (floored to 0 in silver)
SELECT 'silver.erp_inv_q4v1 - negative inv_stock_on_hand', COUNT(*)
FROM silver.erp_inv_q4v1
WHERE inv_stock_on_hand < 0

UNION ALL
-- erp_inv_q4v1: below_reorder flag consistency check
-- If stock < reorder level, flag must be 1; otherwise must be 0
SELECT 'silver.erp_inv_q4v1 - below_reorder flag mismatch', COUNT(*)
FROM silver.erp_inv_q4v1
WHERE below_reorder != CASE
    WHEN inv_stock_on_hand < inv_reorder_level THEN 1
    ELSE 0
END

UNION ALL
-- erp_po_ord44: flag_missing_vendor consistency
-- flag must be 1 if vendor is NULL; must be 0 if vendor exists in silver
SELECT 'silver.erp_po_ord44 - flag_missing_vendor set but vendor exists', COUNT(*)
FROM silver.erp_po_ord44 p
INNER JOIN silver.erp_vnd_z90 v ON p.po_vnd_id = v.vnd_id
WHERE p.flag_missing_vendor = 1

UNION ALL
-- erp_po_ord44: flag must be 1 when vendor is NULL
SELECT 'silver.erp_po_ord44 - flag_missing_vendor=0 but vendor is NULL', COUNT(*)
FROM silver.erp_po_ord44
WHERE po_vnd_id IS NULL AND flag_missing_vendor = 0;


-- ============================================================================
-- S5. DATE LOGIC CHECKS — Silver
-- ============================================================================
PRINT '================================================================';
PRINT '== S5. Date Logic Checks — Silver ==';
PRINT '================================================================';

-- crm_sales_details: shipping date must not be before order date
SELECT 'silver.crm_sales_details - ship_dt before order_dt'   AS check_name,
       COUNT(*) AS issue_count
FROM silver.crm_sales_details
WHERE sls_ship_dt < sls_order_dt

UNION ALL
-- crm_sales_details: due date must not be before order date
SELECT 'silver.crm_sales_details - due_dt before order_dt', COUNT(*)
FROM silver.crm_sales_details
WHERE sls_due_dt < sls_order_dt

UNION ALL
-- crm_prd_info: end date must not be before start date (where end is not NULL)
SELECT 'silver.crm_prd_info - prd_end_dt before prd_start_dt', COUNT(*)
FROM silver.crm_prd_info
WHERE prd_end_dt IS NOT NULL AND prd_end_dt < prd_start_dt

UNION ALL
-- crm_camp_info: non-flagged rows must have valid date range
SELECT 'silver.crm_camp_info - end_date < start_date (flag_invalid_date_range=0)', COUNT(*)
FROM silver.crm_camp_info
WHERE cmp_end_date < cmp_start_date
  AND flag_invalid_date_range = 0

UNION ALL
-- crm_camp_info: flagged rows must actually have an invalid date range
SELECT 'silver.crm_camp_info - flagged as invalid but dates are valid', COUNT(*)
FROM silver.crm_camp_info
WHERE flag_invalid_date_range = 1
  AND cmp_end_date >= cmp_start_date

UNION ALL
-- crm_supp_tkts: resolution date must not be before open date
SELECT 'silver.crm_supp_tkts - resolution_date before open_date', COUNT(*)
FROM silver.crm_supp_tkts
WHERE tkt_resolution_date IS NOT NULL
  AND tkt_resolution_date < tkt_open_date

UNION ALL
-- erp_cust_az12: birth date must not be in the future
SELECT 'silver.erp_cust_az12 - future bdate', COUNT(*)
FROM silver.erp_cust_az12
WHERE bdate > CAST(GETDATE() AS DATE)

UNION ALL
-- erp_hr_hx19: hire date must not be in the future
SELECT 'silver.erp_hr_hx19 - future emp_hire_date', COUNT(*)
FROM silver.erp_hr_hx19
WHERE emp_hire_date > CAST(GETDATE() AS DATE)

UNION ALL
-- erp_po_ord44: order date must not be in the future
SELECT 'silver.erp_po_ord44 - future po_order_date', COUNT(*)
FROM silver.erp_po_ord44
WHERE po_order_date > CAST(GETDATE() AS DATE)

UNION ALL
-- erp_inv_q4v1: snapshot date must not be in the future
SELECT 'silver.erp_inv_q4v1 - future inv_snap_date', COUNT(*)
FROM silver.erp_inv_q4v1
WHERE inv_snap_date > CAST(GETDATE() AS DATE);


-- ============================================================================
-- S6. BUSINESS RULE CHECKS — Silver
-- ============================================================================
PRINT '================================================================';
PRINT '== S6. Business Rule Checks — Silver ==';
PRINT '================================================================';

-- crm_sales_details: sales, quantity, and price must all be positive
SELECT 'silver.crm_sales_details - zero or negative sls_sales'     AS check_name,
       COUNT(*) AS issue_count
FROM silver.crm_sales_details WHERE sls_sales <= 0
UNION ALL
SELECT 'silver.crm_sales_details - zero or negative sls_quantity', COUNT(*)
FROM silver.crm_sales_details WHERE sls_quantity <= 0
UNION ALL
SELECT 'silver.crm_sales_details - zero or negative sls_price', COUNT(*)
FROM silver.crm_sales_details WHERE sls_price <= 0

UNION ALL
-- crm_prd_info: product cost must be >= 0 (silver sets NULL cost → 0)
SELECT 'silver.crm_prd_info - negative prd_cost', COUNT(*)
FROM silver.crm_prd_info WHERE prd_cost < 0

UNION ALL
-- crm_camp_info: budget must be >= 0 (silver sets NULL → 0; negatives → ABS)
SELECT 'silver.crm_camp_info - negative cmp_budget', COUNT(*)
FROM silver.crm_camp_info WHERE cmp_budget < 0

UNION ALL
-- erp_inv_q4v1: reorder level must be positive
SELECT 'silver.erp_inv_q4v1 - zero or negative inv_reorder_level', COUNT(*)
FROM silver.erp_inv_q4v1 WHERE inv_reorder_level <= 0

UNION ALL
-- erp_po_ord44: quantity must be positive (silver rounds fractional values)
SELECT 'silver.erp_po_ord44 - zero or negative po_quantity_ordered', COUNT(*)
FROM silver.erp_po_ord44 WHERE po_quantity_ordered <= 0

UNION ALL
-- erp_po_ord44: total_cost consistency check
-- total_cost should equal quantity * unit_cost (tolerance for float rounding)
SELECT 'silver.erp_po_ord44 - total_cost mismatch vs qty * unit_cost', COUNT(*)
FROM silver.erp_po_ord44
WHERE total_cost IS NOT NULL
  AND po_unit_cost IS NOT NULL
  AND ABS(total_cost - (po_quantity_ordered * po_unit_cost)) > 0.01

UNION ALL
-- erp_hr_hx19: emp_full_name must match concat of first + last name
SELECT 'silver.erp_hr_hx19 - emp_full_name mismatch (first + last)', COUNT(*)
FROM silver.erp_hr_hx19
WHERE emp_full_name != CONCAT(emp_first_name, ' ', emp_last_name);


-- ============================================================================
-- S7. ROW COUNT SUMMARY — Silver
-- ============================================================================
PRINT '================================================================';
PRINT '== S7. Silver Row Count Summary ==';
PRINT '================================================================';

SELECT 'silver.crm_cust_info'    AS table_name, COUNT(*) AS row_count FROM silver.crm_cust_info
UNION ALL
SELECT 'silver.crm_prd_info',     COUNT(*) FROM silver.crm_prd_info
UNION ALL
SELECT 'silver.crm_sales_details',COUNT(*) FROM silver.crm_sales_details
UNION ALL
SELECT 'silver.crm_camp_info',    COUNT(*) FROM silver.crm_camp_info
UNION ALL
SELECT 'silver.crm_supp_tkts',    COUNT(*) FROM silver.crm_supp_tkts
UNION ALL
SELECT 'silver.erp_cust_az12',    COUNT(*) FROM silver.erp_cust_az12
UNION ALL
SELECT 'silver.erp_loc_a101',     COUNT(*) FROM silver.erp_loc_a101
UNION ALL
SELECT 'silver.erp_px_cat_g1v2',  COUNT(*) FROM silver.erp_px_cat_g1v2
UNION ALL
SELECT 'silver.erp_hr_hx19',      COUNT(*) FROM silver.erp_hr_hx19
UNION ALL
SELECT 'silver.erp_vnd_z90',      COUNT(*) FROM silver.erp_vnd_z90
UNION ALL
SELECT 'silver.erp_inv_q4v1',     COUNT(*) FROM silver.erp_inv_q4v1
UNION ALL
SELECT 'silver.erp_po_ord44',     COUNT(*) FROM silver.erp_po_ord44
ORDER BY 1;

