/*
===============================================================================
Stored Procedure: silver.load_silver
Description:      Cleanses, normalizes, and loads data from the bronze layer 
                  into the silver layer tables.
                  Includes print messages, error handling, and duration tracking.
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME;
    DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '==================================================================';
        PRINT 'Starting Execution: Loading Silver Layer';
        PRINT '==================================================================';

        PRINT '------------------------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '------------------------------------------------------------------';

        -- ---------------------------------------------------------------------------
        -- 1. Load silver.crm_cust_info
        -- =============================================================================
        -- Issues resolved:
        --   - cst_firstname / cst_lastname: TRIM whitespace
        --   - cst_marital_status: Normalized 'S' to 'Single', 'M' to 'Married', NULLs to 'n/a'
        --   - cst_gndr: Normalized 'F' to 'Female', 'M' to 'Male', NULLs to 'n/a'
        --   - duplicates: Removed duplicates by keeping the most recent cst_create_date per cst_id
        -- =============================================================================
        -- ---------------------------------------------------------------------------
        PRINT '>> Truncating and Loading Table: silver.crm_cust_info...';
        SET @start_time = GETDATE();

        TRUNCATE TABLE silver.crm_cust_info;
        
        INSERT INTO silver.crm_cust_info(
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date
        )
        SELECT 
            cst_id,
            cst_key,
            TRIM(cst_firstname) cst_firstname,    
            TRIM(cst_lastname) cst_lastname,
            CASE WHEN UPPER(TRIM(cst_marital_status))='S' THEN 'Single' 
                 WHEN UPPER(TRIM(cst_marital_status))='M' THEN 'Married'
                 ELSE 'n/a'                       
            END cst_marital_status,               
            CASE WHEN UPPER(TRIM(cst_gnder))='F' THEN 'Female'
                 WHEN UPPER(TRIM(cst_gnder))='M' THEN 'Male'
                 ELSE 'n/a'
            END cst_gndr,
            cst_create_date
        FROM (
            SELECT *,
            ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) flag_last
            FROM bronze.crm_cust_info 
            WHERE cst_id IS NOT NULL
        ) a
        WHERE a.flag_last=1 ;                     

        SET @end_time = GETDATE();
        PRINT '<< Success: silver.crm_cust_info loaded. Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds.';
        PRINT '------------------------------------------------------------------';

        -- ---------------------------------------------------------------------------
        -- 2. Load silver.crm_prd_info
        -- =============================================================================
        -- Issues resolved:
        --   - cat_id: Extracted category from prd_key and replaced hyphens with underscores
        --   - prd_key: Extracted clean product key from raw string
        --   - prd_cost: Handled NULLs by substituting with 0
        --   - prd_line: Mapped codes (M, R, S, T) to descriptive values
        --   - prd_end_dt: Calculated end date as one day before the next start date
        -- =============================================================================
        -- ---------------------------------------------------------------------------
        PRINT '>> Truncating and Loading Table: silver.crm_prd_info...';
        SET @start_time = GETDATE();

        TRUNCATE TABLE silver.crm_prd_info;
        
        INSERT INTO silver.crm_prd_info(
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        ) 
        SELECT 
            prd_id,
            REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
            SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,     
            prd_nm,
            ISNULL(prd_cost,0) AS prd_cost,
            CASE UPPER(TRIM(prd_line))
                 WHEN 'M' THEN 'Mountain'
                 WHEN 'R' THEN 'Road' 
                 WHEN 'S' THEN 'Other Sales'
                 WHEN 'T' THEN 'Touring'
                 ELSE 'n/a'
            END AS prd_line,                                  
            CAST(prd_start_dt AS DATE) AS prd_start_dt,
            CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) 
            AS prd_end_dt                                     
        FROM bronze.crm_prd_info ;

        SET @end_time = GETDATE();
        PRINT '<< Success: silver.crm_prd_info loaded. Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds.';
        PRINT '------------------------------------------------------------------';

        -- ---------------------------------------------------------------------------
        -- 3. Load silver.crm_sales_details
        -- =============================================================================
        -- Issues resolved:
        --   - sls_order_dt / sls_ship_dt / sls_due_dt: Handled invalid integers (0) or incorrect lengths → NULL, cast to DATE
        --   - sls_sales: Recalculated invalid, NULL, or negative sales using quantity * ABS(price)
        --   - sls_price: Recalculated invalid, NULL, or negative prices using sales / quantity
        -- =============================================================================
        -- ---------------------------------------------------------------------------
        PRINT '>> Truncating and Loading Table: silver.crm_sales_details...';
        SET @start_time = GETDATE();

        TRUNCATE TABLE silver.crm_sales_details;
        
        INSERT INTO silver.crm_sales_details(
            sls_ord_num,
            sls_prd_key,
            sls_cst_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        )  
        SELECT 
            sls_ord_num,
            sls_prd_key,
            sls_cst_id,
            CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL 
                 ELSE CAST(CAST(sls_order_dt AS NVARCHAR) AS DATE)
            END AS sls_order_dt,
            CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL 
                 ELSE CAST(CAST(sls_ship_dt AS NVARCHAR) AS DATE)
            END AS sls_ship_dt,
            CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL 
                 ELSE CAST(CAST(sls_due_dt AS NVARCHAR) AS DATE)
            END AS sls_due_dt,
            CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
                 THEN sls_quantity * ABS(ISNULL(sls_price,1))
                 ELSE sls_sales
            END AS sls_sales,
            sls_quantity,
            CASE WHEN sls_price IS NULL OR sls_price <= 0 
                 THEN sls_sales / NULLIF(sls_quantity,0)
                 ELSE sls_price
            END AS sls_price
        FROM bronze.crm_sales_details;

        SET @end_time = GETDATE();
        PRINT '<< Success: silver.crm_sales_details loaded. Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds.';
        PRINT '------------------------------------------------------------------';

        -- ---------------------------------------------------------------------------
        -- 4. Load silver.crm_camp_info
        -- =============================================================================
        -- Issues resolved:
        --   - channel: 37 variants → 8 clean canonical values
        --   - budget: 17 NULLs → 0; 8 negative values → ABS()
        --   - start_date / end_date: mixed MM/DD/YYYY and YYYY-MM-DD → DATE
        --   - campaign_name: TRIM whitespace; trailing spaces & ALL CAPS variants
        --   - flag_invalid_date_range: 38 rows where end_date < start_date
        -- =============================================================================
        -- ---------------------------------------------------------------------------
        PRINT '>> Truncating and Loading Table: silver.crm_camp_info...';
        SET @start_time = GETDATE();

        TRUNCATE TABLE silver.crm_camp_info;

        INSERT INTO silver.crm_camp_info (
            cmp_id, cmp_name, cmp_channel,
            cmp_budget, cmp_start_date, cmp_end_date, flag_invalid_date_range
        )
        SELECT  
            cmp_id,
            TRIM(cmp_name) AS cmp_name,
            CASE
                WHEN UPPER(TRIM(cmp_channel)) IN ('EMAIL','E-MAIL','E_MAIL') THEN 'Email'
                WHEN UPPER(TRIM(cmp_channel)) IN ('SOCIAL MEDIA','SOCIAL_MEDIA','SOCIALMEDIA') THEN 'Social Media'
                WHEN UPPER(TRIM(cmp_channel)) IN ('SMS','TEXT','SMS') THEN 'SMS'
                WHEN UPPER(TRIM(cmp_channel)) IN ('DIRECT MAIL','DIRECT_MAIL','DM','DIRECTMAIL') THEN 'Direct Mail'
                WHEN UPPER(TRIM(cmp_channel)) IN ('ONLINE','WEB') THEN 'Online'
                WHEN UPPER(TRIM(cmp_channel)) IN ('TV','TELEVISION','TV AD') THEN 'TV'
                WHEN UPPER(TRIM(cmp_channel)) IN ('RADIO','RADIO AD') THEN 'Radio'
                WHEN UPPER(TRIM(cmp_channel)) IN ('EVENTS','EVENT','IN-PERSON','IN PERSON') THEN 'Events'
                ELSE 'Other'
            END AS cmp_channel,
            CASE
                WHEN TRY_CAST(cmp_budget AS FLOAT) IS NULL THEN 0
                WHEN TRY_CAST(cmp_budget AS FLOAT) < 0 THEN ABS(TRY_CAST(cmp_budget AS FLOAT))
                ELSE TRY_CAST(cmp_budget AS FLOAT)
            END AS cmp_budget,
            CASE
                WHEN cmp_start_date LIKE '__/__/____' THEN TRY_CONVERT(DATE, cmp_start_date, 101)
                ELSE TRY_CONVERT(DATE, cmp_start_date, 23)
            END AS cmp_start_date,
            CASE
                WHEN cmp_end_date LIKE '__/__/____' THEN TRY_CONVERT(DATE, cmp_end_date, 101)
                ELSE TRY_CONVERT(DATE, cmp_end_date, 23)
            END AS cmp_end_date,
            CASE
                WHEN (CASE WHEN cmp_end_date LIKE '__/__/____' THEN TRY_CONVERT(DATE,cmp_end_date,101) ELSE TRY_CONVERT(DATE,cmp_end_date,23) END)
                   < (CASE WHEN cmp_start_date LIKE '__/__/____' THEN TRY_CONVERT(DATE,cmp_start_date,101) ELSE TRY_CONVERT(DATE,cmp_start_date,23) END)
                THEN 1
                ELSE 0
            END AS flag_invalid_date_range
        FROM bronze.crm_camp_info;

        SET @end_time = GETDATE();
        PRINT '<< Success: silver.crm_camp_info loaded. Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds.';
        PRINT '------------------------------------------------------------------';

        -- ---------------------------------------------------------------------------
        -- 5. Load silver.crm_supp_tkts
        -- =============================================================================
        -- Issues resolved:
        --   - status: 20 variants → 4 canonical (Open/Closed/Resolved/Pending)
        --   - issue_category: mixed casing (21 variants) → 7 canonical Title Case
        --   - prd_key: 451 NULLs kept as NULL; 718 bad keys fixed
        --              underscore-delimited → hyphen; wrong 'PRD-' prefix stripped
        --   - cst_id: 307 NULLs kept; 269 ghost IDs (999999) → NULL
        --   - emp_id: 451 NULLs kept; invalid EMP9xxx (>EMP300) → NULL
        --   - open_date / resolution_date: mixed formats → DATE
        -- =============================================================================
        -- ---------------------------------------------------------------------------
        PRINT '>> Truncating and Loading Table: silver.crm_supp_tkts...';
        SET @start_time = GETDATE();

        TRUNCATE TABLE silver.crm_supp_tkts;

        INSERT INTO silver.crm_supp_tkts (
            tkt_id, tkt_cst_id, tkt_prd_key, tkt_emp_id,
            tkt_issue_cat, tkt_status, tkt_open_date, tkt_resolution_date
        )
        SELECT
            tkt_id,
            CASE
                WHEN TRIM(tkt_cst_id) = '' OR tkt_cst_id IS NULL THEN NULL
                WHEN TRY_CAST(tkt_cst_id AS INT) = 999999 THEN NULL
                ELSE TRY_CAST(tkt_cst_id AS INT)
            END AS tkt_cst_id,
            CASE
                WHEN TRIM(tkt_prd_key) = '' OR tkt_prd_key IS NULL THEN NULL
                WHEN tkt_prd_key LIKE 'PRD-%' THEN SUBSTRING(TRIM(tkt_prd_key), 5, 100)
                WHEN tkt_prd_key LIKE '%[_]%' THEN REPLACE(TRIM(tkt_prd_key), '_', '-')
                ELSE TRIM(tkt_prd_key)
            END AS tkt_prd_key,
            CASE
                WHEN TRIM(tkt_emp_id) = '' OR tkt_emp_id IS NULL THEN NULL
                WHEN TRY_CAST(SUBSTRING(TRIM(tkt_emp_id), 4, 10) AS INT) > 300 THEN NULL
                ELSE TRIM(tkt_emp_id)
            END AS tkt_emp_id,
            CASE
                WHEN UPPER(TRIM(tkt_issue_cat)) = 'BILLING' THEN 'Billing'
                WHEN UPPER(TRIM(tkt_issue_cat)) = 'TECHNICAL' THEN 'Technical'
                WHEN UPPER(TRIM(tkt_issue_cat)) = 'SHIPPING' THEN 'Shipping'
                WHEN UPPER(TRIM(tkt_issue_cat)) = 'RETURNS' THEN 'Returns'
                WHEN UPPER(TRIM(tkt_issue_cat)) = 'PRODUCT' THEN 'Product'
                WHEN UPPER(TRIM(tkt_issue_cat)) = 'ACCOUNT' THEN 'Account'
                WHEN UPPER(TRIM(tkt_issue_cat)) = 'GENERAL' THEN 'General'
                ELSE 'Unknown'
            END AS tkt_issue_cat,
            CASE
                WHEN UPPER(TRIM(tkt_status)) IN ('OPEN','OPENED','O') THEN 'Open'
                WHEN UPPER(TRIM(tkt_status)) IN ('CLOSED','CLOSE','CLSD') THEN 'Closed'
                WHEN UPPER(TRIM(tkt_status)) IN ('RESOLVED','RESOLVE','RSOLVD') THEN 'Resolved'
                WHEN UPPER(TRIM(tkt_status)) IN ('PENDING','PEND','IN PROGRESS','IN_PROGRESS') THEN 'Pending'
                ELSE 'Unknown'
            END AS tkt_status,
            CASE
                WHEN tkt_open_date LIKE '__/__/____' THEN TRY_CONVERT(DATE, tkt_open_date, 101)
                ELSE TRY_CONVERT(DATE, tkt_open_date, 23)
            END AS tkt_open_date,
            CASE
                WHEN TRIM(tkt_resolution_date) = '' OR tkt_resolution_date IS NULL THEN NULL
                WHEN tkt_resolution_date LIKE '__/__/____' THEN TRY_CONVERT(DATE, tkt_resolution_date, 101)
                ELSE TRY_CONVERT(DATE, tkt_resolution_date, 23)
            END AS tkt_resolution_date
        FROM bronze.crm_supp_tkts;

        SET @end_time = GETDATE();
        PRINT '<< Success: silver.crm_supp_tkts loaded. Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds.';
        PRINT '------------------------------------------------------------------';
        
        PRINT '==================================================================';
        PRINT '------------------------------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '------------------------------------------------------------------';
        
        -- ---------------------------------------------------------------------------
        -- 6. Load silver.erp_cust_az12
        -- =============================================================================
        -- Issues resolved:
        --   - cid: Stripped 'NAS' prefix if present
        --   - bdate: Identified and set future birthdates to NULL
        --   - gen: Normalized variations of Male/Female to 'Male' and 'Female', others to 'n/a'
        -- =============================================================================
        -- ---------------------------------------------------------------------------
        PRINT '>> Truncating and Loading Table: silver.erp_cust_az12...';
        SET @start_time = GETDATE();

        TRUNCATE TABLE silver.erp_cust_az12;
        
        INSERT INTO silver.erp_cust_az12 (
            cid,
            bdate,
            gen
        )
        SELECT 
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
                 ELSE cid
            END AS cid,
            CASE WHEN bdate > GETDATE() THEN NULL
                 ELSE bdate 
            END AS bdate,                                     
            CASE WHEN UPPER(TRIM(gen)) IN ('F','Female') THEN 'Female'
                 WHEN UPPER(TRIM(gen)) IN ('M','Male') THEN 'Male'
                 ELSE 'n/a' 
            END AS gen                                        
        FROM bronze.erp_cust_az12;

        SET @end_time = GETDATE();
        PRINT '<< Success: silver.erp_cust_az12 loaded. Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds.';
        PRINT '------------------------------------------------------------------';

        -- ---------------------------------------------------------------------------
        -- 7. Load silver.erp_loc_a101
        -- =============================================================================
        -- Issues resolved:
        --   - cid: Removed hyphens from ID string
        --   - cntry: Normalized country codes/names (DE -> Germany, US/USA -> United States), blanks/NULLs to 'n/a'
        -- =============================================================================
        -- ---------------------------------------------------------------------------
        PRINT '>> Truncating and Loading Table: silver.erp_loc_a101...';
        SET @start_time = GETDATE();

        TRUNCATE TABLE silver.erp_loc_a101;
        
        INSERT INTO silver.erp_loc_a101(
            cid,
            cntry
        )
        SELECT 
            REPLACE(cid,'-','') AS cid,
            CASE WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
                 WHEN UPPER(TRIM(cntry)) IN ('US','USA','United States') THEN 'United States'
                 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a' 
                 ELSE TRIM(cntry)  
            END AS cntry                                      
        FROM bronze.erp_loc_a101;

        SET @end_time = GETDATE();
        PRINT '<< Success: silver.erp_loc_a101 loaded. Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds.';
        PRINT '------------------------------------------------------------------';

        -- ---------------------------------------------------------------------------
        -- 8. Load silver.erp_px_cat_g1v2
        -- =============================================================================
        -- Issues resolved:
        --   - Direct load: No data transformations required for this dimension
        -- =============================================================================
        -- ---------------------------------------------------------------------------
        PRINT '>> Truncating and Loading Table: silver.erp_px_cat_g1v2...';
        SET @start_time = GETDATE();

        TRUNCATE TABLE silver.erp_px_cat_g1v2;
        
        INSERT INTO silver.erp_px_cat_g1v2(
            id,
            cat,
            subcat,
            maintenance
        )
        SELECT 
            id,
            cat,
            subcat,
            maintenance
        FROM bronze.erp_px_cat_g1v2;

        SET @end_time = GETDATE();
        PRINT '<< Success: silver.erp_px_cat_g1v2 loaded. Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds.';
        PRINT '------------------------------------------------------------------';

        -- ---------------------------------------------------------------------------
        -- 9. Load silver.erp_hr_hx19
        -- =============================================================================
        -- Issues resolved:
        --   - emp_id: 9 duplicates → keep latest hire_date via ROW_NUMBER
        --   - first_name / last_name: 8/4 NULLs → 'N/A'; TRIM whitespace
        --   - role: 40 variants → 10 canonical job titles
        --   - branch_id: 15 NULLs → 'Unknown'
        --   - hire_date: mixed MM/DD/YYYY and YYYY-MM-DD → DATE
        -- =============================================================================
        -- ---------------------------------------------------------------------------
        PRINT '>> Truncating and Loading Table: silver.erp_hr_hx19...';
        SET @start_time = GETDATE();

        TRUNCATE TABLE silver.erp_hr_hx19;

        ;WITH parsed AS (
            SELECT
                emp_id,
                TRIM(COALESCE(NULLIF(TRIM(emp_first_name), ''), 'N/A')) AS emp_first_name,
                TRIM(COALESCE(NULLIF(TRIM(emp_last_name),  ''), 'N/A')) AS emp_last_name,
                CASE
                    WHEN UPPER(TRIM(emp_role)) IN ('MANAGER','MGR','MG','MANAGER') THEN 'Manager'
                    WHEN UPPER(TRIM(emp_role)) IN ('SALES REPRESENTATIVE','SALES REP','SLS_REP','SR') THEN 'Sales Representative'
                    WHEN UPPER(TRIM(emp_role)) IN ('ANALYST','ANLST','ANALST') THEN 'Analyst'
                    WHEN UPPER(TRIM(emp_role)) IN ('ENGINEER','ENG','ENGR') THEN 'Engineer'
                    WHEN UPPER(TRIM(emp_role)) IN ('SUPPORT AGENT','SUP AGENT','SUPP_AGT') THEN 'Support Agent'
                    WHEN UPPER(TRIM(emp_role)) IN ('TEAM LEAD','TL','T.LEAD','TEAM_LEAD') THEN 'Team Lead'
                    WHEN UPPER(TRIM(emp_role)) IN ('DIRECTOR','DIR','DIR.') THEN 'Director'
                    WHEN UPPER(TRIM(emp_role)) IN ('COORDINATOR','COORD','CO-ORD') THEN 'Coordinator'
                    WHEN UPPER(TRIM(emp_role)) IN ('SPECIALIST','SPEC','SPCL') THEN 'Specialist'
                    WHEN UPPER(TRIM(emp_role)) IN ('CONSULTANT','CNSLT','CONSULT') THEN 'Consultant'
                    ELSE TRIM(emp_role)
                END AS emp_role,
                COALESCE(NULLIF(TRIM(emp_branch_id), ''), 'Unknown') AS emp_branch_id,
                CASE
                    WHEN emp_hire_date LIKE '__/__/____' THEN TRY_CONVERT(DATE, emp_hire_date, 101)
                    ELSE TRY_CONVERT(DATE, emp_hire_date, 23)
                END AS emp_hire_date
            FROM bronze.erp_hr_hx19
        ),
        deduped AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY emp_id
                       ORDER BY emp_hire_date DESC
                   ) AS rn
            FROM parsed
        )
        INSERT INTO silver.erp_hr_hx19 (
            emp_id, emp_first_name, emp_last_name, emp_full_name, emp_role, emp_branch_id, emp_hire_date
        )
        SELECT
            emp_id,
            emp_first_name,
            emp_last_name,
            CONCAT(emp_first_name, ' ', emp_last_name) AS emp_full_name,
            emp_role,
            emp_branch_id,
            emp_hire_date
        FROM deduped
        WHERE rn = 1;

        SET @end_time = GETDATE();
        PRINT '<< Success: silver.erp_hr_hx19 loaded. Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds.';
        PRINT '------------------------------------------------------------------';

        -- ---------------------------------------------------------------------------
        -- 10. Load silver.erp_vnd_z90
        -- =============================================================================
        -- Issues resolved:
        --   - vendor_id: 9 duplicates → keep first occurrence via ROW_NUMBER
        --   - vendor_name: 5 NULLs → 'Unknown Vendor'; TRIM whitespace
        --   - country: 31 variants → 10 canonical country names
        -- =============================================================================
        -- ---------------------------------------------------------------------------
        PRINT '>> Truncating and Loading Table: silver.erp_vnd_z90...';
        SET @start_time = GETDATE();

        TRUNCATE TABLE silver.erp_vnd_z90;

        ;WITH deduped AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY vnd_id
                       ORDER BY (SELECT NULL)  
                   ) AS rn
            FROM bronze.erp_vnd_z90
        )
        INSERT INTO silver.erp_vnd_z90 (vnd_id, vnd_name, vnd_country)
        SELECT
            vnd_id,
            TRIM(COALESCE(NULLIF(TRIM(vnd_name), ''), 'Unknown Vendor')),
            CASE
                WHEN UPPER(TRIM(vnd_country)) IN ('USA','US','UNITED STATES','U.S.A','U.S.') THEN 'United States'
                WHEN UPPER(TRIM(vnd_country)) IN ('UK','UNITED KINGDOM','GBR','U.K.','ENGLAND','GB') THEN 'United Kingdom'
                WHEN UPPER(TRIM(vnd_country)) IN ('GERMANY','DE','GER','DEUTSCHLAND','DEU') THEN 'Germany'
                WHEN UPPER(TRIM(vnd_country)) IN ('FRANCE','FR','FRA') THEN 'France'
                WHEN UPPER(TRIM(vnd_country)) IN ('CANADA','CAN','CA') THEN 'Canada'
                WHEN UPPER(TRIM(vnd_country)) IN ('AUSTRALIA','AUS','AU') THEN 'Australia'
                WHEN UPPER(TRIM(vnd_country)) IN ('JAPAN','JPN','JP') THEN 'Japan'
                WHEN UPPER(TRIM(vnd_country)) IN ('INDIA','IND','IN') THEN 'India'
                WHEN UPPER(TRIM(vnd_country)) IN ('SPAIN','ESP','SP') THEN 'Spain'
                WHEN UPPER(TRIM(vnd_country)) IN ('ITALY','ITA','IT') THEN 'Italy'
                ELSE TRIM(vnd_country)
            END AS vnd_country
        FROM deduped
        WHERE rn = 1;
        
        SET @end_time = GETDATE();
        PRINT '<< Success: silver.erp_vnd_z90 loaded. Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds.';
        PRINT '------------------------------------------------------------------';

        -- ---------------------------------------------------------------------------
        -- 11. Load silver.erp_inv_q4v1
        -- =============================================================================
        -- Issues resolved:
        --   - inv_id: 24 duplicates → keep latest snapshot_date via ROW_NUMBER
        --   - prd_id: 47 orphan IDs (>397, not in prd_info) → excluded
        --   - warehouse_loc: 30 casing variants + trailing spaces → UPPER(TRIM)
        --   - stock_on_hand: 48 negative values → 0 (floor at zero)
        --   - snapshot_date: mixed formats → DATE
        -- =============================================================================
        -- ---------------------------------------------------------------------------
        PRINT '>> Truncating and Loading Table: silver.erp_inv_q4v1...';
        SET @start_time = GETDATE();

        TRUNCATE TABLE silver.erp_inv_q4v1;

        ;WITH parsed AS (
            SELECT
                inv_id,
                TRY_CAST(inv_prd_id AS INT) AS inv_prd_id,
                UPPER(TRIM(inv_wh_loc)) AS inv_wh_loc,
                CASE
                    WHEN TRY_CAST(inv_stock_on_hand AS INT) < 0 THEN 0
                    ELSE TRY_CAST(inv_stock_on_hand AS INT)
                END AS inv_stock_on_hand,
                TRY_CAST(inv_reorder_level AS INT) AS inv_reorder_level,
                CASE
                    WHEN inv_snap_date LIKE '__/__/____' THEN TRY_CONVERT(DATE, inv_snap_date, 101)
                    ELSE TRY_CONVERT(DATE, inv_snap_date, 23)
                END AS inv_snap_date
            FROM bronze.erp_inv_q4v1
            WHERE TRY_CAST(inv_prd_id AS INT) <= 397
              AND TRY_CAST(inv_prd_id AS INT) IS NOT NULL
        ),
        deduped AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY inv_id
                       ORDER BY inv_snap_date DESC   
                   ) AS rn
            FROM parsed
        )
        INSERT INTO silver.erp_inv_q4v1 (
            inv_id, inv_prd_id, inv_wh_loc,
            inv_stock_on_hand, inv_reorder_level, below_reorder, inv_snap_date
        )
        SELECT
            inv_id,
            inv_prd_id,
            inv_wh_loc,
            inv_stock_on_hand,
            inv_reorder_level,
            CASE WHEN inv_stock_on_hand < inv_reorder_level THEN 1 ELSE 0 END AS below_reorder,
            inv_snap_date
        FROM deduped
        WHERE rn = 1;

        SET @end_time = GETDATE();
        PRINT '<< Success: silver.erp_inv_q4v1 loaded. Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds.';
        PRINT '------------------------------------------------------------------';

        -- ---------------------------------------------------------------------------
        -- 12. Load silver.erp_po_ord44
        -- =============================================================================
        -- Issues resolved:
        --   - po_number: 107 duplicates → keep latest order_date via ROW_NUMBER
        --   - vendor_id: 159 NULLs kept as NULL; 69 unresolvable IDs kept (flagged)
        --   - quantity_ordered: Float → INT via ROUND; 374 fractional values corrected
        --   - unit_cost: 4 patterns cleaned:
        --       '$XX.XX'  → strip '$', cast to FLOAT
        --       'N/A'     → NULL
        --       NULL/''   → NULL
        --       'integer' → cast to FLOAT
        --   - order_date: mixed formats → DATE
        -- =============================================================================
        -- ---------------------------------------------------------------------------
        PRINT '>> Truncating and Loading Table: silver.erp_po_ord44...';
        SET @start_time = GETDATE();

        TRUNCATE TABLE silver.erp_po_ord44;
        
        ;WITH parsed AS (
            SELECT
                po_number,
                NULLIF(TRIM(po_vnd_id), '') AS po_vnd_id,
                TRY_CAST(po_prd_id AS INT) AS po_prd_id,
                CAST(ROUND(TRY_CAST(po_quantity_ordered AS FLOAT), 0) AS INT) AS po_quantity_ordered,
                CASE
                    WHEN TRIM(po_unit_cost) = '' OR po_unit_cost IS NULL THEN NULL
                    WHEN UPPER(TRIM(po_unit_cost)) IN ('N/A','NA','N.A.') THEN NULL
                    WHEN po_unit_cost LIKE '$%' THEN TRY_CAST(SUBSTRING(TRIM(po_unit_cost), 2, 50) AS FLOAT)
                    ELSE TRY_CAST(TRIM(po_unit_cost) AS FLOAT)
                END AS po_unit_cost,
                CASE
                    WHEN po_order_date LIKE '__/__/____' THEN TRY_CONVERT(DATE, po_order_date, 101)
                    ELSE TRY_CONVERT(DATE, po_order_date, 23)
                END AS po_order_date
            FROM bronze.erp_po_ord44
        ),
        deduped AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY po_number
                       ORDER BY po_order_date DESC   
                   ) AS rn
            FROM parsed
        )
        INSERT INTO silver.erp_po_ord44 (
            po_number, po_vnd_id, po_prd_id,
            po_quantity_ordered, po_unit_cost, total_cost,
            po_order_date, flag_missing_vendor
        )
        SELECT
            po_number,
            po_vnd_id,
            po_prd_id,
            po_quantity_ordered,
            po_unit_cost,
            CASE
                WHEN po_quantity_ordered IS NOT NULL AND po_unit_cost IS NOT NULL
                THEN CAST(po_quantity_ordered AS FLOAT) * po_unit_cost
                ELSE NULL
            END AS total_cost,
            po_order_date,
            CASE
                WHEN po_vnd_id IS NULL THEN 1
                WHEN po_vnd_id NOT IN (SELECT vnd_id FROM silver.erp_vnd_z90) THEN 1
                ELSE 0
            END AS flag_missing_vendor
        FROM deduped
        WHERE rn = 1;

        SET @end_time = GETDATE();
        PRINT '<< Success: silver.erp_po_ord44 loaded. Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds.';
        PRINT '------------------------------------------------------------------';

        -- ---------------------------------------------------------------------------
        -- Batch Completion
        -- ---------------------------------------------------------------------------
        SET @batch_end_time = GETDATE();
        PRINT '==================================================================';
        PRINT 'Execution Completed Successfully.';
        PRINT 'Total Load Duration for Silver Layer: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS VARCHAR) + ' seconds.';
        PRINT '==================================================================';

    END TRY
    BEGIN CATCH
        PRINT '==================================================================';
        PRINT 'ERROR OCCURRED DURING LOAD!';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number:  ' + CAST(ERROR_NUMBER() AS VARCHAR);
        PRINT 'Error Line:    ' + CAST(ERROR_LINE() AS VARCHAR);
        PRINT '==================================================================';
        
        -- Optionally, re-throw the error so the calling process knows it failed
        THROW;
    END CATCH
END;

GO

-- Execute the unified procedure
EXEC silver.load_silver;

