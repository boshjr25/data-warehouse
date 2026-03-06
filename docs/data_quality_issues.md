# Data Quality Issues — DWH Medallion Architecture

**Project:** SQL Server Data Warehouse — Medallion Architecture  
**Pipeline:** Bronze → Silver → Gold  
**Sources:** 5 CRM tables + 7 ERP tables (12 total)  
**Last Updated:** 2026

---

## How to Read This Document

Every issue is documented with four fields:

- **Issue** — what the raw data problem is  
- **Scope** — how many rows or values are affected  
- **Fix Applied in Silver** — the exact transformation used  
- **QC Check** — the validation query in `quality_checks_silver_gold.sql`

Issues marked ⚠️ are expected to produce non-zero results in QC (by design). All others expect 0.

---

## CRM Sources

### crm_cust_info — 18,494 raw rows → 18,484 silver rows

| # | Issue | Scope | Fix Applied in Silver |
|---|-------|-------|-----------------------|
| 1 | Duplicate `cst_id` rows — same customer appears multiple times with different `cst_create_date` values | 9 duplicate IDs (10 extra rows) | `ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC)` — keep most recent record per customer |
| 2 | NULL `cst_id` rows — records with no customer identifier, impossible to link | 4 rows | Excluded via `WHERE cst_id IS NOT NULL` before deduplication |
| 3 | `cst_marital_status` stored as single-letter codes `'M'` and `'S'` | All 18,494 rows | `CASE UPPER(TRIM(...)) WHEN 'M' THEN 'Married' WHEN 'S' THEN 'Single' ELSE 'n/a' END` |
| 4 | `cst_gndr` stored as single-letter codes `'M'` and `'F'`; 4,578 rows are blank/NULL | 4,578 blank + coded rows | `CASE UPPER(TRIM(...)) WHEN 'M' THEN 'Male' WHEN 'F' THEN 'Female' ELSE 'n/a' END` |
| 5 | Leading/trailing whitespace on `cst_firstname` and `cst_lastname` | Scattered across rows | `TRIM()` applied to both name columns |

**QC Checks:** S1 (NULLs), S3 (duplicates), S4 (marital_status and gender canonical values)

---

### crm_prd_info — 397 raw rows

| # | Issue | Scope | Fix Applied in Silver |
|---|-------|-------|-----------------------|
| 1 | `prd_key` contains a category prefix (e.g., `AC-HE-HL-U509-B`) — the first 5 characters are the category code, not part of the product key | All 397 rows | `REPLACE(SUBSTRING(prd_key,1,5),'-','_')` extracts `cat_id`; `SUBSTRING(prd_key,7,LEN(prd_key))` extracts clean `prd_key` |
| 2 | `prd_cost` is NULL for 2 rows — cost unknown | 2 rows | `ISNULL(prd_cost, 0)` — substituted with 0 |
| 3 | `prd_line` stored as single-letter codes: `M`, `R`, `S`, `T`; 17 rows are blank | 17 blank + all coded rows | `CASE WHEN 'M' THEN 'Mountain' WHEN 'R' THEN 'Road' WHEN 'S' THEN 'Other Sales' WHEN 'T' THEN 'Touring' ELSE 'n/a' END` |
| 4 | `prd_end_dt` is NULL for all rows — no expiry dates in source data; needed to identify the currently active product version | All 397 rows | `LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1` — end date is calculated as one day before the next version's start date; the current active version gets NULL |

**QC Checks:** S1 (NULLs), S3 (duplicate `(prd_key, prd_start_dt)` compound key), S4 (prd_line canonical values), S5 (end_dt before start_dt)

---

### crm_sales_details — 60,398 rows

| # | Issue | Scope | Fix Applied in Silver |
|---|-------|-------|-----------------------|
| 1 | `sls_order_dt`, `sls_ship_dt`, `sls_due_dt` stored as integers (e.g., `20121228`), not dates | All rows | `CASE WHEN ... = 0 OR LEN(...) != 8 THEN NULL ELSE CAST(CAST(... AS NVARCHAR) AS DATE) END` |
| 2 | Invalid date values stored as `0` (integer zero) | 17 rows | Set to NULL — caught by the length check `LEN() != 8` |
| 3 | `sls_sales` is NULL, zero, or negative for 5 rows; also 15 rows where `sls_sales != sls_quantity * ABS(sls_price)` (math mismatch) | 20 rows total | `CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(ISNULL(sls_price,1)) ELSE sls_sales END` |
| 4 | `sls_price` is NULL or negative for 5 rows | 5 rows | `CASE WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity,0) ELSE sls_price END` |
| 5 | `sls_ord_num` appears multiple times per order — this is expected behaviour, not a data error. One order can contain multiple product lines (e.g., `SO51179` = bike + tube + tire + stand) | 17,991 order numbers have 2+ lines | No deduplication. Grain of this table is one row per **order line**, not per order. See `gold.fact_sales_orders` for order-level aggregation. |

**QC Checks:** S1 (NULLs), S5 (date ordering), S6 (positive financials)

---

### crm_camp_info — 250 rows

| # | Issue | Scope | Fix Applied in Silver |
|---|-------|-------|-----------------------|
| 1 | `budget` is NULL for 15 rows | 15 rows | `CASE WHEN TRY_CAST(cmp_budget AS FLOAT) IS NULL THEN 0 ...` — set to 0 |
| 2 | `budget` is negative for 7 rows (likely data entry errors) | 7 rows | `ABS(TRY_CAST(cmp_budget AS FLOAT))` — take absolute value |
| 3 | `start_date` and `end_date` stored in mixed formats — both `MM/DD/YYYY` and `YYYY-MM-DD` appear in the same column | All 250 rows | `CASE WHEN ... LIKE '__/__/____' THEN TRY_CONVERT(DATE,...,101) ELSE TRY_CONVERT(DATE,...,23) END` |
| 4 | `channel` has 37+ raw variant spellings (e.g., `E-MAIL`, `SOCIALMEDIA`, `TV AD`, `IN-PERSON`) | All 250 rows | CASE map to 8 canonical values: `Email`, `Social Media`, `SMS`, `Direct Mail`, `Online`, `TV`, `Radio`, `Events`; unrecognised values → `Other` |
| 5 | ⚠️ 233 out of 250 campaigns had `start_date` years mismatched to the year in the campaign name (e.g., "Black Friday 2021" with dates in 2013). Corrected in the fixed source file | 233 rows | Corrected upstream in `camp_info_fixed.csv` — replace source file before Bronze load |
| 6 | Leading/trailing whitespace on `campaign_name` | Scattered | `TRIM()` applied |

**QC Checks:** S1 (NULLs), S3 (duplicate `cmp_id`), S4 (channel canonical values), S5 (date logic), S6 (negative budget)

---

### crm_supp_tkts — 15,000 rows

| # | Issue | Scope | Fix Applied in Silver |
|---|-------|-------|-----------------------|
| 1 | `status` has 20 raw variants (e.g., `open`, `OPEN`, `Opened`, `o`, `Resolve`, `Rsolvd`, `Clsd`, `In Progress`) | ~4,500 rows with non-canonical values | CASE map to 4 canonical values: `Open`, `Closed`, `Resolved`, `Pending`; unmapped → `Unknown` |
| 2 | `issue_category` has 16 raw variants across mixed cases | ~1,500 rows with non-canonical casing | `UPPER(TRIM())` and CASE map to 7 Title Case canonical values: `Billing`, `Technical`, `Shipping`, `Returns`, `Product`, `Account`, `General`; unmapped → `Unknown` |
| 3 | ⚠️ `cst_id` is NULL for 281 rows — tickets with no customer linkage | 281 rows | Kept as NULL — legitimate unknown customer cases |
| 4 | ⚠️ `cst_id = 999999` — a ghost/placeholder ID used in source system when customer is unknown | 232 rows | `CASE WHEN TRY_CAST(tkt_cst_id AS INT) = 999999 THEN NULL ...` — converted to NULL |
| 5 | ⚠️ `emp_id` is NULL for 426 rows — tickets with no assigned employee | 426 rows | Kept as NULL |
| 6 | ⚠️ `emp_id` references `EMP292`–`EMP300` — employee IDs that do not exist in `HR_HX19` (which only goes to `EMP291`) | 398 rows | `CASE WHEN TRY_CAST(SUBSTRING(tkt_emp_id,4,10) AS INT) > 300 THEN NULL ...` — filtered to NULL. Note: filter threshold is `> 300`; a tighter fix would be `> 291` |
| 7 | ⚠️ `prd_key` is NULL for 441 rows — tickets referencing no specific product | 441 rows | Kept as NULL |
| 8 | `prd_key` has formatting variants: underscore-delimited (`HL_U509`) instead of hyphen, and wrong `PRD-` prefix | Scattered | `REPLACE(..., '_', '-')` to fix delimiters; `SUBSTRING(tkt_prd_key, 5, 100)` to strip `PRD-` prefix |
| 9 | `open_date` and `resolution_date` in mixed date formats | All rows | `TRY_CONVERT(DATE, ..., 101/23)` with format detection |

**QC Checks:** S1 (NULLs), S3 (duplicate `tkt_id`), S4 (status and issue_category canonical values), S5 (resolution before open)

---

## ERP Sources

### CUST_AZ12 — 18,484 rows

| # | Issue | Scope | Fix Applied in Silver |
|---|-------|-------|-----------------------|
| 1 | `CID` has `NAS` prefix on some records (e.g., `NASAWC00011000` instead of `AWC00011000`) — prefix is a system artefact, not part of the ID | Subset of rows | `CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) ELSE cid END` |
| 2 | `BDATE` has 16 future dates — birth dates set in the future are clearly data entry errors | 16 rows | `CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END` |
| 3 | `GEN` has 5 raw variants: `Male`, `Female`, `M`, `F`, and blank | 1,485 rows with non-canonical or blank values | `CASE UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female' IN ('M','MALE') THEN 'Male' ELSE 'n/a' END` |

**QC Checks:** S2 (NULLs), S4 (gen canonical values), S5 (future birthdates)

---

### LOC_A101 — 18,484 rows

| # | Issue | Scope | Fix Applied in Silver |
|---|-------|-------|-----------------------|
| 1 | `CID` contains hyphens that don't appear in the matching CRM customer key format | All rows | `REPLACE(cid, '-', '')` |
| 2 | `CNTRY` has 10 raw variants for only 7 actual countries — abbreviations and alternate spellings mixed in | All 18,484 rows | CASE map: `'US'`, `'USA'` → `'United States'`; `'DE'` → `'Germany'`; canonical full names passed through; blank/NULL → `'n/a'` |
| 3 | 337 rows with NULL or blank `CNTRY` — customer location unknown | 337 rows | Mapped to `'n/a'` |

**Country normalisation applied:**

| Raw values | Silver canonical value |
|---|---|
| `US`, `USA`, `United States` | `United States` |
| `DE`, `Germany` | `Germany` |
| `Australia` | `Australia` |
| `Canada` | `Canada` |
| `United Kingdom` | `United Kingdom` |
| `France` | `France` |
| `''`, NULL | `n/a` |

**QC Checks:** S2 (NULLs on `cid` and `cntry`)

---

### PX_CAT_G1V2 — 37 rows

| # | Issue | Scope | Fix Applied in Silver |
|---|-------|-------|-----------------------|
| 1 | No data quality issues found | — | Direct load from Bronze — no transformations applied |

**QC Checks:** S2 (NULLs on `id` and `cat`)

---

### HR_HX19 — 300 raw rows → 291 silver rows

| # | Issue | Scope | Fix Applied in Silver |
|---|-------|-------|-----------------------|
| 1 | 9 duplicate `emp_id` values — same employee appears multiple times with different `hire_date` values | 9 duplicate IDs (9 extra rows) | `ROW_NUMBER() OVER (PARTITION BY emp_id ORDER BY emp_hire_date DESC)` — keep most recent hire record |
| 2 | `first_name` and `last_name` contain NULLs or empty strings | 8 NULL first names, 4 NULL last names | `TRIM(COALESCE(NULLIF(TRIM(name), ''), 'N/A'))` — substituted with `'N/A'` |
| 3 | `role` has 40+ raw variants for 10 actual job titles (e.g., `Analst`, `ANLST`, `Dir.`, `Engr`, `Sup Agent`, `SUPP_AGT`, `TL`, `Coord`, `COORD`) | ~90 rows with non-canonical values | CASE map to 10 canonical titles: `Analyst`, `Engineer`, `Support Agent`, `Team Lead`, `Director`, `Coordinator`, `Specialist`, `Consultant`, `Sales Representative`, `Manager` |
| 4 | `branch_id` is NULL or blank for 14 rows | 14 rows | `COALESCE(NULLIF(TRIM(emp_branch_id), ''), 'Unknown')` |
| 5 | `hire_date` stored in mixed formats — both `MM/DD/YYYY` and `YYYY-MM-DD` | All 300 rows | `CASE WHEN ... LIKE '__/__/____' THEN TRY_CONVERT(DATE,...,101) ELSE TRY_CONVERT(DATE,...,23) END` |
| 6 | `emp_full_name` is not in the source — it is a derived column | N/A (derived) | `CONCAT(emp_first_name, ' ', emp_last_name)` added in Silver |

**QC Checks:** S2 (NULLs), S3 (duplicate `emp_id`), S4 (branch_id not NULL), S5 (future hire_date), S6 (emp_full_name derivation check)

---

### VND_Z90 — 120 raw rows → 111 silver rows

| # | Issue | Scope | Fix Applied in Silver |
|---|-------|-------|-----------------------|
| 1 | 9 duplicate `vendor_id` values — same vendor ID appears multiple times | 9 duplicate IDs (9 extra rows) | `ROW_NUMBER() OVER (PARTITION BY vnd_id ORDER BY (SELECT NULL))` — keep first occurrence |
| 2 | `vendor_name` is NULL or blank for 2 rows | 2 rows | `COALESCE(NULLIF(TRIM(vnd_name), ''), 'Unknown Vendor')` |
| 3 | `country` has 28 raw variants for 10 actual countries — abbreviations (`FRA`, `ITA`, `ESP`, `DE`, `UK`, `CAN`, `JPN`, `IND`, `AU`), alternate spellings (`england`, `france`, `germany`, `italy`, `spain`), and ISO codes mixed together | All 120 rows | CASE map to 10 canonical country names: `United States`, `United Kingdom`, `Germany`, `France`, `Canada`, `Australia`, `Japan`, `India`, `Spain`, `Italy` |

**Vendor country normalisation applied (28 → 10 variants):**

| Raw variants | Silver canonical value |
|---|---|
| `USA`, `US`, `United States`, `U.S.A`, `U.S.`, `us` | `United States` |
| `UK`, `United Kingdom`, `england`, `GB`, `GBR`, `U.K.` | `United Kingdom` |
| `Germany`, `DE`, `GER`, `germany`, `DEU` | `Germany` |
| `France`, `FRA`, `france`, `FR` | `France` |
| `Canada`, `CAN`, `CA` | `Canada` |
| `Australia`, `AU`, `AUS` | `Australia` |
| `Japan`, `JPN`, `JP` | `Japan` |
| `India`, `IND`, `IN`, `india` | `India` |
| `Spain`, `ESP`, `SP`, `spain` | `Spain` |
| `Italy`, `ITA`, `IT`, `italy` | `Italy` |

**QC Checks:** S2 (NULLs), S3 (duplicate `vnd_id`), S4 (vnd_name not NULL)

---

### INV_Q4V1 — 1,191 raw rows → 267 silver rows (after dedup + orphan filter)

| # | Issue | Scope | Fix Applied in Silver |
|---|-------|-------|-----------------------|
| 1 | `inv_id` has 24 duplicates — same inventory snapshot ID appears multiple times | 24 duplicate IDs | `ROW_NUMBER() OVER (PARTITION BY inv_id ORDER BY inv_snap_date DESC)` — keep most recent snapshot |
| 2 | `prd_id` references 47 product IDs > 397 — these IDs do not exist in `crm_prd_info` and are orphan references | 47 rows | `WHERE TRY_CAST(inv_prd_id AS INT) <= 397 AND NOT NULL` — excluded entirely |
| 3 | `warehouse_loc` has 30+ variants of the same location codes due to inconsistent casing and trailing spaces (e.g., `Warehouse A`, `WAREHOUSE A`, `warehouse a`) | All 1,191 rows | `UPPER(TRIM(inv_wh_loc))` — standardise to uppercase trimmed |
| 4 | `stock_on_hand` is negative for 48 rows — negative stock is physically impossible | 48 rows | `CASE WHEN TRY_CAST(inv_stock_on_hand AS INT) < 0 THEN 0 ELSE ... END` — floored at 0 |
| 5 | `snapshot_date` stored in mixed date formats | All rows | `CASE WHEN ... LIKE '__/__/____' THEN TRY_CONVERT(DATE,...,101) ELSE TRY_CONVERT(DATE,...,23) END` |
| 6 | `below_reorder` flag is not in source — derived in Silver | N/A (derived) | `CASE WHEN inv_stock_on_hand < inv_reorder_level THEN 1 ELSE 0 END` |

**QC Checks:** S2 (NULLs), S3 (duplicate `inv_id`), S4 (below_reorder flag consistency), S5 (future snapshot_date), S6 (negative stock, reorder level)

---

### PO_ORD44 — 5,000 raw rows → 5,000 silver rows

| # | Issue | Scope | Fix Applied in Silver |
|---|-------|-------|-----------------------|
| 1 | ⚠️ `vendor_id` is NULL for 210 rows — purchase orders with no supplier recorded | 210 rows | Kept as NULL; `flag_missing_vendor = 1` applied |
| 2 | ⚠️ `vendor_id` references 219 IDs not present in `VND_Z90` — orphan vendor references | 219 rows | Kept as-is; `flag_missing_vendor = 1` applied via `WHERE po_vnd_id NOT IN (SELECT vnd_id FROM silver.erp_vnd_z90)` |
| 3 | `quantity_ordered` is a float for 369 rows (e.g., `1.0`, `2.5`) — unit quantity should be a whole number | 369 rows | `CAST(ROUND(TRY_CAST(po_quantity_ordered AS FLOAT), 0) AS INT)` — round to nearest integer |
| 4 | `unit_cost` has 4 dirty patterns: `'$XX.XX'` (dollar sign prefix), `'N/A'` string, blank/NULL, and plain numeric | Mixed across 5,000 rows | Strip `$` with `SUBSTRING`; map `N/A`/blank/NULL → NULL; `TRY_CAST` remainder to FLOAT |
| 5 | `order_date` stored in mixed date formats | All rows | Same dual-format `TRY_CONVERT` pattern as other date columns |
| 6 | `total_cost` not in source — derived column | N/A (derived) | `CASE WHEN quantity IS NOT NULL AND unit_cost IS NOT NULL THEN quantity * unit_cost ELSE NULL END` |

**Note on flag_missing_vendor:** 210 + 219 = 429 total PO rows with no resolvable vendor. These are flagged but retained — they represent 8.6% of POs. The Gold `fact_purchase_orders` view surfaces this flag so analysts know which PO spend cannot be attributed to a known vendor.

**QC Checks:** S2 (NULLs), S3 (duplicate `po_number`), S4 (flag_missing_vendor consistency), S5 (future order_date), S6 (positive quantity, total_cost calculation)

---

## Cross-Layer Known Gaps (Gold)

These issues produce expected non-zero values in Gold QC and are by design, not bugs.

| Fact Table | Column | Expected NULLs | Root Cause |
|---|---|---|---|
| `fact_support_tickets` | `customer_id` | ~513 | 281 raw NULLs + 232 ghost IDs (999999) correctly nulled in Silver |
| `fact_support_tickets` | `employee_id` | ~824 | 426 raw NULLs + 398 tickets referencing EMP292–EMP300 (not in HR) |
| `fact_support_tickets` | `product_key` | ~441 | 441 raw NULL prd_keys in source tickets |
| `fact_purchase_orders` | `vendor_id` | ~429 | 210 raw NULL + 219 orphan vendor IDs; all flagged with `flag_missing_vendor = 1` |

**Expected Gold join match rates:**

| Fact | Dimension | Expected Match % |
|---|---|---|
| `fact_sales` | Products | 100% |
| `fact_sales` | Customers | 100% |
| `fact_support_tickets` | Customers | ~96.6% |
| `fact_support_tickets` | Employees | ~94.5% |
| `fact_support_tickets` | Products | ~97.1% |
| `fact_purchase_orders` | Vendors | ~91.4% |
| `fact_purchase_orders` | Products | 100% |
| `fact_inventory` | Products | 100% |
