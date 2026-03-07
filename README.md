# data-warehouse
A scalable data warehousing solution designed to transform raw, fragmented data into actionable business intelligence.

> **A production-grade, end-to-end Data Warehouse built on SQL Server using the Medallion Architecture (Bronze → Silver → Gold).**  
> Integrates 12 source tables across CRM and ERP systems into a clean star schema ready for BI and analytics.

---

## 📐 Architecture

![Data Architecture](docs/architecture.png)

---

## 🗂️ Repository Structure

```
data-warehouse-project/
│
├── 📁 datasets/
│   ├── source_crm/
│   │   ├── cust_info.csv          # 18,484 customers
│   │   ├── prd_info.csv           # 397 products (versioned)
│   │   ├── sales_details.csv      # 60,398 sales transactions
│   │   ├── camp_info.csv          # 250 marketing campaigns   
│   │   └── supp_tkts.csv          # 15,000 support tickets    
│   └── source_erp/
│       ├── CUST_AZ12.csv          # 18,484 customer demographics
│       ├── LOC_A101.csv           # 18,484 customer locations
│       ├── PX_CAT_G1V2.csv        # 37 product categories
│       ├── HR_HX19.csv            # 300 employees             
│       ├── VND_Z90.csv            # 120 vendors               
│       ├── INV_Q4V1.csv           # 1,191 inventory snapshots 
│       └── PO_ORD44.csv           # 5,000 purchase orders     
│
├── 📁 scripts/
│   ├── DDL_Create_Tables_Bronze_Layer.sql   # Create all 12 bronze tables
│   ├── DML_Bulk_Insert_Bronze_Layer.sql     # Load all 12 CSVs into bronze
│   ├── DDL_Create_Tables_Silver_Layer.sql   # Create all 12 silver tables
│   ├── DML_Insert_Silver_Layer.sql          # Transform bronze → silver
│   └── DDL_Create_Views_Gold.sql            # Create 5 dims + 4 facts
│
├── 📁 docs/
│   ├── DWH_Integration_Guide.docx           # Full architecture guide
│   ├── architecture.png                     # Data Architecture  
│   ├── data_catalog.md                      # Column-level data dictionary
│   └── data_quality_issues.md               # All dirty data patterns documented
│
├── 📁 tests/
│   └── quality_checks_silver.sql            # Data quality validation queries
│   └── quality_checks_gold.sql              # Data quality validation queries
└── README.md
```

---

## 🔢 Project Metrics

| Metric | Value |
|---|---|
| Source Tables | **12** |
| Total Source Records | **~138,100** |
| Gold Dimensions | **5** |
| Gold Facts | **4** |
| Analytics Domains | **Sales · HR · Inventory · Procurement · Marketing · Support** |

---

## 🧹 Data Quality Issues Handled

This project intentionally contains dirty data in the source CSVs to practice real-world ETL transformations. Every issue below is resolved in the Silver layer.

### CRM — `supp_tkts`
| Column | Issue | Silver Fix |
|---|---|---|
| `cst_id` | 307 NULLs + 269 ghost IDs (999999) | NULL → NULL; 999999 → NULL |
| `prd_key` | 407 underscore-delimited, 311 with wrong `PRD-` prefix | Replace `_` with `-`; strip `PRD-` prefix |
| `emp_id` | 451 NULLs + invalid IDs (EMP > 300) | NULL → NULL; invalid → NULL |
| `issue_category` | 21 variants for 7 categories | Standardize to Title Case |
| `status` | 20 variants for 4 statuses | Open / Closed / Resolved / Pending |
| `open_date` | Mixed `YYYY-MM-DD` and `MM/DD/YYYY` | `TRY_CONVERT` with dual format |
| `resolution_date` | 7,535 NULLs (valid for open tickets) | NULL kept; closed tickets parsed |

### CRM — `camp_info`
| Column | Issue | Silver Fix |
|---|---|---|
| `channel` | 37 variants for 8 channels | Mapped to 8 canonical values |
| `budget` | 17 NULLs + 8 negatives | NULL → 0; negative → `ABS()` |
| `start_date` / `end_date` | Mixed formats + 38 rows end < start | Dual parse; flag invalid range |

### ERP — `HR_HX19`
| Column | Issue | Silver Fix |
|---|---|---|
| `emp_id` | 9 duplicate emp_ids | `ROW_NUMBER()`, keep latest `hire_date` |
| `first_name` / `last_name` | 8/4 NULLs + whitespace | `TRIM()` + COALESCE to 'N/A' |
| `role` | 40 variants for 10 roles | Mapped to 10 canonical job titles |
| `branch_id` | 15 NULLs | → 'Unknown' |
| `hire_date` | Mixed formats | Dual format parse → DATE |

### ERP — `VND_Z90`
| Column | Issue | Silver Fix |
|---|---|---|
| `vendor_id` | 9 duplicates | `ROW_NUMBER()`, keep first |
| `vendor_name` | 5 NULLs + whitespace | → 'Unknown Vendor' + `TRIM()` |
| `country` | 31 variants for 10 countries | Full name standardization |

### ERP — `INV_Q4V1`
| Column | Issue | Silver Fix |
|---|---|---|
| `inv_id` | 24 duplicates | `ROW_NUMBER()`, keep latest snapshot |
| `prd_id` | 47 orphan IDs not in product master | Excluded via `WHERE prd_id <= 397` |
| `warehouse_loc` | 30 casing variants + trailing spaces | `UPPER(TRIM())` |
| `stock_on_hand` | 48 negative values | `CASE WHEN < 0 THEN 0` |
| `snapshot_date` | Mixed formats | Dual format parse → DATE |

### ERP — `PO_ORD44`
| Column | Issue | Silver Fix |
|---|---|---|
| `po_number` | 107 duplicates | `ROW_NUMBER()`, keep latest order_date |
| `vendor_id` | 159 NULLs + invalid IDs | NULL → NULL; invalid → `flag_missing_vendor = 1` |
| `quantity_ordered` | 374 fractional values (Float) | `ROUND()` → `CAST INT` |
| `unit_cost` | `$XX.XX`, `N/A`, NULL, integer strings | Strip `$`, `N/A` → NULL, CAST FLOAT |
| `order_date` | Mixed formats | Dual format parse → DATE |

---

## 🌟 Gold Layer — Star Schema

### Dimensions
| View | Source | Key Attributes |
|---|---|---|
| `dim_customers` | crm_cust_info + erp_cust_az12 + erp_loc_a101 | customer_id, full_name, country, gender, birth_date |
| `dim_products` | crm_prd_info + erp_px_cat_g1v2 | product_id, product_number, name, category, subcategory, cost |
| `dim_employees` | erp_hr_hx19 | employee_id, full_name, job_title, branch_id, seniority_band |
| `dim_vendors` | erp_vnd_z90 | vendor_id, vendor_name, country, region |
| `dim_campaigns` | crm_camp_info | campaign_id, name, channel, budget, duration_days |

### Facts
| View | Grain | Key Measures |
|---|---|---|
| `fact_sales` | 1 row per sales order line | sales_amount, quantity, price |
| `fact_support_tickets` | 1 row per ticket | days_to_resolve, is_overdue |
| `fact_inventory` | 1 row per snapshot × product × warehouse | stock_on_hand, below_reorder, estimated_stock_value |
| `fact_purchase_orders` | 1 row per PO line | quantity_ordered, unit_cost, total_cost |

---

## 🚀 How to Run

> **Prerequisites:** SQL Server 2019+ | SSMS or Azure Data Studio

### 1. Set up schemas
```sql
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
```

### 2. Create tables
```sql
-- Run in order:
-- 1. scripts/DDL_Create_Tables_Bronze_Layer.sql
-- 2. scripts/DDL_Create_Tables_Silver_Layer.sql
```

### 3. Load Bronze (update file paths to your machine)
```sql
EXEC bronze.load_bronze;
```

### 4. Transform to Silver
```sql
EXEC silver.load_silver;
```

### 5. Create Gold views
```sql
-- scripts/DDL_Create_Views_Gold.sql
```

### 6. Validate
```sql
SELECT * FROM gold.dim_customers;        -- should have 0 NULLs on key columns
SELECT * FROM gold.dim_products;         -- active products only
SELECT * FROM gold.fact_sales;           -- ~60,398 rows
SELECT * FROM gold.fact_support_tickets; -- 15,000 rows
SELECT * FROM gold.fact_inventory;       -- ~1,150 rows
SELECT * FROM gold.fact_purchase_orders; -- ~5,000 rows
```

---

## 🔍 Sample Analytics Queries

```sql
-- 1. Top 10 customers by total sales
SELECT TOP 10
    c.customer_id,
    c.first_name + ' ' + c.last_name AS customer_name,
    SUM(f.sales_amount) AS total_sales
FROM gold.fact_sales f
JOIN gold.dim_customers c ON f.customer_key = c.customer_key
GROUP BY c.customer_id, c.first_name, c.last_name
ORDER BY total_sales DESC;

-- 2. Agent performance: avg days to resolve by agent
SELECT
    e.employee_full_name,
    e.job_title,
    COUNT(*) AS total_tickets,
    AVG(t.days_to_resolve) AS avg_days_to_resolve,
    SUM(t.is_overdue) AS overdue_count
FROM gold.fact_support_tickets t
JOIN gold.dim_employees e ON t.employee_id = e.employee_id
GROUP BY e.employee_full_name, e.job_title
ORDER BY avg_days_to_resolve;

-- 3. Inventory alerts: products below reorder level
SELECT
    warehouse_location,
    product_name,
    product_category,
    stock_on_hand,
    reorder_level,
    stock_vs_reorder
FROM gold.fact_inventory
WHERE below_reorder = 1
ORDER BY stock_vs_reorder;

-- 4. Vendor spend analysis
SELECT
    v.vendor_name,
    v.vendor_country,
    v.vendor_region,
    COUNT(*) AS total_orders,
    SUM(p.total_cost) AS total_spend,
    AVG(p.unit_cost) AS avg_unit_cost
FROM gold.fact_purchase_orders p
JOIN gold.dim_vendors v ON p.vendor_id = v.vendor_id
GROUP BY v.vendor_name, v.vendor_country, v.vendor_region
ORDER BY total_spend DESC;

-- 5. Campaign channel budget distribution
SELECT
    channel,
    COUNT(*) AS num_campaigns,
    SUM(budget) AS total_budget,
    AVG(campaign_duration_days) AS avg_duration_days
FROM gold.dim_campaigns
WHERE flag_invalid_date_range = 0
GROUP BY channel
ORDER BY total_budget DESC;
```

---

## 🛠️ Tech Stack

| Tool | Purpose |
|---|---|
| **SQL Server 2019** | Database engine |
| **T-SQL** | ETL scripting, views, stored procedures |
| **SSMS / Azure Data Studio** | Development & testing |
| **Python** | Source data generation (dirty CSVs) |
| **Git** | Version control |

---

## 📊 Skills Demonstrated

- **Data Architecture** — Medallion Architecture design (Bronze/Silver/Gold)
- **ETL Engineering** — Stored procedures, BULK INSERT, incremental patterns
- **Data Quality** — Identifying and resolving 15+ categories of dirty data
- **Data Modelling** — Star schema design (5 dimensions, 4 fact tables)
- **SQL** — Complex CTEs, window functions, CASE logic, multi-source joins
- **Documentation** — Data catalogues, architecture diagrams, lineage documentation

---

## 👤 Author

**Bashar Al-Jariri**  
Data Engineer | Implementation Specialist  
[LinkedIn]([www.linkedin.com/in/bashar-aljariri-27a48029b]) 

---

## 📄 License

This project is for educational and portfolio purposes.
