# Data Catalog for Gold Layer

## Overview

The Gold Layer is the business-level data representation, structured to support analytical and reporting use cases. It consists of **dimension tables** (descriptive data) and **fact tables** (quantitative metrics) designed for end-user consumption.

---

### 1. **gold.dim_customers**

* **Purpose:** Stores enriched customer details by merging CRM information with ERP demographics and location data.
* **Columns:**

| Column Name | Data Type | Description |
| --- | --- | --- |
| customer_key | INT | Surrogate key uniquely identifying each customer record, generated via ROW_NUMBER. |
| customer_id | INT | The unique numerical identifier for the customer from CRM source data. |
| customer_number | NVARCHAR(50) | The alphanumeric customer key used to link CRM and ERP records. |
| first_name | NVARCHAR(50) | The customer's first name. |
| last_name | NVARCHAR(50) | The customer's last name. |
| country | NVARCHAR(50) | The country of the customer, sourced from ERP location data. |
| marital_status | NVARCHAR(50) | The customer's marital status (e.g., 'Single', 'Married'). |
| gender | NVARCHAR(50) | Customer gender, prioritized from CRM with ERP fallback. |
| birth_date | DATE | The customer's date of birth from ERP demographics. |
| create_date | DATE | The date the customer was first created in the CRM system. |

---

### 2. **gold.dim_products**

* **Purpose:** Provides a unified view of all active products, integrating CRM product details with ERP category hierarchies.
* **Columns:**

| Column Name | Data Type | Description |
| --- | --- | --- |
| product_key | INT | Surrogate key for the product dimension, ordered by start date and ID. |
| product_id | INT | Unique numerical identifier for the product. |
| product_number | NVARCHAR(50) | Alphanumeric product key used for referencing and joining. |
| product_name | NVARCHAR(50) | The descriptive name of the product. |
| category_id | NVARCHAR(50) | Identifier for the product's high-level category. |
| category | NVARCHAR(50) | The broad classification of the product (e.g., Bikes). |
| subcategory | NVARCHAR(50) | Detailed classification within the category. |
| maintenance | NVARCHAR(50) | Indicates maintenance requirements or status from ERP. |
| cost | INT | The base cost of the product. |
| product_line | NVARCHAR(50) | The specific line or series the product belongs to. |
| start_date | DATE | The date the product became active in the catalog. |

---

### 3. **gold.dim_employees**

* **Purpose:** Contains HR data used to track agents assigned to support tickets and branch performance.
* **Columns:**

| Column Name | Data Type | Description |
| --- | --- | --- |
| employee_id | NVARCHAR(20) | Stable natural key for the employee. |
| employee_first_name | NVARCHAR(100) | First name of the employee. |
| employee_last_name | NVARCHAR(100) | Last name of the employee. |
| employee_full_name | NVARCHAR(200) | Combined full name for reporting. |
| job_title | NVARCHAR(100) | The professional role of the employee (e.g., Manager, MGR). |
| branch_id | NVARCHAR(20) | ID of the branch where the employee is based. |
| hire_date | DATE | The official date the employee joined the company. |
| years_of_service | INT | Calculated years of tenure based on hire date. |
| seniority_band | VARCHAR | Categorization based on years of service (Junior, Mid-Level, Senior, Veteran). |

---

### 4. **gold.dim_vendors**

* **Purpose:** Master list of vendors for procurement analysis, including regional groupings.
* **Columns:**

| Column Name | Data Type | Description |
| --- | --- | --- |
| vendor_id | NVARCHAR(20) | Unique identifier for the vendor. |
| vendor_name | NVARCHAR(200) | The official name of the vendor. |
| vendor_country | NVARCHAR(100) | The country where the vendor is based. |
| vendor_region | VARCHAR | Geographic region grouping (e.g., North America, Europe, Asia-Pacific). |

---

### 5. **gold.dim_campaigns**

* **Purpose:** Stores metadata for marketing campaigns, ensuring valid date ranges for analysis.
* **Columns:**

| Column Name | Data Type | Description |
| --- | --- | --- |
| campaign_id | NVARCHAR(20) | Unique identifier for the campaign. |
| campaign_name | NVARCHAR(200) | The name of the marketing initiative. |
| channel | NVARCHAR(50) | The marketing channel used (e.g., Email, Web). |
| budget | FLOAT | The allocated budget for the campaign. |
| start_date | DATE | The launch date of the campaign. |
| end_date | DATE | The conclusion date of the campaign. |
| campaign_duration_days | INT | Calculated duration of the campaign in days. |
| campaign_year | INT | The year the campaign was launched. |

---

### 6. **gold.fact_sales**

* **Purpose:** Centralized sales transactions linking customers and products for revenue analysis.
* **Columns:**

| Column Name | Data Type | Description |
| --- | --- | --- |
| order_number | NVARCHAR(50) | Unique identifier for the sales order. |
| product_key | INT | Foreign key to `gold.dim_products`. |
| customer_key | INT | Foreign key to `gold.dim_customers`. |
| order_date | DATE | The date the order was placed. |
| shipping_date | DATE | The date the products were shipped. |
| due_date | DATE | The date the payment for the order is due. |
| sales_amount | INT | Total sales value for the line item. |
| quantity | INT | Number of units sold. |
| price | INT | The unit price of the product at the time of sale. |

---

### 7. **gold.fact_support_tickets**

* **Purpose:** Tracks customer support tickets, their resolution status, and assigned agents.
* **Columns:**

| Column Name | Data Type | Description |
| --- | --- | --- |
| id | NVARCHAR(20) | Unique identifier for the support ticket. |
| open_date | DATE | The date the ticket was opened. |
| resolution_date | DATE | The date the ticket was resolved. |
| days_to_resolve | INT | Calculated days taken to resolve (or days since open if unresolved). |
| is_overdue | INT | Flag (1) if a ticket is open and older than 30 days. |
| issue_category | NVARCHAR(50) | The type of issue reported (e.g., Billing). |
| status | NVARCHAR(20) | Current status of the ticket (e.g., Open, Closed). |
| customer_id | INT | Link to the reporting customer. |
| employee_id | NVARCHAR(20) | Link to the assigned agent (`gold.dim_employees`). |
| product_key | NVARCHAR(50) | Link to the product related to the ticket. |

---

### 8. **gold.fact_inventory**

* **Purpose:** Snapshot of stock levels to manage warehouse replenishment and value.
* **Columns:**

| Column Name | Data Type | Description |
| --- | --- | --- |
| inventory_id | NVARCHAR(20) | Unique identifier for the inventory record. |
| snapshot_date | DATE | The date the stock level was recorded. |
| warehouse_location | NVARCHAR(10) | The warehouse code where the stock is stored. |
| product_id | INT | Foreign key to `gold.dim_products`. |
| stock_on_hand | INT | Number of units currently in stock. |
| reorder_level | INT | The stock threshold that triggers a reorder. |
| below_reorder | BIT | Flag indicating if stock is currently below reorder level. |
| stock_vs_reorder | INT | Difference between actual stock and the reorder threshold. |
| estimated_stock_value | FLOAT | Calculated value based on stock on hand and product cost. |

---

### 9. **gold.fact_purchase_orders**

* **Purpose:** Tracks procurement orders from vendors, including volume and cost metrics.
* **Columns:**

| Column Name | Data Type | Description |
| --- | --- | --- |
| po_number | NVARCHAR(20) | Unique identifier for the purchase order. |
| order_date | DATE | The date the PO was placed. |
| vendor_id | NVARCHAR(20) | Link to the vendor in `gold.dim_vendors`. |
| product_id | INT | Link to the product in `gold.dim_products`. |
| quantity_ordered | INT | Total units requested from the vendor. |
| unit_cost | FLOAT | The cost per unit specified in the PO. |
| total_cost | FLOAT | Total financial value of the order (quantity x unit cost). |
| flag_missing_vendor | BIT | Quality flag (1) if the vendor ID is missing or invalid. |

---

Would you like me to create a **README.md** file that summarizes the end-to-end ELT process for your repository as well?
