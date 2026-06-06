# Pipeline Decisions & Data Analysis

Step-by-step record of every schema decision, data quality finding,
and the exact query used to reach each conclusion.

---

## Step 1 — Inspecting the raw data

After loading the 3 CSV files into the `raw_` tables (all TEXT columns),
we ran inspection queries on each table to understand the data before defining types.

### 1.1 raw_product_master

**Query used to inspect:**

```sql
SELECT * FROM raw_product_master LIMIT 10;
```

**Findings:**


| Column                 | Sample values               | Decision                                       |
| ---------------------- | --------------------------- | ---------------------------------------------- |
| ProductID              | "680", "706", "707"         | All integers → `INTEGER` (PK)                  |
| ProductDesc            | "HL Road Frame - Black, 58" | Free text → `VARCHAR(255)`                     |
| ProductNumber          | "FR-R92B-58"                | Alphanumeric code → `VARCHAR(50)`              |
| MakeFlag               | "True", "False"             | Boolean string → `BOOLEAN`                     |
| Color                  | "Black", "Red", ""          | Empty = no color → `VARCHAR(50)` nullable      |
| SafetyStockLevel       | "500", "4"                  | Integer with "Level" in name → `INTEGER`       |
| ReorderPoint           | "375", "3"                  | Integer with "Point" in name → `INTEGER`       |
| StandardCost           | "1059.31", "13.0863"        | Decimal with "Cost" in name → `DECIMAL(18,6)`  |
| ListPrice              | "1431.5", "34.99"           | Decimal with "Price" in name → `DECIMAL(18,6)` |
| Size                   | "58", "M", "L", ""          | Mixed numeric/alpha → `VARCHAR(20)`            |
| SizeUnitMeasureCode    | "CM ", ""                   | Short code, needs TRIM → `VARCHAR(10)`         |
| Weight                 | "2.24", ""                  | Decimal or empty → `FLOAT` nullable            |
| WeightUnitMeasureCode  | "LB ", ""                   | Short code → `VARCHAR(10)`                     |
| ProductCategoryName    | "", "Bikes"                 | Often empty → `VARCHAR(100)` nullable          |
| ProductSubCategoryName | "Road Frames", "Helmets"    | Always present → `VARCHAR(100)`                |


**Type rules applied:**

- Column contains `ID` + numeric values → `INTEGER`
- Column contains `Flag` + True/False values → `BOOLEAN`
- Column contains `Cost` or `Price` → `DECIMAL(18,6)`
- Column contains `Level` or `Point` → `INTEGER`
- `Weight` → `FLOAT` (can have decimals, nullable)
- Empty strings → `NULL` in the store layer

---

### 1.2 Duplicate ProductID check

**Query used:**

```sql
SELECT "ProductID", COUNT(*) AS occurrences
FROM raw_product_master
GROUP BY "ProductID"
HAVING COUNT(*) > 1;
```

**Findings:** 8 ProductIDs appeared twice: 713, 714, 715, 716, 881, 882, 883, 884.

**Inspecting the duplicate pairs:**

```sql
SELECT * FROM raw_product_master
WHERE "ProductID" IN ('713','714','715','716','881','882','883','884')
ORDER BY "ProductID";
```

**What we found per duplicate pair (example for 713):**


| ProductID | ProductCategoryName | ProductSubCategoryName |
| --------- | ------------------- | ---------------------- |
| 713       | *(empty)*           | Jerseys                |
| 713       | Clothing            | Shirt                  |


One row had `ProductCategoryName` and `ProductSubCategoryName` filled.
The other had `ProductCategoryName` empty.

**Decision:** Keep the row with the **fewest NULL values** (most complete data).
This logic was implemented as a generic function `deduplicate_by_completeness()` so it can be reused on any table.

---

### 1.3 Type validation for products

**Query to check all casts are safe:**

```sql
-- Check ProductID can be cast to INTEGER (expect 0 rows)
SELECT "ProductID" FROM raw_product_master
WHERE "ProductID" !~ '^\d+$';

-- Check MakeFlag only has True/False
SELECT DISTINCT "MakeFlag" FROM raw_product_master;

-- Check StandardCost and ListPrice are numeric
SELECT "StandardCost", "ListPrice" FROM raw_product_master
WHERE "StandardCost" !~ '^\d+(\.\d+)?$'
   OR "ListPrice"    !~ '^\d+(\.\d+)?$';

-- Check Weight is numeric or empty
SELECT DISTINCT "Weight" FROM raw_product_master
WHERE "Weight" <> '' AND "Weight" !~ '^\d+(\.\d+)?$';

-- Count of nullable columns
SELECT
    COUNT(*) AS total,
    COUNT(NULLIF("Color", ''))           AS color_filled,
    COUNT(NULLIF("ProductCategoryName", '')) AS category_filled
FROM raw_product_master;
```

**Results:** All checks passed. All casts confirmed safe.

---

### 1.4 raw_sales_order_header

**Query used:**

```sql
SELECT * FROM raw_sales_order_header LIMIT 10;
```

**Findings:**


| Column          | Sample values              | Decision                              |
| --------------- | -------------------------- | ------------------------------------- |
| SalesOrderID    | "43828", "43659"           | Integer → `INTEGER` (PK)              |
| OrderDate       | "2021-06", "2021-05-31"    | **TWO formats** → see below           |
| ShipDate        | "2021-07-05", "2021-06-07" | Full date → `DATE`                    |
| OnlineOrderFlag | "True", "False"            | Boolean → `BOOLEAN`                   |
| AccountNumber   | "10-4030-027605"           | Code with hyphens → `VARCHAR(20)`     |
| CustomerID      | "27605", "29825"           | Integer → `INTEGER` (FK)              |
| SalesPersonID   | "279", ""                  | Integer or empty → `INTEGER` nullable |
| Freight         | "89.4568", "616.094"       | Decimal → `DECIMAL(18,4)`             |



---

### 1.5 OrderDate format anomaly

**Query to confirm the two formats:**

```sql
SELECT LENGTH("OrderDate") AS date_length, COUNT(*) AS occurrences
FROM raw_sales_order_header
GROUP BY 1
ORDER BY 1;
```

**Result (verified against the actual file):**


| date_length     | format  | occurrences |
| --------------- | ------- | ----------- |
| 7 (YYYY-MM)     | partial | 5           |
| 10 (YYYY-MM-DD) | full    | 31,460      |


Only **5 rows** are missing the day — the rest are full dates. We still need a full date for
`LeadTimeInBusinessDays`, so those 5 are derived (length-based detection, not flag-based).

**Query to confirm the 7-day pattern:**

```sql
SELECT
    "SalesOrderID",
    "OrderDate",
    "ShipDate",
    "OnlineOrderFlag"
FROM raw_sales_order_header
WHERE "OnlineOrderFlag" = 'False'
LIMIT 10;
```

**Observed pattern from in-store orders:**

- 2021-05-31 → 2021-06-07 (7 days)
- 2021-06-05 → 2021-06-12 (7 days)
- 2021-06-04 → 2021-06-11 (7 days)

**Decision:** For the 5 partial rows (`YYYY-MM`), derive `order_date = ship_date - 7 days`.  
This is documented as an assumption, not a fact — but it is consistent with all inspected  
records and affects only really small portion of the data, so the analytical impact is negligible.

---

### 1.6 SalesPersonID nullable check

**Query:**

```sql
SELECT DISTINCT "SalesPersonID" FROM raw_sales_order_header ;
```

**Result:** Online orders have empty string ("") for SalesPersonID. This is expected — online orders have no assigned salesperson. Empty string → `NULL` in store layer.

---

### 1.7 Value range check for SalesOrderHeader IDs

**Query:**

```sql
SELECT
    MIN("SalesOrderID"::INTEGER)  AS min_sales_order_id,
    MAX("SalesOrderID"::INTEGER)  AS max_sales_order_id,
    MIN("CustomerID"::INTEGER)    AS min_customer_id,
    MAX("CustomerID"::INTEGER)    AS max_customer_id,
    MIN(NULLIF("SalesPersonID", '')::INTEGER) AS min_sales_person_id,
    MAX(NULLIF("SalesPersonID", '')::INTEGER) AS max_sales_person_id
FROM raw_sales_order_header;
```

**Result:**

- SalesOrderID: 43,659 → 75,123
- CustomerID: 11,000 → 30,118
- SalesPersonID: 274 → 290



---

### 1.8 raw_sales_order_detail

**Query used:**

```sql
SELECT * FROM raw_sales_order_detail;
```

**Findings:**


| Column             | Sample values  | Decision                         |
| ------------------ | -------------- | -------------------------------- |
| SalesOrderID       | "43659"        | INTEGER (FK to header)           |
| SalesOrderDetailID | "1", "2"       | INTEGER (PK)                     |
| OrderQty           | "1", "3", "-1" | INTEGER (small qty; -1 = return) |
| ProductID          | "776", "714"   | INTEGER (FK to products)         |
| UnitPrice          | "2024.9940"    | DECIMAL(18,6) — "Price" rule     |
| UnitPriceDiscount  | ".0000"        | DECIMAL(18,6) — "Price" rule     |


---

### 1.9 Negative OrderQty check

**Query:**

```sql
SELECT
    "SalesOrderDetailID",
    "SalesOrderID",
    "OrderQty",
    "ProductID",
    "UnitPrice"
FROM raw_sales_order_detail
WHERE "OrderQty"::INTEGER < 0;
```

**Result:**


| SalesOrderDetailID | SalesOrderID | OrderQty | ProductID | UnitPrice |
| ------------------ | ------------ | -------- | --------- | --------- |
| 112                | 43670        | -1       | 710       | 5.70      |
| 339                | 43694        | -1       | 707       | 20.19     |


**Decision:** `qty = -1` is a return/reversal entry — a standard pattern in sales systems (customer returned 1 unit, so the line is credited). These rows are **kept** in all layers. `total_line_extended_price` will be negative for these rows (a credit). The DQ check logs a WARNING but does not fail the pipeline.

---

## Step 2 — Store layer decisions

### 2.1 Primary keys


| Table                    | PK column             | Reasoning                   |
| ------------------------ | --------------------- | --------------------------- |
| store_product_master     | product_id            | Unique product identifier   |
| store_sales_order_header | sales_order_id        | Unique order identifier     |
| store_sales_order_detail | sales_order_detail_id | Unique line item identifier |


### 2.2 Foreign keys (documented, not enforced)


| Table                    | Column          | References                              |
| ------------------------ | --------------- | --------------------------------------- |
| store_sales_order_detail | sales_order_id  | store_sales_order_header.sales_order_id |
| store_sales_order_detail | product_id      | store_product_master.product_id         |
| store_sales_order_header | customer_id     | customer dimension (not in dataset)     |
| store_sales_order_header | sales_person_id | salesperson dimension (not in dataset)  |


FK constraints not enforced in PostgreSQL because the customer and salesperson dimension tables are not part of this dataset. Relationships are documented here for clarity.

### 2.3 Store layer persistence model

`store_product_master` → persistent, UPSERT (insert new, update existing by PK)
`store_sales_order_header` → persistent, UPSERT
`store_sales_order_detail` → persistent, UPSERT

All store tables use `CREATE TABLE IF NOT EXISTS` so data accumulates across pipeline runs.

---

## Step 3 — Publish layer decisions

### 3.1 publish_product transformations

**Rule 1 — Color NULL → 'N/A':**

```sql
-- How many products have empty Color?
SELECT COUNT(*) FROM raw_product_master WHERE "Color" = '';
-- Result: 77 products have no color
```

These get `color = 'N/A'` in publish_product.

**Rule 2 — ProductCategoryName enrichment:**

```sql
-- Which subcategories have NULL category?
SELECT DISTINCT "ProductSubCategoryName", "ProductCategoryName"
FROM raw_product_master
WHERE "ProductCategoryName" = ''
ORDER BY "ProductSubCategoryName";
```

Subcategories with missing category were mapped as specified in the assessment:

- Gloves, Shorts, Socks, Tights, Vests → `Clothing`
- Locks, Lights, Headsets, Helmets, Pedals, Pumps → `Accessories`
- Any containing "Frames" or Wheels, Saddles → `Components`

Products that don't match any rule retain `NULL` category.

### 3.2 publish_orders transformations

**LeadTimeInBusinessDays:** Calculated using a Python UDF that counts weekdays (Mon–Fri) between `order_date` (exclusive) and `ship_date` (inclusive). Saturdays (weekday=5) and Sundays (weekday=6) are excluded.

**TotalLineExtendedPrice:** `order_qty × (unit_price - unit_price_discount)`
For return rows (qty=-1): result is negative — this is a credit amount.

**Column selection:** All columns from SalesOrderDetail + all from SalesOrderHeader except `SalesOrderId` (already in detail) + rename `Freight` → `TotalOrderFreight`. 