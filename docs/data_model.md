# Data Model — Type Decisions & Assumptions

## Decision rules applied


| Rule          | Condition                                                 | PostgreSQL type                |
| ------------- | --------------------------------------------------------- | ------------------------------ |
| ID columns    | Column name contains `ID` and values are numeric integers | `INTEGER`                      |
| Flag columns  | Column name contains `Flag` and values are `True`/`False` | `BOOLEAN`                      |
| Cost columns  | Column name contains `Cost`                               | `DECIMAL(18,6)`                |
| Price columns | Column name contains `Price`                              | `DECIMAL(18,6)`                |
| Level columns | Column name contains `Level`                              | `INTEGER`                      |
| Point columns | Column name contains `Point`                              | `INTEGER`                      |
| Weight        | Products weight column only                               | `FLOAT` (nullable)             |
| Free text     | Names, codes, descriptions                                | `VARCHAR` or `TEXT`            |
| Nullable text | Columns with empty strings in raw                         | `NULL` in store (empty → NULL) |


---

## store_product_master

Source: `raw_product_master`


| Raw column               | Store column                | Type            | Nullable | Rule applied                         |
| ------------------------ | --------------------------- | --------------- | -------- | ------------------------------------ |
| `ProductID`              | `product_id`                | `INTEGER`       | NO       | ID rule → PK                         |
| `ProductDesc`            | `product_desc`              | `VARCHAR(255)`  | YES      | free text                            |
| `ProductNumber`          | `product_number`            | `VARCHAR(50)`   | YES      | free text                            |
| `MakeFlag`               | `make_flag`                 | `BOOLEAN`       | YES      | Flag rule                            |
| `Color`                  | `color`                     | `VARCHAR(50)`   | YES      | empty string → NULL                  |
| `SafetyStockLevel`       | `safety_stock_level`        | `INTEGER`       | YES      | Level rule                           |
| `ReorderPoint`           | `reorder_point`             | `INTEGER`       | YES      | Point rule                           |
| `StandardCost`           | `standard_cost`             | `DECIMAL(18,6)` | YES      | Cost rule                            |
| `ListPrice`              | `list_price`                | `DECIMAL(18,6)` | YES      | Price rule                           |
| `Size`                   | `size`                      | `VARCHAR(20)`   | YES      | mixed (58, M, L) — cannot be numeric |
| `SizeUnitMeasureCode`    | `size_unit_measure_code`    | `VARCHAR(10)`   | YES      | free text, trimmed                   |
| `Weight`                 | `weight`                    | `FLOAT`         | YES      | Weight rule; empty string → NULL     |
| `WeightUnitMeasureCode`  | `weight_unit_measure_code`  | `VARCHAR(10)`   | YES      | free text, trimmed                   |
| `ProductCategoryName`    | `product_category_name`     | `VARCHAR(100)`  | YES      | empty string → NULL                  |
| `ProductSubCategoryName` | `product_sub_category_name` | `VARCHAR(100)`  | YES      | empty string → NULL                  |


**Primary key:** `product_id`

**Note on Size:** Size values include both numeric (`58`) and alphabetic (`M`, `L`, `XL`) entries.
A numeric type would fail on alphabetic sizes, so `VARCHAR` is the correct choice.

**Note on empty strings:** The source CSV uses `""` for missing values. All empty strings are
converted to `NULL` during the store transformation so downstream layers can use `IS NULL` checks.

---

## store_sales_order_header

Source: `raw_sales_order_header`


| Raw column        | Store column        | Type            | Nullable | Rule applied                          |
| ----------------- | ------------------- | --------------- | -------- | ------------------------------------- |
| `SalesOrderID`    | `sales_order_id`    | `INTEGER`       | NO       | ID rule → PK                          |
| `OrderDate`       | `order_date`        | `DATE`          | NO       | See OrderDate assumption below        |
| `ShipDate`        | `ship_date`         | `DATE`          | NO       | Full date always present              |
| `OnlineOrderFlag` | `online_order_flag` | `BOOLEAN`       | YES      | Flag rule                             |
| `AccountNumber`   | `account_number`    | `VARCHAR(20)`   | YES      | Format "10-4030-027605"               |
| `CustomerID`      | `customer_id`       | `INTEGER`       | NO       | ID rule — FK documented, not enforced |
| `SalesPersonID`   | `sales_person_id`   | `INTEGER`       | YES      | ID rule — NULL for online orders      |
| `Freight`         | `freight`           | `DECIMAL(18,4)` | YES      | 4 decimal places (e.g. 89.4568)       |


**Primary key:** `sales_order_id`

**Foreign keys (documented, not enforced):**

- `customer_id` → customer dimension table (not present in this dataset)
- `sales_person_id` → salesperson dimension table (not present in this dataset)

### OrderDate assumption

The raw `OrderDate` column is **almost entirely** in full `YYYY-MM-DD` format. Only a
**very small number of rows — 5 out of 31,465** — contain a partial `YYYY-MM` value (no day):


| Format              | Example      | Row count | Strategy                       |
| ------------------- | ------------ | --------- | ------------------------------ |
| `YYYY-MM-DD` (full) | `2021-05-31` | 31,460    | Cast directly to DATE          |
| `YYYY-MM` (partial) | `2021-06`    | 5         | Derive as `ship_date - 7 days` |


Detection is **length-based** (`length(OrderDate) > 7`), not flag-based, so it is robust
regardless of `OnlineOrderFlag`.

**Why 7 days?** For comparable in-store records a consistent 7-day gap was observed between
OrderDate and ShipDate (e.g. `2021-05-31 → 2021-06-07`, `2021-06-05 → 2021-06-12`).

Because only **5 rows (~0.016%)** are affected, the impact on `LeadTimeInBusinessDays` and on
the Q2 analysis is **negligible** — the assumption is documented purely for traceability.

---

## store_sales_order_detail

Source: `raw_sales_order_detail`


| Raw column           | Store column            | Type            | Nullable | Rule applied                           |
| -------------------- | ----------------------- | --------------- | -------- | -------------------------------------- |
| `SalesOrderDetailID` | `sales_order_detail_id` | `INTEGER`       | NO       | ID rule → PK; all values fit in 32-bit |
| `SalesOrderID`       | `sales_order_id`        | `INTEGER`       | NO       | ID rule — FK to store header           |
| `OrderQty`           | `order_qty`             | `INTEGER`       | YES      | small quantities, safe as 32-bit       |
| `ProductID`          | `product_id`            | `INTEGER`       | YES      | ID rule — FK to store_product_master   |
| `UnitPrice`          | `unit_price`            | `DECIMAL(18,6)` | YES      | Price rule                             |
| `UnitPriceDiscount`  | `unit_price_discount`   | `DECIMAL(18,6)` | YES      | Price rule                             |


**Primary key:** `sales_order_detail_id`

**Note on negative order_qty:**
2 rows were found with `order_qty = -1`:


| sales_order_detail_id | sales_order_id | order_qty | product_id | unit_price |
| --------------------- | -------------- | --------- | ---------- | ---------- |
| 112                   | 43670          | -1        | 710        | 5.70       |
| 339                   | 43694          | -1        | 707        | 20.19      |


**Decision:** These are **return/reversal entries** — a quantity of exactly -1 is the standard pattern for a returned line item in sales systems. They are kept in the table and not filtered out. The `total_line_extended_price` for these rows is correctly negative (representing a credit to the customer). The DQ check logs a WARNING for negative quantities but does not fail the pipeline.

---

## publish_product

Source: `store_product_master`

Transformations applied on top of the store schema:


| Field                   | Transformation                                          | Reason                 |
| ----------------------- | ------------------------------------------------------- | ---------------------- |
| `color`                 | `NULL → 'N/A'`                                          | Assessment requirement |
| `product_category_name` | Enriched when NULL based on `product_sub_category_name` | Assessment requirement |


**Category enrichment logic:**


| Subcategory values                                      | Assigned category |
| ------------------------------------------------------- | ----------------- |
| Gloves, Shorts, Socks, Tights, Vests                    | `Clothing`        |
| Locks, Lights, Headsets, Helmets, Pedals, Pumps         | `Accessories`     |
| Any subcategory containing `Frames`, or Wheels, Saddles | `Components`      |


Products that do not match any of the above rules and have no existing category
retain `NULL` in `product_category_name`.

---

## Duplicate records — finding and resolution

### Finding

During the store layer transformation, **8 ProductIDs were found with 2 rows each**
in the raw data:


| ProductID | ProductDesc                     |
| --------- | ------------------------------- |
| 713       | Long-Sleeve Logo Jersey, S      |
| 714       | Long-Sleeve Logo Jersey, M      |
| 715       | Long-Sleeve Logo Jersey, L      |
| 716       | Long-Sleeve Logo Jersey, XL     |
| 881       | Short-Sleeve Classic Jersey, S  |
| 882       | Short-Sleeve Classic Jersey, M  |
| 883       | Short-Sleeve Classic Jersey, L  |
| 884       | Short-Sleeve Classic Jersey, XL |


Each duplicate pair had the following difference:


| Version | ProductCategoryName | ProductSubCategoryName | Decision                     |
| ------- | ------------------- | ---------------------- | ---------------------------- |
| Row A   | *(empty)*           | `Jerseys`              | **DROPPED** — 1 empty column |
| Row B   | `Clothing`          | `Shirt`                | **KEPT** — 0 empty columns   |


### Decision

Keep the **most complete row** per primary key — the one with the fewest NULL values.
Drop the row with the most NULL columns (least complete data).

This is not a blind `dropDuplicates()`. The selection is deliberate:
more data = more trustworthy record.

### Generic implementation

To make this reusable across all tables, a generic function was created:

```python
deduplicate_by_completeness(df, pk_col="product_id")
```

How it works:

1. For each group of rows sharing the same `pk_col` value, count how many columns are `NULL` per row.
2. Rank rows within the group: rank 1 = fewest NULLs (most complete).
3. Keep only rank-1 rows; drop all others.
4. Log the number of dropped rows with a warning.

This function can be applied to any DataFrame from any table by passing the
appropriate PK column name. It will be reused in the sales order store layers
if duplicates are detected there.

---

