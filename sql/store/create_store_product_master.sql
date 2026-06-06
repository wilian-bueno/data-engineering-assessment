-- Typed version of raw_product_master.

CREATE TABLE IF NOT EXISTS store_product_master (
    product_id                 INTEGER        NOT NULL PRIMARY KEY,
    product_desc               VARCHAR(255),
    product_number             VARCHAR(50),
    make_flag                  BOOLEAN,
    color                      VARCHAR(50),           -- NULL when not applicable
    safety_stock_level         INTEGER,
    reorder_point              INTEGER,
    standard_cost              DECIMAL(18,6),
    list_price                 DECIMAL(18,6),
    size                       VARCHAR(20),           -- values can be '58', 'M', 'L', etc.
    size_unit_measure_code     VARCHAR(10),
    weight                     FLOAT,                 -- NULL when product has no weight
    weight_unit_measure_code   VARCHAR(10),
    product_category_name      VARCHAR(100),          -- NULL for products without a category
    product_sub_category_name  VARCHAR(100),
    etl_created_at             TIMESTAMP WITH TIME ZONE,
    source_file_name           TEXT
);
