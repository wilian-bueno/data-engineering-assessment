-- Final business-ready product table.


CREATE TABLE IF NOT EXISTS publish_product (
    product_id                 INTEGER        NOT NULL PRIMARY KEY,
    product_desc               VARCHAR(255),
    product_number             VARCHAR(50),
    make_flag                  BOOLEAN,
    color                      VARCHAR(50)    NOT NULL,
    safety_stock_level         INTEGER,
    reorder_point              INTEGER,
    standard_cost              DECIMAL(18,6),
    list_price                 DECIMAL(18,6),
    size                       VARCHAR(20),
    size_unit_measure_code     VARCHAR(10),
    weight                     FLOAT,
    weight_unit_measure_code   VARCHAR(10),
    product_category_name      VARCHAR(100),
    product_sub_category_name  VARCHAR(100),
    etl_created_at             TIMESTAMP WITH TIME ZONE
);
