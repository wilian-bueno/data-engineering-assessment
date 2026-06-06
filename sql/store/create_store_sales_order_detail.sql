-- Typed version of raw_sales_order_detail.

CREATE TABLE IF NOT EXISTS store_sales_order_detail (
    sales_order_detail_id  INTEGER        NOT NULL PRIMARY KEY,
    sales_order_id         INTEGER        NOT NULL,
    order_qty              INTEGER,
    product_id             INTEGER,
    unit_price             DECIMAL(18,6),
    unit_price_discount    DECIMAL(18,6),
    etl_created_at         TIMESTAMP WITH TIME ZONE,
    source_file_name       TEXT
);
